# src/agent/distinct_advisor.py
"""
DISTINCT 사용 여부 판단 Tool
- JOIN 관계에서 중복 발생 가능성 분석
- SELECT DISTINCT 또는 COUNT(DISTINCT ...) 필요 여부 판단
"""

import mysql.connector
from typing import Dict, Any, List, Optional


def check_distinct_need(
    tables: List[str],
    join_pairs: List[Dict[str, str]],
    select_columns: List[str],
    conn_info: Dict[str, Any],
    db_id: str = "dw"
) -> Dict[str, Any]:
    """
    JOIN 시 DISTINCT가 필요한지 분석합니다.

    Args:
        tables: 사용할 테이블 리스트 (예: ["EMPLOYEE", "DEPARTMENT"])
        join_pairs: JOIN 조건들 (예: [{"left": "EMPLOYEE.DEPT_ID", "right": "DEPARTMENT.ID"}])
        select_columns: SELECT할 컬럼들 (예: ["EMPLOYEE.NAME", "DEPARTMENT.NAME"])
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID

    Returns:
        {
            "needs_distinct": bool,
            "risk_level": "none" | "low" | "medium" | "high",
            "reason": str,
            "duplicate_source": str (있으면),
            "suggestion": str
        }
    """
    result = {
        "tables": tables,
        "join_pairs": join_pairs,
        "select_columns": select_columns
    }

    if not join_pairs:
        result["needs_distinct"] = False
        result["risk_level"] = "none"
        result["reason"] = "Single table query - no JOIN duplication risk"
        result["suggestion"] = "DISTINCT not needed for single table queries unless filtering duplicates in the table itself."
        return result

    conn = None
    try:
        conn = mysql.connector.connect(
            host=conn_info.get('host', '127.0.0.1'),
            port=conn_info.get('port', 3306),
            user=conn_info.get('user', 'root'),
            password=conn_info.get('password', ''),
            database=db_id
        )
        cursor = conn.cursor(dictionary=True)

        # 각 JOIN에 대해 cardinality 분석
        cardinalities = []
        duplicate_sources = []

        for join in join_pairs:
            left_parts = join["left"].split(".")
            right_parts = join["right"].split(".")

            if len(left_parts) != 2 or len(right_parts) != 2:
                continue

            left_table, left_col = left_parts
            right_table, right_col = right_parts

            try:
                # 왼쪽 테이블의 키 uniqueness 확인
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT(DISTINCT `{left_col}`) as distinct_count
                    FROM `{left_table}`
                """)
                left_stats = cursor.fetchone()
                left_is_unique = (left_stats['total'] == left_stats['distinct_count'])

                # 오른쪽 테이블의 키 uniqueness 확인
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT(DISTINCT `{right_col}`) as distinct_count
                    FROM `{right_table}`
                """)
                right_stats = cursor.fetchone()
                right_is_unique = (right_stats['total'] == right_stats['distinct_count'])

                # Cardinality 결정
                if left_is_unique and right_is_unique:
                    card = "1:1"
                elif left_is_unique and not right_is_unique:
                    card = "1:N"
                    duplicate_sources.append(f"{right_table}.{right_col} has duplicates")
                elif not left_is_unique and right_is_unique:
                    card = "N:1"
                    duplicate_sources.append(f"{left_table}.{left_col} has duplicates")
                else:
                    card = "M:N"
                    duplicate_sources.append(f"Both {left_table}.{left_col} and {right_table}.{right_col} have duplicates")

                cardinalities.append({
                    "join": f"{left_table}.{left_col} = {right_table}.{right_col}",
                    "cardinality": card,
                    "left_unique": left_is_unique,
                    "right_unique": right_is_unique
                })

            except Exception as e:
                cardinalities.append({
                    "join": f"{left_table}.{left_col} = {right_table}.{right_col}",
                    "cardinality": "UNKNOWN",
                    "error": str(e)[:100]
                })

        result["cardinality_analysis"] = cardinalities

        # 위험도 판단
        has_mn = any(c.get("cardinality") == "M:N" for c in cardinalities)
        has_1n = any(c.get("cardinality") in ["1:N", "N:1"] for c in cardinalities)

        if has_mn:
            result["needs_distinct"] = True
            result["risk_level"] = "high"
            result["reason"] = "M:N (many-to-many) relationship detected. JOIN will produce duplicate rows."
            result["duplicate_source"] = "; ".join(duplicate_sources)
            result["suggestion"] = """Use SELECT DISTINCT or aggregate functions:
- SELECT DISTINCT col1, col2 FROM ...
- COUNT(DISTINCT column_name) instead of COUNT(*)
- Use subquery to deduplicate before JOIN if needed"""

        elif has_1n:
            result["needs_distinct"] = True
            result["risk_level"] = "medium"
            result["reason"] = "1:N relationship detected. Each row from the 'one' side will be duplicated for each match on the 'many' side."
            result["duplicate_source"] = "; ".join(duplicate_sources)
            result["suggestion"] = """Consider:
- SELECT DISTINCT if you only need unique combinations
- GROUP BY with aggregate functions if counting/summing
- Be aware that row counts will multiply"""

        else:
            result["needs_distinct"] = False
            result["risk_level"] = "low"
            result["reason"] = "1:1 relationships only. No duplication expected from JOINs."
            result["suggestion"] = "DISTINCT is optional - only use if you want to remove duplicates that exist in the source tables."

        return result

    except Exception as e:
        result["needs_distinct"] = None
        result["risk_level"] = "unknown"
        result["reason"] = f"Could not analyze: {str(e)[:200]}"
        result["suggestion"] = "When in doubt, use SELECT DISTINCT to be safe."
        return result

    finally:
        if conn:
            conn.close()


def format_distinct_advice(result: Dict[str, Any]) -> str:
    """분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅"""

    lines = []

    risk = result.get("risk_level", "unknown")

    # Cardinality 요약 (간결하게)
    cardinalities = result.get("cardinality_analysis", [])
    card_summary = []
    for c in cardinalities:
        card = c.get("cardinality", "?")
        join_str = c.get("join", "?")
        # 테이블명.컬럼 = 테이블명.컬럼 형태에서 간략화
        card_summary.append(f"{join_str}: {card}")

    if risk == "high":
        lines.append("⚠️ [DISTINCT] HIGH RISK - M:N relationship detected")
        lines.append("   Duplicates WILL occur. Use SELECT DISTINCT or COUNT(DISTINCT ...).")
    elif risk == "medium":
        lines.append("⚡ [DISTINCT] MEDIUM RISK - 1:N relationship detected")
        lines.append("   Duplicates may occur. Consider DISTINCT if selecting from the '1' side.")
    elif risk == "low":
        lines.append("✅ [DISTINCT] LOW RISK - 1:1 relationships only")
        lines.append("   DISTINCT is optional.")
    else:
        lines.append("❓ [DISTINCT] Could not analyze - consider using DISTINCT to be safe.")

    # Cardinality 상세 (간결하게)
    if card_summary:
        lines.append("")
        lines.append("Cardinality:")
        for cs in card_summary:
            lines.append(f"  {cs}")

    return "\n".join(lines)


# 테스트
if __name__ == "__main__":
    # Test case 1: M:N relationship
    result1 = check_distinct_need(
        tables=["EMPLOYEE", "PROJECT", "EMPLOYEE_PROJECT"],
        join_pairs=[
            {"left": "EMPLOYEE.ID", "right": "EMPLOYEE_PROJECT.EMPLOYEE_ID"},
            {"left": "PROJECT.ID", "right": "EMPLOYEE_PROJECT.PROJECT_ID"}
        ],
        select_columns=["EMPLOYEE.NAME", "PROJECT.NAME"],
        conn_info={},
        db_id="test"
    )
    print("Test 1 (M:N expected):")
    print(format_distinct_advice(result1))
    print()
