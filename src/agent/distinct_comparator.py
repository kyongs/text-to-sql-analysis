# src/agent/distinct_comparator.py
"""
DISTINCT 유무에 따른 쿼리 결과 비교 Tool
- 생성된 SQL에서 DISTINCT를 추가/제거했을 때 결과 차이를 보여줌
- LLM이 DISTINCT 필요 여부를 판단하는데 도움
"""

import mysql.connector
import re
from typing import Dict, Any, List, Optional


def compare_distinct_results(
    sql: str,
    conn_info: Dict[str, Any],
    db_id: str = "dw",
    limit: int = 10
) -> Dict[str, Any]:
    """
    SQL에서 DISTINCT 유무에 따른 결과 차이를 비교합니다.

    Args:
        sql: 비교할 SQL 쿼리
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID
        limit: 샘플 결과 개수

    Returns:
        {
            "original_sql": str,
            "has_distinct": bool,
            "with_distinct": {"row_count": int, "sample": list},
            "without_distinct": {"row_count": int, "sample": list},
            "difference": int,
            "duplicate_ratio": float,
            "recommendation": str,
            "duplicate_examples": list (있으면)
        }
    """
    result = {
        "original_sql": sql[:200] + "..." if len(sql) > 200 else sql
    }

    # SQL에서 DISTINCT 여부 확인
    sql_upper = sql.upper().strip()
    has_distinct = bool(re.search(r'\bSELECT\s+DISTINCT\b', sql_upper))
    result["has_distinct"] = has_distinct

    # DISTINCT 추가/제거된 버전 생성
    if has_distinct:
        sql_with_distinct = sql
        # SELECT DISTINCT -> SELECT
        sql_without_distinct = re.sub(
            r'\bSELECT\s+DISTINCT\b',
            'SELECT',
            sql,
            count=1,
            flags=re.IGNORECASE
        )
    else:
        sql_without_distinct = sql
        # SELECT -> SELECT DISTINCT
        sql_with_distinct = re.sub(
            r'\bSELECT\b',
            'SELECT DISTINCT',
            sql,
            count=1,
            flags=re.IGNORECASE
        )

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

        # WITH DISTINCT 실행
        try:
            # COUNT 쿼리
            count_sql_distinct = f"SELECT COUNT(*) as cnt FROM ({sql_with_distinct}) as subq"
            cursor.execute(count_sql_distinct)
            count_distinct = cursor.fetchone()['cnt']

            # 샘플 쿼리
            sample_sql_distinct = f"{sql_with_distinct} LIMIT {limit}"
            cursor.execute(sample_sql_distinct)
            sample_distinct = cursor.fetchall()

            result["with_distinct"] = {
                "row_count": count_distinct,
                "sample": sample_distinct[:5]  # 샘플은 5개만
            }
        except Exception as e:
            result["with_distinct"] = {"error": str(e)[:200]}

        # WITHOUT DISTINCT 실행
        try:
            count_sql_no_distinct = f"SELECT COUNT(*) as cnt FROM ({sql_without_distinct}) as subq"
            cursor.execute(count_sql_no_distinct)
            count_no_distinct = cursor.fetchone()['cnt']

            sample_sql_no_distinct = f"{sql_without_distinct} LIMIT {limit}"
            cursor.execute(sample_sql_no_distinct)
            sample_no_distinct = cursor.fetchall()

            result["without_distinct"] = {
                "row_count": count_no_distinct,
                "sample": sample_no_distinct[:5]
            }
        except Exception as e:
            result["without_distinct"] = {"error": str(e)[:200]}

        # 차이 계산
        if "error" not in result.get("with_distinct", {}) and "error" not in result.get("without_distinct", {}):
            count_with = result["with_distinct"]["row_count"]
            count_without = result["without_distinct"]["row_count"]

            result["difference"] = count_without - count_with
            result["duplicate_ratio"] = round((count_without - count_with) / count_without * 100, 1) if count_without > 0 else 0

            # 중복 예시 찾기 (DISTINCT 없을 때만 나타나는 중복)
            if result["difference"] > 0:
                try:
                    # GROUP BY로 중복 찾기 - 컬럼 추출
                    # SELECT 절에서 컬럼들 추출
                    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_without_distinct, re.IGNORECASE | re.DOTALL)
                    if select_match:
                        select_clause = select_match.group(1).strip()
                        # DISTINCT 제거
                        select_clause = re.sub(r'^\s*DISTINCT\s+', '', select_clause, flags=re.IGNORECASE)

                        # 중복 row 찾기
                        dup_sql = f"""
                            SELECT {select_clause}, COUNT(*) as dup_count
                            FROM ({sql_without_distinct}) as subq
                            GROUP BY {select_clause}
                            HAVING COUNT(*) > 1
                            LIMIT 3
                        """
                        cursor.execute(dup_sql)
                        duplicates = cursor.fetchall()
                        if duplicates:
                            result["duplicate_examples"] = duplicates
                except Exception:
                    pass  # 중복 찾기 실패해도 계속 진행

            # 권장사항
            if result["difference"] == 0:
                result["recommendation"] = "NO_DIFFERENCE"
                result["recommendation_detail"] = "DISTINCT makes no difference - no duplicates exist. You can omit DISTINCT for better performance."
            elif result["duplicate_ratio"] > 50:
                result["recommendation"] = "DISTINCT_CRITICAL"
                result["recommendation_detail"] = f"HIGH duplicate ratio ({result['duplicate_ratio']}%). DISTINCT is likely needed to avoid incorrect results."
            elif result["duplicate_ratio"] > 10:
                result["recommendation"] = "DISTINCT_RECOMMENDED"
                result["recommendation_detail"] = f"Moderate duplicate ratio ({result['duplicate_ratio']}%). Consider if duplicates should be included based on question semantics."
            else:
                result["recommendation"] = "DISTINCT_OPTIONAL"
                result["recommendation_detail"] = f"Low duplicate ratio ({result['duplicate_ratio']}%). DISTINCT may or may not be needed depending on requirements."

        return result

    except Exception as e:
        result["error"] = str(e)[:300]
        return result

    finally:
        if conn:
            conn.close()


def format_distinct_comparison(result: Dict[str, Any]) -> str:
    """비교 결과를 LLM이 이해하기 쉬운 형태로 포맷팅"""

    lines = []

    if result.get("error"):
        return f"[DISTINCT COMPARISON] Error: {result['error']}"

    has_distinct = result.get("has_distinct", False)
    lines.append(f"[DISTINCT COMPARISON] Original query {'HAS' if has_distinct else 'does NOT have'} DISTINCT")
    lines.append("")

    # Row count 비교
    with_d = result.get("with_distinct", {})
    without_d = result.get("without_distinct", {})

    if "error" not in with_d and "error" not in without_d:
        lines.append("Row Count Comparison:")
        lines.append(f"  - WITH DISTINCT:    {with_d.get('row_count', '?'):,} rows")
        lines.append(f"  - WITHOUT DISTINCT: {without_d.get('row_count', '?'):,} rows")
        lines.append(f"  - Difference:       {result.get('difference', 0):,} duplicate rows ({result.get('duplicate_ratio', 0)}%)")
        lines.append("")

    # 중복 예시
    if result.get("duplicate_examples"):
        lines.append("Duplicate Examples (rows that appear multiple times):")
        for i, dup in enumerate(result["duplicate_examples"][:3], 1):
            dup_count = dup.pop('dup_count', '?')
            lines.append(f"  {i}. {dup} (appears {dup_count}x)")
        lines.append("")

    # 권장사항
    rec = result.get("recommendation", "UNKNOWN")
    detail = result.get("recommendation_detail", "")

    if rec == "NO_DIFFERENCE":
        lines.append("RECOMMENDATION: DISTINCT not needed (no duplicates)")
    elif rec == "DISTINCT_CRITICAL":
        lines.append("RECOMMENDATION: DISTINCT STRONGLY RECOMMENDED")
    elif rec == "DISTINCT_RECOMMENDED":
        lines.append("RECOMMENDATION: Consider using DISTINCT")
    else:
        lines.append(f"RECOMMENDATION: {rec}")

    if detail:
        lines.append(f"  {detail}")

    return "\n".join(lines)


# 테스트
if __name__ == "__main__":
    # 테스트 SQL
    test_sql = """
    SELECT e.name, d.department_name
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    """

    print("Test SQL:")
    print(test_sql)
    print()

    # Mock result for demonstration
    mock_result = {
        "has_distinct": False,
        "with_distinct": {"row_count": 100, "sample": []},
        "without_distinct": {"row_count": 150, "sample": []},
        "difference": 50,
        "duplicate_ratio": 33.3,
        "recommendation": "DISTINCT_RECOMMENDED",
        "recommendation_detail": "Moderate duplicate ratio (33.3%). Consider if duplicates should be included."
    }

    print(format_distinct_comparison(mock_result))
