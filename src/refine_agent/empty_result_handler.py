# src/refine_agent/empty_result_handler.py
"""
Empty Result Handler Refine Agent
SQL 실행 결과가 0행일 때 원인을 분석하고 수정 권고사항을 제공합니다.

트리거: SQL 실행 성공 but row_count == 0
"""

import re
import os
import mysql.connector
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()


def analyze_empty_result(
    sql: str,
    conn_info: Dict[str, Any],
    db_id: str = "dw",
    question: Optional[str] = None
) -> Dict[str, Any]:
    """
    결과가 0행인 SQL을 분석하고 원인과 권고사항을 반환합니다.

    Args:
        sql: 실행한 SQL 쿼리
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID
        question: 원본 질문 (있으면 더 정확한 분석)

    Returns:
        {
            "sql": str,
            "analysis": {
                "where_conditions": [...],
                "join_conditions": [...],
                "potential_issues": [...]
            },
            "checks_performed": [...],
            "suggestions": [...]
        }
    """
    result = {
        "sql": sql,
        "analysis": {
            "where_conditions": [],
            "join_conditions": [],
            "potential_issues": []
        },
        "checks_performed": [],
        "suggestions": []
    }

    # SQL 파싱
    parsed = _parse_sql_basic(sql)
    result["analysis"]["where_conditions"] = parsed.get("where_conditions", [])
    result["analysis"]["join_conditions"] = parsed.get("join_conditions", [])
    result["analysis"]["tables"] = parsed.get("tables", [])

    # DB 연결 후 검사 수행
    try:
        password = os.getenv('MYSQL_PASSWORD') if conn_info.get('password') == 'from_env' else conn_info.get('password')

        conn = mysql.connector.connect(
            host=conn_info.get('host', '127.0.0.1'),
            port=conn_info.get('port', 3306),
            user=conn_info.get('user', 'root'),
            password=password,
            database=db_id
        )
        cursor = conn.cursor(dictionary=True)

        # 1. WHERE 조건 값 검사
        where_checks = _check_where_values(cursor, parsed)
        result["checks_performed"].extend(where_checks)

        # 2. JOIN 조건 검사 (매칭되는 행이 있는지)
        join_checks = _check_join_matches(cursor, parsed)
        result["checks_performed"].extend(join_checks)

        # 3. 개별 테이블 행 수 확인
        table_counts = _check_table_counts(cursor, parsed)
        result["checks_performed"].extend(table_counts)

        cursor.close()
        conn.close()

    except Exception as e:
        result["checks_performed"].append({
            "check": "db_connection",
            "result": f"DB 연결 실패: {str(e)[:100]}"
        })

    # 분석 결과 기반 권고사항 생성
    result["suggestions"] = _generate_suggestions(result)

    return result


def _parse_sql_basic(sql: str) -> Dict[str, Any]:
    """SQL에서 기본 정보 추출 (간단한 파싱)"""
    result = {
        "tables": [],
        "where_conditions": [],
        "join_conditions": []
    }

    sql_upper = sql.upper()

    # 테이블 추출 (FROM, JOIN 뒤)
    # FROM table
    from_match = re.search(r'FROM\s+`?(\w+)`?', sql, re.IGNORECASE)
    if from_match:
        result["tables"].append(from_match.group(1))

    # JOIN table
    join_matches = re.findall(r'JOIN\s+`?(\w+)`?', sql, re.IGNORECASE)
    result["tables"].extend(join_matches)

    # WHERE 조건 추출
    where_match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    if where_match:
        where_clause = where_match.group(1).strip()
        # 간단히 AND로 분리
        conditions = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        for cond in conditions:
            cond = cond.strip()
            if cond:
                # = 조건 파싱
                eq_match = re.search(r'`?(\w+)`?\.?`?(\w+)?`?\s*=\s*[\'"]?([^\'"]+)[\'"]?', cond)
                if eq_match:
                    result["where_conditions"].append({
                        "raw": cond,
                        "table": eq_match.group(1) if eq_match.group(2) else None,
                        "column": eq_match.group(2) or eq_match.group(1),
                        "value": eq_match.group(3).strip().rstrip("'\"")
                    })
                else:
                    result["where_conditions"].append({"raw": cond})

    # JOIN 조건 추출
    join_on_matches = re.findall(r'ON\s+([^JOIN]+?)(?:JOIN|WHERE|GROUP|ORDER|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
    for join_on in join_on_matches:
        result["join_conditions"].append(join_on.strip())

    return result


def _check_where_values(cursor, parsed: Dict) -> List[Dict]:
    """WHERE 조건의 값이 실제로 존재하는지 확인"""
    checks = []

    for cond in parsed.get("where_conditions", []):
        if "column" not in cond or "value" not in cond:
            continue

        column = cond["column"]
        value = cond["value"]
        table = cond.get("table")

        # 테이블 추정 (첫 번째 테이블)
        if not table and parsed.get("tables"):
            table = parsed["tables"][0]

        if not table:
            continue

        try:
            # 값 존재 여부 확인
            check_sql = f"SELECT COUNT(*) as cnt FROM `{table}` WHERE `{column}` = %s"
            cursor.execute(check_sql, (value,))
            result = cursor.fetchone()
            count = result['cnt'] if result else 0

            if count == 0:
                # 유사값 검색
                like_sql = f"""
                    SELECT DISTINCT `{column}` as val
                    FROM `{table}`
                    WHERE `{column}` LIKE %s
                    LIMIT 5
                """
                cursor.execute(like_sql, (f"%{value}%",))
                similar = [row['val'] for row in cursor.fetchall()]

                checks.append({
                    "check": "where_value",
                    "table": table,
                    "column": column,
                    "searched_value": value,
                    "found": False,
                    "similar_values": similar,
                    "issue": f"'{value}'가 {table}.{column}에 없습니다."
                })
            else:
                checks.append({
                    "check": "where_value",
                    "table": table,
                    "column": column,
                    "searched_value": value,
                    "found": True,
                    "count": count
                })

        except Exception as e:
            checks.append({
                "check": "where_value",
                "table": table,
                "column": column,
                "error": str(e)[:100]
            })

    return checks


def _check_join_matches(cursor, parsed: Dict) -> List[Dict]:
    """JOIN 조건에서 매칭되는 행이 있는지 확인"""
    checks = []

    for join_cond in parsed.get("join_conditions", []):
        # 간단히 = 조건 파싱
        match = re.search(r'`?(\w+)`?\.`?(\w+)`?\s*=\s*`?(\w+)`?\.`?(\w+)`?', join_cond)
        if not match:
            continue

        t1, c1, t2, c2 = match.groups()

        try:
            # 양쪽 테이블에서 공통 값이 있는지 확인
            check_sql = f"""
                SELECT COUNT(*) as cnt
                FROM (SELECT DISTINCT `{c1}` as v FROM `{t1}`) a
                JOIN (SELECT DISTINCT `{c2}` as v FROM `{t2}`) b
                ON a.v = b.v
            """
            cursor.execute(check_sql)
            result = cursor.fetchone()
            common_count = result['cnt'] if result else 0

            if common_count == 0:
                checks.append({
                    "check": "join_match",
                    "condition": join_cond,
                    "left": f"{t1}.{c1}",
                    "right": f"{t2}.{c2}",
                    "common_values": 0,
                    "issue": f"JOIN 조건 {t1}.{c1} = {t2}.{c2}에 매칭되는 값이 없습니다."
                })
            else:
                checks.append({
                    "check": "join_match",
                    "condition": join_cond,
                    "left": f"{t1}.{c1}",
                    "right": f"{t2}.{c2}",
                    "common_values": common_count
                })

        except Exception as e:
            checks.append({
                "check": "join_match",
                "condition": join_cond,
                "error": str(e)[:100]
            })

    return checks


def _check_table_counts(cursor, parsed: Dict) -> List[Dict]:
    """각 테이블의 전체 행 수 확인"""
    checks = []

    for table in parsed.get("tables", []):
        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table}`")
            result = cursor.fetchone()
            count = result['cnt'] if result else 0

            checks.append({
                "check": "table_count",
                "table": table,
                "row_count": count
            })

        except Exception as e:
            checks.append({
                "check": "table_count",
                "table": table,
                "error": str(e)[:100]
            })

    return checks


def _generate_suggestions(result: Dict) -> List[str]:
    """분석 결과 기반 권고사항 생성"""
    suggestions = []

    checks = result.get("checks_performed", [])

    # WHERE 값 문제
    where_issues = [c for c in checks if c.get("check") == "where_value" and not c.get("found", True)]
    for issue in where_issues:
        value = issue.get("searched_value", "")
        column = issue.get("column", "")
        table = issue.get("table", "")
        similar = issue.get("similar_values", [])

        if similar:
            suggestions.append(
                f"WHERE 조건의 '{value}'가 {table}.{column}에 없습니다. "
                f"유사한 값: {', '.join(str(s) for s in similar[:3])}"
            )
        else:
            suggestions.append(
                f"WHERE 조건의 '{value}'가 {table}.{column}에 없습니다. "
                f"정확한 값을 lookup_column_values 도구로 확인하세요."
            )

    # JOIN 매칭 문제
    join_issues = [c for c in checks if c.get("check") == "join_match" and c.get("common_values") == 0]
    for issue in join_issues:
        left = issue.get("left", "")
        right = issue.get("right", "")
        suggestions.append(
            f"JOIN 조건 {left} = {right}에 매칭되는 값이 없습니다. "
            f"올바른 JOIN 키인지 inspect_join_relationship으로 확인하세요."
        )

    # 테이블이 비어있는 경우
    empty_tables = [c for c in checks if c.get("check") == "table_count" and c.get("row_count") == 0]
    for issue in empty_tables:
        table = issue.get("table", "")
        suggestions.append(f"테이블 {table}이 비어있습니다.")

    # 기본 권고사항
    if not suggestions:
        suggestions.append("WHERE 조건의 값이 정확한지 확인하세요.")
        suggestions.append("JOIN 조건이 올바른지 확인하세요.")
        suggestions.append("필터 조건을 완화해 보세요 (예: LIKE 사용, 날짜 범위 확대).")

    return suggestions


def format_empty_result_advice(result: Dict[str, Any]) -> str:
    """
    분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅
    """
    lines = []

    lines.append("🟡 [EMPTY RESULT] 결과 0행 분석")
    lines.append("")

    # 검사 결과 요약
    checks = result.get("checks_performed", [])

    # WHERE 값 검사 결과
    where_checks = [c for c in checks if c.get("check") == "where_value"]
    if where_checks:
        lines.append("📋 WHERE 조건 검사:")
        for c in where_checks:
            if c.get("found"):
                lines.append(f"   ✅ {c.get('table')}.{c.get('column')} = '{c.get('searched_value')}' → {c.get('count')}행 존재")
            else:
                similar = c.get("similar_values", [])
                lines.append(f"   ❌ {c.get('table')}.{c.get('column')} = '{c.get('searched_value')}' → 없음!")
                if similar:
                    lines.append(f"      유사값: {', '.join(str(s) for s in similar[:3])}")
        lines.append("")

    # JOIN 검사 결과
    join_checks = [c for c in checks if c.get("check") == "join_match"]
    if join_checks:
        lines.append("🔗 JOIN 조건 검사:")
        for c in join_checks:
            common = c.get("common_values", 0)
            if common > 0:
                lines.append(f"   ✅ {c.get('left')} = {c.get('right')} → {common}개 공통값")
            else:
                lines.append(f"   ❌ {c.get('left')} = {c.get('right')} → 매칭 없음!")
        lines.append("")

    # 테이블 행 수
    table_checks = [c for c in checks if c.get("check") == "table_count"]
    if table_checks:
        lines.append("📊 테이블 현황:")
        for c in table_checks:
            lines.append(f"   - {c.get('table')}: {c.get('row_count', '?')}행")
        lines.append("")

    # 권고사항
    suggestions = result.get("suggestions", [])
    if suggestions:
        lines.append("💡 권고사항:")
        for i, s in enumerate(suggestions, 1):
            lines.append(f"   {i}. {s}")

    return "\n".join(lines)


# 테스트
if __name__ == "__main__":
    # 간단한 파싱 테스트
    test_sql = """
    SELECT e.NAME, d.DEPT_NAME
    FROM EMPLOYEE e
    JOIN DEPARTMENT d ON e.DEPT_ID = d.ID
    WHERE e.STATUS = 'Active' AND d.LOCATION = 'Seoul'
    """

    parsed = _parse_sql_basic(test_sql)
    print("Parsed SQL:")
    print(f"  Tables: {parsed['tables']}")
    print(f"  WHERE conditions: {parsed['where_conditions']}")
    print(f"  JOIN conditions: {parsed['join_conditions']}")
