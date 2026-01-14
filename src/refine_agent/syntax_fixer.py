# src/refine_agent/syntax_fixer.py
"""
Syntax Fixer Refine Agent
SQL 실행 에러 발생 시 에러 메시지를 분석하고 수정 권고사항을 제공합니다.

트리거: SQL 실행 시 syntax_error 발생
"""

import re
from typing import Dict, Any, List, Optional


# 에러 패턴과 권고사항 매핑
ERROR_PATTERNS = {
    # 테이블/컬럼 존재하지 않음
    r"Table '([^']+)' doesn't exist": {
        "category": "table_not_found",
        "suggestion": "테이블명이 잘못되었습니다. 스키마에서 올바른 테이블명을 확인하세요.",
        "action": "check_table_name"
    },
    r"Unknown column '([^']+)'": {
        "category": "column_not_found",
        "suggestion": "컬럼명이 잘못되었습니다. 해당 테이블의 컬럼 목록을 확인하세요.",
        "action": "check_column_name"
    },
    r"Column '([^']+)' in field list is ambiguous": {
        "category": "ambiguous_column",
        "suggestion": "여러 테이블에 동일한 컬럼명이 있습니다. 테이블 별칭을 사용하세요. (예: t.column_name)",
        "action": "add_table_alias"
    },

    # 구문 오류
    r"You have an error in your SQL syntax": {
        "category": "syntax_error",
        "suggestion": "SQL 구문 오류입니다. 괄호, 따옴표, 키워드 철자를 확인하세요.",
        "action": "check_syntax"
    },
    r"check the manual that corresponds to your MySQL server version for the right syntax": {
        "category": "syntax_error",
        "suggestion": "MySQL 구문 오류입니다. SELECT, FROM, WHERE, JOIN 순서와 키워드를 확인하세요.",
        "action": "check_syntax"
    },

    # GROUP BY 관련
    r"isn't in GROUP BY": {
        "category": "group_by_error",
        "suggestion": "SELECT에 있는 컬럼이 GROUP BY에 없습니다. 집계 함수로 감싸거나 GROUP BY에 추가하세요.",
        "action": "fix_group_by"
    },
    r"this is incompatible with sql_mode=only_full_group_by": {
        "category": "group_by_error",
        "suggestion": "GROUP BY 절에 SELECT의 비집계 컬럼을 모두 포함해야 합니다.",
        "action": "fix_group_by"
    },

    # JOIN 관련
    r"Unknown table '([^']+)'": {
        "category": "unknown_table",
        "suggestion": "FROM 또는 JOIN 절에서 참조하는 테이블이 정의되지 않았습니다.",
        "action": "check_join"
    },

    # 데이터 타입 관련
    r"Incorrect (datetime|date|integer|decimal) value": {
        "category": "data_type_error",
        "suggestion": "데이터 타입이 맞지 않습니다. 날짜는 'YYYY-MM-DD', 숫자는 따옴표 없이 사용하세요.",
        "action": "check_data_type"
    },
    r"Truncated incorrect": {
        "category": "data_type_error",
        "suggestion": "값의 형식이 컬럼 타입과 맞지 않습니다.",
        "action": "check_data_type"
    },

    # 서브쿼리 관련
    r"Subquery returns more than 1 row": {
        "category": "subquery_error",
        "suggestion": "서브쿼리가 여러 행을 반환합니다. = 대신 IN을 사용하거나 LIMIT 1을 추가하세요.",
        "action": "fix_subquery"
    },
    r"Every derived table must have its own alias": {
        "category": "subquery_error",
        "suggestion": "서브쿼리에 별칭이 필요합니다. (SELECT ...) AS subquery_alias",
        "action": "add_subquery_alias"
    },

    # 함수 관련
    r"FUNCTION ([^ ]+) does not exist": {
        "category": "function_error",
        "suggestion": "존재하지 않는 함수입니다. MySQL 함수명을 확인하세요.",
        "action": "check_function"
    },
    r"Incorrect parameter count in the call to": {
        "category": "function_error",
        "suggestion": "함수의 파라미터 개수가 잘못되었습니다.",
        "action": "check_function_params"
    },

    # 타임아웃
    r"max_execution_time exceeded|Query execution was interrupted": {
        "category": "timeout",
        "suggestion": "쿼리 실행 시간이 초과되었습니다. 인덱스 사용, LIMIT 추가, 또는 조인 최적화를 고려하세요.",
        "action": "optimize_query"
    },
}


def analyze_sql_error(
    sql: str,
    error_message: str,
    schema_info: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    SQL 에러를 분석하고 수정 권고사항을 반환합니다.

    Args:
        sql: 실행한 SQL 쿼리
        error_message: MySQL 에러 메시지
        schema_info: 스키마 정보 (있으면 더 정확한 권고)

    Returns:
        {
            "error_message": str,
            "category": str,
            "matched_pattern": str,
            "extracted_info": dict,  # 에러에서 추출한 정보 (테이블명, 컬럼명 등)
            "suggestion": str,
            "action": str,
            "detailed_advice": str
        }
    """
    result = {
        "error_message": error_message,
        "sql": sql,
        "category": "unknown",
        "matched_pattern": None,
        "extracted_info": {},
        "suggestion": "에러 메시지를 확인하고 SQL을 수정하세요.",
        "action": "manual_review",
        "detailed_advice": ""
    }

    # 에러 패턴 매칭
    for pattern, info in ERROR_PATTERNS.items():
        match = re.search(pattern, error_message, re.IGNORECASE)
        if match:
            result["category"] = info["category"]
            result["matched_pattern"] = pattern
            result["suggestion"] = info["suggestion"]
            result["action"] = info["action"]

            # 그룹이 있으면 추출
            if match.groups():
                if info["category"] == "table_not_found":
                    result["extracted_info"]["table"] = match.group(1)
                elif info["category"] == "column_not_found":
                    result["extracted_info"]["column"] = match.group(1)
                elif info["category"] == "ambiguous_column":
                    result["extracted_info"]["column"] = match.group(1)

            # 상세 조언 생성
            result["detailed_advice"] = _generate_detailed_advice(
                result["category"],
                result["extracted_info"],
                sql,
                schema_info
            )
            break

    return result


def _generate_detailed_advice(
    category: str,
    extracted_info: Dict,
    sql: str,
    schema_info: Optional[Dict]
) -> str:
    """카테고리별 상세 조언 생성"""

    advice_lines = []

    if category == "table_not_found":
        table = extracted_info.get("table", "")
        advice_lines.append(f"❌ 테이블 '{table}'이(가) 존재하지 않습니다.")
        advice_lines.append("")
        advice_lines.append("확인사항:")
        advice_lines.append("  1. 테이블명 철자 확인 (대소문자 주의)")
        advice_lines.append("  2. 스키마에서 유사한 테이블명 찾기")
        if schema_info and 'tables' in schema_info:
            similar = _find_similar_names(table, schema_info['tables'])
            if similar:
                advice_lines.append(f"  → 유사한 테이블: {', '.join(similar[:3])}")

    elif category == "column_not_found":
        column = extracted_info.get("column", "")
        advice_lines.append(f"❌ 컬럼 '{column}'이(가) 존재하지 않습니다.")
        advice_lines.append("")
        advice_lines.append("확인사항:")
        advice_lines.append("  1. 컬럼명 철자 확인")
        advice_lines.append("  2. 해당 컬럼이 어느 테이블에 속하는지 확인")
        advice_lines.append("  3. JOIN된 테이블의 컬럼인 경우 별칭 사용")

    elif category == "ambiguous_column":
        column = extracted_info.get("column", "")
        advice_lines.append(f"⚠️ 컬럼 '{column}'이(가) 여러 테이블에 존재합니다.")
        advice_lines.append("")
        advice_lines.append("해결방법:")
        advice_lines.append(f"  변경 전: SELECT {column} FROM ...")
        advice_lines.append(f"  변경 후: SELECT t.{column} FROM table_name t ...")

    elif category == "group_by_error":
        advice_lines.append("⚠️ GROUP BY 절 오류")
        advice_lines.append("")
        advice_lines.append("해결방법:")
        advice_lines.append("  1. SELECT의 모든 비집계 컬럼을 GROUP BY에 추가")
        advice_lines.append("  2. 또는 집계 함수(MAX, MIN, ANY_VALUE)로 감싸기")
        advice_lines.append("")
        advice_lines.append("예시:")
        advice_lines.append("  SELECT a, b, COUNT(*) → GROUP BY a, b")

    elif category == "subquery_error":
        advice_lines.append("⚠️ 서브쿼리 오류")
        advice_lines.append("")
        advice_lines.append("해결방법:")
        advice_lines.append("  1. = 대신 IN 사용: WHERE col IN (SELECT ...)")
        advice_lines.append("  2. 또는 LIMIT 1 추가: WHERE col = (SELECT ... LIMIT 1)")
        advice_lines.append("  3. 서브쿼리 별칭 추가: (SELECT ...) AS sub")

    elif category == "timeout":
        advice_lines.append("⏱️ 쿼리 실행 시간 초과")
        advice_lines.append("")
        advice_lines.append("최적화 방법:")
        advice_lines.append("  1. WHERE 조건 추가로 결과 범위 축소")
        advice_lines.append("  2. 불필요한 JOIN 제거")
        advice_lines.append("  3. SELECT * 대신 필요한 컬럼만 선택")
        advice_lines.append("  4. LIMIT 추가")

    else:
        advice_lines.append("일반적인 SQL 오류입니다.")
        advice_lines.append("SQL 구문을 다시 확인하세요.")

    return "\n".join(advice_lines)


def _find_similar_names(target: str, candidates: List[str], threshold: float = 0.6) -> List[str]:
    """유사한 이름 찾기 (간단한 유사도 기반)"""
    target_lower = target.lower()
    similar = []

    for candidate in candidates:
        cand_lower = candidate.lower()

        # 포함 관계
        if target_lower in cand_lower or cand_lower in target_lower:
            similar.append(candidate)
            continue

        # 간단한 유사도 (공통 문자 비율)
        common = set(target_lower) & set(cand_lower)
        if len(common) / max(len(target_lower), len(cand_lower)) >= threshold:
            similar.append(candidate)

    return similar


def format_syntax_fix_advice(result: Dict[str, Any]) -> str:
    """
    분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅
    """
    lines = []

    category = result.get("category", "unknown")
    suggestion = result.get("suggestion", "")
    detailed = result.get("detailed_advice", "")

    # 카테고리별 아이콘
    icons = {
        "table_not_found": "🔴",
        "column_not_found": "🔴",
        "ambiguous_column": "🟡",
        "syntax_error": "🔴",
        "group_by_error": "🟡",
        "subquery_error": "🟡",
        "data_type_error": "🟡",
        "function_error": "🔴",
        "timeout": "⏱️",
        "unknown": "❓"
    }

    icon = icons.get(category, "❓")

    lines.append(f"{icon} [SYNTAX FIX] {category.upper().replace('_', ' ')}")
    lines.append("")

    if detailed:
        lines.append(detailed)
    else:
        lines.append(suggestion)

    # 원본 에러 메시지 (간략화)
    error_msg = result.get("error_message", "")
    if error_msg:
        # 너무 길면 자르기
        if len(error_msg) > 200:
            error_msg = error_msg[:200] + "..."
        lines.append("")
        lines.append(f"원본 에러: {error_msg}")

    return "\n".join(lines)


# 테스트
if __name__ == "__main__":
    # Test 1: Table not found
    result1 = analyze_sql_error(
        "SELECT * FROM EMPLOYE",
        "1146 (42S02): Table 'dw.EMPLOYE' doesn't exist"
    )
    print("Test 1 (Table not found):")
    print(format_syntax_fix_advice(result1))
    print()

    # Test 2: Ambiguous column
    result2 = analyze_sql_error(
        "SELECT NAME FROM EMP JOIN DEPT ON EMP.DEPT_ID = DEPT.ID",
        "1052 (23000): Column 'NAME' in field list is ambiguous"
    )
    print("Test 2 (Ambiguous column):")
    print(format_syntax_fix_advice(result2))
    print()

    # Test 3: GROUP BY error
    result3 = analyze_sql_error(
        "SELECT dept, name, COUNT(*) FROM emp GROUP BY dept",
        "Expression #2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'name' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by"
    )
    print("Test 3 (GROUP BY error):")
    print(format_syntax_fix_advice(result3))
