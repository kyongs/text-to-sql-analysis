"""
Gold SQL에서 구조적 힌트(skeleton hint)를 추출하는 유틸리티

단순 키워드 파싱으로 SQL 구조의 뼈대를 파악:
- GROUP BY 사용 여부
- Window Function 사용 여부
- CTE (WITH) 사용 여부
- UNION 사용 여부
- CASE WHEN 사용 여부
- Subquery 사용 여부
- HAVING 사용 여부
- DISTINCT 사용 여부
- ORDER BY + LIMIT 사용 여부
"""

import re
from typing import Dict, List, Optional


def extract_skeleton_hints(sql: str) -> Dict[str, bool]:
    """
    SQL에서 구조적 힌트 추출

    Args:
        sql: SQL 쿼리 문자열

    Returns:
        각 구조적 요소의 사용 여부 딕셔너리
    """
    if not sql:
        return {}

    # 대소문자 무시, 문자열 리터럴 제거 (오탐 방지)
    sql_upper = sql.upper()
    # 문자열 리터럴 제거 (싱글/더블 쿼트)
    sql_cleaned = re.sub(r"'[^']*'", "''", sql_upper)
    sql_cleaned = re.sub(r'"[^"]*"', '""', sql_cleaned)

    hints = {}

    # GROUP BY
    hints['group_by'] = bool(re.search(r'\bGROUP\s+BY\b', sql_cleaned))

    # Window Functions (OVER 절)
    hints['window_function'] = bool(re.search(r'\bOVER\s*\(', sql_cleaned))

    # CTE (WITH ... AS)
    hints['cte'] = bool(re.search(r'\bWITH\s+\w+\s+AS\s*\(', sql_cleaned))

    # UNION (UNION ALL 포함)
    hints['union'] = bool(re.search(r'\bUNION\b', sql_cleaned))

    # CASE WHEN
    hints['case_when'] = bool(re.search(r'\bCASE\s+WHEN\b', sql_cleaned))

    # Subquery (SELECT 안의 SELECT, FROM 절의 서브쿼리)
    # 메인 SELECT 제외하고 추가 SELECT가 있으면 서브쿼리
    select_count = len(re.findall(r'\bSELECT\b', sql_cleaned))
    hints['subquery'] = select_count > 1

    # HAVING
    hints['having'] = bool(re.search(r'\bHAVING\b', sql_cleaned))

    # DISTINCT
    hints['distinct'] = bool(re.search(r'\bDISTINCT\b', sql_cleaned))

    # ORDER BY + LIMIT (Top-N 패턴)
    has_order = bool(re.search(r'\bORDER\s+BY\b', sql_cleaned))
    has_limit = bool(re.search(r'\bLIMIT\b', sql_cleaned))
    hints['top_n'] = has_order and has_limit

    # EXCEPT / INTERSECT
    hints['set_operation'] = bool(re.search(r'\b(EXCEPT|INTERSECT)\b', sql_cleaned))

    return hints


def format_skeleton_hint(hints: Dict[str, bool]) -> str:
    """
    추출된 힌트를 프롬프트용 텍스트로 포맷팅

    Args:
        hints: extract_skeleton_hints()의 결과

    Returns:
        프롬프트에 삽입할 힌트 문자열
    """
    if not hints:
        return ""

    active_hints = []

    hint_descriptions = {
        'group_by': 'GROUP BY 사용',
        'window_function': 'Window Function (OVER) 사용',
        'cte': 'CTE (WITH ... AS) 사용',
        'union': 'UNION 사용',
        'case_when': 'CASE WHEN 사용',
        'subquery': 'Subquery 사용',
        'having': 'HAVING 사용',
        'distinct': 'DISTINCT 사용',
        'top_n': 'ORDER BY + LIMIT (Top-N 패턴)',
        'set_operation': 'EXCEPT/INTERSECT 사용',
    }

    for key, desc in hint_descriptions.items():
        if hints.get(key):
            active_hints.append(f"- {desc}")

    if not active_hints:
        return ""

    return "[SQL 구조 힌트]\n" + "\n".join(active_hints)


def generate_skeleton_hints_for_dataset(dataset: List[Dict]) -> List[Dict[str, bool]]:
    """
    데이터셋 전체에 대해 skeleton hints 생성

    Args:
        dataset: 데이터셋 리스트 (각 항목에 'sql' 또는 'gold_sql' 필드)

    Returns:
        각 항목에 대한 힌트 딕셔너리 리스트
    """
    hints_list = []

    for item in dataset:
        gold_sql = item.get('sql') or item.get('gold_sql') or item.get('SQL', '')
        hints = extract_skeleton_hints(gold_sql)
        hints_list.append(hints)

    return hints_list


def get_skeleton_hint_stats(hints_list: List[Dict[str, bool]]) -> Dict[str, int]:
    """
    힌트 통계 계산

    Args:
        hints_list: generate_skeleton_hints_for_dataset()의 결과

    Returns:
        각 힌트 유형별 사용 횟수
    """
    stats = {}

    for hints in hints_list:
        for key, value in hints.items():
            if value:
                stats[key] = stats.get(key, 0) + 1

    return stats
