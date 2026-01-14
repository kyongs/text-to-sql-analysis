# src/agent/aggregation_advisor.py
"""
Aggregation 방식 판단 Tool
- 테이블 구조와 질문을 기반으로 GROUP BY vs Window Function 권장
- SQL 작성 전에 호출해서 방향 결정
"""

import re
from typing import Dict, Any, List, Optional
import pymysql
from dotenv import load_dotenv
import os

load_dotenv()


def check_aggregation_pattern(
    question: str,
    tables: List[str],
    conn_info: Dict[str, Any],
    db_id: str = "dw"
) -> Dict[str, Any]:
    """
    질문 패턴과 테이블 구조를 분석해서 적절한 aggregation 방식을 권장합니다.

    SQL 작성 전에 호출하여 GROUP BY vs Window Function 방향을 결정합니다.

    Args:
        question: 자연어 질문
        tables: 사용할 테이블 리스트
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID

    Returns:
        {
            "recommendation": "window_function" | "group_by" | "no_aggregation",
            "confidence": float,
            "reason": str,
            "warning": str (있으면),
            "example_pattern": str
        }
    """
    question_lower = question.lower()
    result = {"tables": tables, "question_snippet": question[:100]}

    # 1. 집계 필요 여부 확인
    agg_keywords = ['total', 'count', 'sum', 'average', 'number of', 'how many',
                    'min', 'max', 'amount', 'quantity']
    needs_aggregation = any(kw in question_lower for kw in agg_keywords)

    if not needs_aggregation:
        result["recommendation"] = "no_aggregation"
        result["confidence"] = 0.8
        result["reason"] = "No aggregation keywords found in question"
        result["example_pattern"] = "SELECT col1, col2 FROM table WHERE condition"
        return result

    # 2. 패턴 분석: 상세 정보 + 집계 vs 집계만

    # 상세 정보 요청 패턴 (Window Function 힌트)
    detail_patterns = [
        r'list.*(names?|titles?|address|description).*(total|count|sum|amount)',
        r'(names?|titles?|isbn|address).*(and|,).*(total|count|sum|number)',
        r'what are the .*(titles?|names?).*(and|,).*(total|count)',
        r'along with.*(total|count|sum)',
        r'(individual|each|every).*(with|and).*(total|sum|count)',
    ]

    detail_match = any(re.search(p, question_lower) for p in detail_patterns)

    # 순수 집계 패턴 (GROUP BY 힌트)
    pure_agg_patterns = [
        r'^what is the (total|count|number|sum)',
        r'^how many',
        r'^give (me )?(the )?(total|count|number)',
        r'for each .* what is the (count|total|number|sum)',
        r'(count|total|number) (of|for) .* (for each|per|by)',
    ]

    pure_agg_match = any(re.search(p, question_lower) for p in pure_agg_patterns)

    # 3. "for each" 분석 - 핵심 판단 기준
    for_each_match = re.search(r'for each ([a-z_\s]+)', question_lower)
    group_entity = for_each_match.group(1).strip() if for_each_match else None

    # 요청된 컬럼 수 추정 (쉼표와 and로 분리)
    # "list A, B, C, and D for each X" -> 4개 컬럼
    list_match = re.search(r'(list|what are|provide|give|show)\s+(.+?)\s+(for each|$)', question_lower)
    if list_match:
        items_str = list_match.group(2)
        # 쉼표와 'and'로 분리
        items = re.split(r',\s*|\s+and\s+', items_str)
        requested_columns = len([i for i in items if i.strip()])
    else:
        requested_columns = 0

    # 4. 최종 판단
    if detail_match and requested_columns >= 3:
        result["recommendation"] = "window_function"
        result["confidence"] = 0.85
        result["reason"] = f"Question requests {requested_columns} detail columns along with aggregation. Window function preserves each row while adding aggregate values."
        result["warning"] = "Using GROUP BY here would lose individual row details or require all columns in GROUP BY clause."
        result["example_pattern"] = """
SELECT
    detail_col1,
    detail_col2,
    SUM(amount) OVER (PARTITION BY group_col) as total_amount
FROM table
-- Each row preserved with its aggregate value"""

    elif pure_agg_match or (group_entity and requested_columns <= 2):
        result["recommendation"] = "group_by"
        result["confidence"] = 0.85
        result["reason"] = f"Question asks for aggregated values" + (f" grouped by '{group_entity}'" if group_entity else "") + ". Standard GROUP BY is appropriate."
        result["example_pattern"] = f"""
SELECT
    {group_entity or 'group_col'},
    COUNT(*) as count,
    SUM(amount) as total
FROM table
GROUP BY {group_entity or 'group_col'}"""

    elif requested_columns >= 3 and 'for each' in question_lower:
        # 다수 컬럼 + for each = Window 가능성
        result["recommendation"] = "window_function"
        result["confidence"] = 0.7
        result["reason"] = f"Question requests {requested_columns} columns with 'for each' pattern. Consider if you need to preserve individual rows."
        result["warning"] = "If individual row details are needed alongside aggregation, use Window Function."
        result["example_pattern"] = """
-- Option 1: Window Function (preserves rows)
SELECT col1, col2, col3, SUM(x) OVER (PARTITION BY group_col)
FROM table

-- Option 2: GROUP BY (aggregates rows)
SELECT group_col, COUNT(*), SUM(x)
FROM table
GROUP BY group_col"""

    else:
        result["recommendation"] = "group_by"
        result["confidence"] = 0.6
        result["reason"] = "Standard aggregation pattern detected."
        result["example_pattern"] = """
SELECT group_col, COUNT(*), SUM(amount)
FROM table
GROUP BY group_col"""

    return result


def format_aggregation_advice(result: Dict[str, Any]) -> str:
    """분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅"""

    rec = result["recommendation"]
    conf = result["confidence"]

    lines = []

    if rec == "window_function":
        lines.append(f"[AGGREGATION ADVICE] Recommend: WINDOW FUNCTION (confidence: {conf:.0%})")
    elif rec == "group_by":
        lines.append(f"[AGGREGATION ADVICE] Recommend: GROUP BY (confidence: {conf:.0%})")
    else:
        lines.append(f"[AGGREGATION ADVICE] No aggregation needed (confidence: {conf:.0%})")

    lines.append(f"Reason: {result['reason']}")

    if result.get("warning"):
        lines.append(f"WARNING: {result['warning']}")

    lines.append("")
    lines.append("Example pattern:")
    lines.append(result.get("example_pattern", "N/A"))

    return "\n".join(lines)


# 테스트
if __name__ == "__main__":
    # Test 1: Window function case
    r1 = check_aggregation_pattern(
        question="List the unique course instructor names, course titles, and the amount of material for each course instructor key",
        tables=["LIBRARY_COURSE_INSTRUCTOR", "LIBRARY_SUBJECT_OFFERED"],
        conn_info={},
        db_id="dw"
    )
    print("Test 1 (should be window_function):", r1["recommendation"], r1["confidence"])
    print("Reason:", r1["reason"])
    print()

    # Test 2: GROUP BY case
    r2 = check_aggregation_pattern(
        question="What is the total number of courses for each department?",
        tables=["COURSES", "DEPARTMENTS"],
        conn_info={},
        db_id="dw"
    )
    print("Test 2 (should be group_by):", r2["recommendation"], r2["confidence"])
    print("Reason:", r2["reason"])
    print()

    # Test 3: Complex case
    r3 = check_aggregation_pattern(
        question="What are the subject titles, their material titles, ISBN numbers, new shelf prices, and total costs of new materials for each subject title?",
        tables=["TIP_SUBJECT_OFFERED", "TIP_MATERIAL"],
        conn_info={},
        db_id="dw"
    )
    print("Test 3 (should be window_function):", r3["recommendation"], r3["confidence"])
    print("Reason:", r3["reason"])
