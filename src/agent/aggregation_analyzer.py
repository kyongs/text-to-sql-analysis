# src/agent/aggregation_analyzer.py
"""
Aggregation 방식 분석 Tool
- 질문과 스키마를 분석해서 GROUP BY vs Window Function 권장
"""

import re
from typing import Dict, Any, List


def analyze_aggregation_need(
    question: str,
    select_columns: List[str],
    aggregation_columns: List[str],
    group_by_columns: List[str]
) -> Dict[str, Any]:
    """
    질문과 SELECT 컬럼 구조를 분석해서 aggregation 방식을 권장합니다.

    Args:
        question: 자연어 질문
        select_columns: SELECT할 컬럼들 (예: ['instructor_name', 'course_title', 'COUNT(isbn)'])
        aggregation_columns: 집계 대상 컬럼들 (예: ['COUNT(isbn)', 'SUM(price)'])
        group_by_columns: GROUP BY할 컬럼들 (예: ['instructor_key'])

    Returns:
        {
            "recommendation": "window_function" | "group_by" | "uncertain",
            "confidence": float (0-1),
            "reason": str,
            "pattern_detected": str,
            "suggestion": str  # 구체적인 SQL 패턴 제안
        }
    """
    question_lower = question.lower()

    # 비집계 컬럼 수 계산
    non_agg_columns = [c for c in select_columns if c not in aggregation_columns]

    result = {
        "select_columns": select_columns,
        "aggregation_columns": aggregation_columns,
        "group_by_columns": group_by_columns,
        "non_aggregation_columns": non_agg_columns,
    }

    # 패턴 1: 개별 항목 + 집계 = Window Function
    # "List names, titles, AND total/count" 패턴
    detail_keywords = ['names', 'titles', 'isbn', 'address', 'description',
                       'their', 'its', 'each item', 'individual']
    has_detail_request = any(kw in question_lower for kw in detail_keywords)

    # 패턴 2: 집계만 = GROUP BY
    # "What is the count/total for each X" 패턴
    agg_only_patterns = [
        r'what is the (count|total|sum|average|number)',
        r'how many .* for each',
        r'give (me )?(the )?(count|total|number)',
    ]
    is_agg_only = any(re.search(p, question_lower) for p in agg_only_patterns)

    # 비집계 컬럼이 GROUP BY 컬럼보다 많으면 Window Function 가능성 높음
    # 예: SELECT name, title, isbn, COUNT(*) GROUP BY key
    # -> name, title, isbn은 GROUP BY에 없으므로 에러 또는 Window 필요
    extra_columns = len(non_agg_columns) - len(group_by_columns)

    # 판단 로직
    if extra_columns > 1 and has_detail_request:
        result["recommendation"] = "window_function"
        result["confidence"] = 0.8
        result["pattern_detected"] = "detail_with_aggregation"
        result["reason"] = f"SELECT에 {len(non_agg_columns)}개의 상세 컬럼이 있고, GROUP BY는 {len(group_by_columns)}개만 있습니다. 개별 row 정보와 집계값을 함께 보여줘야 합니다."
        result["suggestion"] = f"SUM/COUNT(...) OVER (PARTITION BY {', '.join(group_by_columns)}) 사용을 권장합니다."

    elif is_agg_only and extra_columns <= 0:
        result["recommendation"] = "group_by"
        result["confidence"] = 0.9
        result["pattern_detected"] = "aggregation_only"
        result["reason"] = "집계 결과만 요청하는 질문입니다."
        result["suggestion"] = f"GROUP BY {', '.join(group_by_columns)} 사용이 적절합니다."

    elif extra_columns > 1:
        result["recommendation"] = "window_function"
        result["confidence"] = 0.6
        result["pattern_detected"] = "column_mismatch"
        result["reason"] = f"SELECT 컬럼({len(non_agg_columns)}개)이 GROUP BY 컬럼({len(group_by_columns)}개)보다 많습니다. Window Function이 필요할 수 있습니다."
        result["suggestion"] = "GROUP BY에 없는 컬럼들을 유지하려면 Window Function을 사용하세요."

    else:
        result["recommendation"] = "group_by"
        result["confidence"] = 0.7
        result["pattern_detected"] = "standard_groupby"
        result["reason"] = "일반적인 GROUP BY 패턴입니다."
        result["suggestion"] = f"GROUP BY {', '.join(group_by_columns)} 사용이 적절합니다."

    return result


def format_aggregation_analysis(result: Dict[str, Any]) -> str:
    """분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅"""

    rec = result["recommendation"]
    conf = result["confidence"]

    if rec == "window_function":
        icon = "🪟"
        rec_text = "WINDOW FUNCTION"
    else:
        icon = "📊"
        rec_text = "GROUP BY"

    output = []
    output.append(f"{icon} Aggregation Analysis Result: **{rec_text}** (confidence: {conf:.0%})")
    output.append("")
    output.append(f"Pattern: {result['pattern_detected']}")
    output.append(f"Reason: {result['reason']}")
    output.append("")
    output.append(f"📝 Suggestion: {result['suggestion']}")

    if rec == "window_function":
        output.append("")
        output.append("⚠️  If you use GROUP BY instead:")
        output.append("   - Columns not in GROUP BY will cause errors or be aggregated incorrectly")
        output.append("   - Individual row details will be lost")
        output.append("")
        output.append("Example pattern:")
        output.append("   SELECT col1, col2, SUM(amount) OVER (PARTITION BY group_col) as total")
        output.append("   FROM table")
        output.append("   -- No GROUP BY needed, each row preserved with aggregated value")

    return "\n".join(output)


# 간단한 테스트
if __name__ == "__main__":
    # Test case 1: Window function needed
    result1 = analyze_aggregation_need(
        question="List the unique course instructor names, course titles, and the amount of material for each course instructor key",
        select_columns=["instructor_name", "course_title", "COUNT(isbn)"],
        aggregation_columns=["COUNT(isbn)"],
        group_by_columns=["instructor_key"]
    )
    print("Test 1 (should be window_function):")
    print(format_aggregation_analysis(result1))
    print()

    # Test case 2: GROUP BY sufficient
    result2 = analyze_aggregation_need(
        question="What is the total number of courses for each department?",
        select_columns=["department_name", "COUNT(course_id)"],
        aggregation_columns=["COUNT(course_id)"],
        group_by_columns=["department_name"]
    )
    print("Test 2 (should be group_by):")
    print(format_aggregation_analysis(result2))
