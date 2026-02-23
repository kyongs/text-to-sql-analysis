# src/utils/nlq_condition_hints.py
"""
NLQ 키워드 기반 조건 힌트 생성기.
질문(NLQ)에 특정 키워드가 포함되면, 스키마만 봐서는 추측하기 어려운
WHERE 조건이나 필터 패턴을 힌트로 제공한다.
"""

import re

# (키워드 패턴, 힌트 텍스트) 리스트
# 패턴은 re.IGNORECASE로 매칭
NLQ_CONDITION_RULES = [
    {
        "pattern": r"street\s*address",
        "hint": "Filter condition: ADDRESS_PURPOSE = 'STREET'",
    },
]


def generate_nlq_condition_hints(question: str) -> str:
    """
    질문에서 키워드를 감지하여 조건 힌트를 생성한다.

    Args:
        question: 자연어 질문

    Returns:
        힌트 문자열 (해당 없으면 빈 문자열)
    """
    hints = []
    for rule in NLQ_CONDITION_RULES:
        if re.search(rule["pattern"], question, re.IGNORECASE):
            hints.append(rule["hint"])

    if not hints:
        return ""

    return "Implicit Filter Conditions:\n" + "\n".join(f"- {h}" for h in hints)
