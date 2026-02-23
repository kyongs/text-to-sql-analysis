"""
Ask Granularity Tool
집계 질문에서 사용자에게 원하는 granularity(결과 단위)를 물어보는 interactive tool.

두 가지 모드:
1. Interactive 모드: 실제 사용자가 선택
2. User Agent 모드: LLM2가 "사용자 역할"로 선택 (Gold SQL 참고)
"""

import os
import json
from openai import OpenAI
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()


# User Agent용 프롬프트
USER_AGENT_PROMPT = """당신은 SQL 질문에 대한 사용자 역할입니다.

질문과 여러 선택지가 주어졌을 때, **정답 SQL(Gold SQL)**을 참고하여
가장 적합한 선택지를 골라주세요.

## 판단 기준
1. Gold SQL의 GROUP BY 패턴 확인
   - GROUP BY col1 → 해당 컬럼만으로 그룹화하는 선택지
   - GROUP BY col1, col2 → 두 컬럼 조합 선택지
   - Window Function 사용 → OVER (PARTITION BY ...) 선택지

2. Gold SQL의 SELECT 절 확인
   - 집계값만 있으면 GROUP BY 선택지
   - 세부 행 + 집계값 같이 있으면 Window Function 선택지

## 출력 형식
반드시 다음 JSON 형식으로만 응답하세요:
{{
    "selected": "A",
    "reason": "Gold SQL이 GROUP BY instructor만 사용하므로 A가 적합"
}}

---

질문: {question}

선택지:
{options_text}

Gold SQL:
{gold_sql}

위 정보를 바탕으로 가장 적합한 선택지를 골라주세요.
"""


def user_agent_select(
    question: str,
    options: List[Dict[str, Any]],
    gold_sql: str,
    model_name: str = "gpt-4o-mini"
) -> Dict[str, Any]:
    """
    User Agent (LLM2)가 Gold SQL을 참고하여 선택지를 고릅니다.

    Args:
        question: 원본 자연어 질문
        options: 선택지 리스트
        gold_sql: 정답 SQL
        model_name: User Agent에 사용할 모델

    Returns:
        {
            "selected": "A",
            "reason": "선택 이유",
            "success": True/False,
            "error": None or str
        }
    """
    try:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return {"success": False, "error": "OPENAI_API_KEY not set"}

        client = OpenAI(api_key=api_key)

        # 선택지 텍스트로 변환
        options_text = ""
        for opt in options:
            options_text += f"[{opt['id']}] {opt['sql_pattern']}\n"
            options_text += f"    {opt['description']}\n"
            if opt.get('result_preview'):
                options_text += f"    예시: {opt['result_preview'][:100]}\n"
            options_text += "\n"

        prompt = USER_AGENT_PROMPT.format(
            question=question,
            options_text=options_text,
            gold_sql=gold_sql
        )

        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a user selecting an option. Always respond in valid JSON format."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content
        result = json.loads(content)

        selected = result.get("selected", "A")
        reason = result.get("reason", "")

        # 유효한 선택지인지 확인
        valid_ids = [opt.get("id") for opt in options]
        if selected not in valid_ids:
            selected = valid_ids[0]  # fallback to first option
            reason = f"Invalid selection '{selected}', falling back to {valid_ids[0]}"

        return {
            "success": True,
            "selected": selected,
            "reason": reason,
            "error": None
        }

    except Exception as e:
        return {
            "success": False,
            "selected": options[0].get("id", "A") if options else "A",
            "reason": f"Error: {str(e)}",
            "error": str(e)
        }


def format_options_for_display(options: List[Dict[str, Any]], reason: str = "") -> str:
    """
    LLM이 생성한 선택지를 사용자에게 보여줄 형태로 포맷팅합니다.

    Args:
        options: LLM이 생성한 선택지 리스트
        reason: 왜 clarification이 필요한지 설명

    Returns:
        포맷팅된 문자열
    """
    output = []
    output.append("=" * 70)
    output.append("🤔 AGGREGATION GRANULARITY - Please select one option")
    output.append("=" * 70)

    if reason:
        output.append(f"\n📋 {reason}")
        output.append("")

    output.append("💬 How would you like the results grouped?\n")

    for opt in options:
        opt_id = opt.get("id", "?")
        sql_pattern = opt.get("sql_pattern", "")
        description = opt.get("description", "")
        result_preview = opt.get("result_preview", "")

        output.append(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        output.append(f"[{opt_id}] {sql_pattern}")
        output.append(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        if description:
            output.append(f"    {description}")

        if result_preview:
            output.append(f"    결과 예시:")
            # 테이블 형태 들여쓰기
            for line in result_preview.strip().split('\n'):
                output.append(f"    {line}")

        output.append("")

    output.append("=" * 70)
    output.append(f"👉 Select: {' / '.join(opt.get('id', '?') for opt in options)}")
    output.append("=" * 70)

    return "\n".join(output)


def ask_granularity(
    question: str,
    options: List[Dict[str, Any]],
    reason: str = "",
    gold_sql: str = None,
    use_user_agent: bool = False,
    user_agent_model: str = "gpt-4o-mini"
) -> str:
    """
    Tool entry point: 선택지를 처리합니다.

    두 가지 모드:
    1. Interactive 모드 (use_user_agent=False): 선택지를 포맷팅하여 반환 (사용자가 선택)
    2. User Agent 모드 (use_user_agent=True): LLM2가 Gold SQL 보고 자동 선택

    Args:
        question: 원본 자연어 질문
        options: LLM이 생성한 선택지 리스트
        reason: clarification이 필요한 이유 (선택적)
        gold_sql: 정답 SQL (User Agent 모드에서 필요)
        use_user_agent: True면 LLM2가 자동 선택
        user_agent_model: User Agent에 사용할 모델

    Returns:
        - Interactive 모드: 포맷팅된 선택지 + METADATA
        - User Agent 모드: 선택 결과 + 이유 (바로 SQL 생성 가능)
    """

    # User Agent 모드: LLM2가 선택
    if use_user_agent and gold_sql:
        agent_result = user_agent_select(
            question=question,
            options=options,
            gold_sql=gold_sql,
            model_name=user_agent_model
        )

        selected_id = agent_result.get("selected", "A")
        selected_option = next((opt for opt in options if opt.get("id") == selected_id), options[0])
        sql_pattern = selected_option.get('sql_pattern', '')
        description = selected_option.get('description', '')

        # LLM1이 SQL을 생성하도록 명시적 지시 포함
        output = []
        output.append("=" * 70)
        output.append("USER SELECTED OPTION: " + selected_id)
        output.append("=" * 70)
        output.append("")
        output.append(f"The user chose option [{selected_id}]: {sql_pattern}")
        output.append(f"Description: {description}")
        output.append("")
        output.append("**IMPORTANT INSTRUCTION**")
        output.append(f"NOW you MUST generate the COMPLETE SQL query using the pattern: {sql_pattern}")
        output.append("Do NOT just output the option letter. Generate the full SQL query based on this pattern.")
        output.append("=" * 70)

        return "\n".join(output)

    # Interactive 모드: 사용자에게 선택지 표시
    formatted = format_options_for_display(options, reason)

    metadata = {
        "type": "USER_INPUT_REQUIRED",
        "question": question,
        "options": options,
        "valid_choices": [opt.get("id") for opt in options]
    }

    return f"{formatted}\n\n[METADATA]\n{json.dumps(metadata, ensure_ascii=False)}"


# CLI 테스트용
if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    test_question = "List the unique course instructor names, course titles, and the amount of material for each course instructor key and the key of subject offered."

    # LLM이 생성할 선택지 예시
    test_options = [
        {
            "id": "A",
            "sql_pattern": "GROUP BY COURSE_INSTRUCTOR_KEY",
            "description": "강사당 1행 - 각 강사의 총 자료 수",
            "result_preview": "| instructor | total_materials |\n| Kim Prof | 15 |\n| Lee Prof | 8 |"
        },
        {
            "id": "B",
            "sql_pattern": "GROUP BY COURSE_INSTRUCTOR_KEY, SUBJECT_OFFERED_KEY",
            "description": "강사-과목 조합당 1행 - 강사별 과목별 자료 수",
            "result_preview": "| instructor | subject | cnt |\n| Kim Prof | Math | 10 |\n| Kim Prof | Physics | 5 |"
        },
        {
            "id": "C",
            "sql_pattern": "COUNT(*) OVER (PARTITION BY COURSE_INSTRUCTOR_KEY)",
            "description": "자료당 1행 + 강사별 총계 컬럼 추가 (Window Function)",
            "result_preview": "| material | instructor | subject | instructor_total |\n| Book1 | Kim Prof | Math | 15 |\n| Book2 | Kim Prof | Math | 15 |"
        }
    ]

    test_reason = "질문에서 'for each course instructor key and subject'가 있지만, 결과 단위가 강사별/강사-과목별/자료별 중 어느 것인지 불명확"

    # Gold SQL 예시 (User Agent 테스트용)
    test_gold_sql = """
    SELECT COURSE_INSTRUCTOR_NAME, SUBJECT_TITLE, COUNT(CATALOG_ISBN)
    FROM LIBRARY_COURSE_INSTRUCTOR
    JOIN LIBRARY_RESERVE_CATALOG USING (LIBRARY_COURSE_INSTRUCTOR_KEY)
    JOIN LIBRARY_SUBJECT_OFFERED USING (LIBRARY_SUBJECT_OFFERED_KEY)
    GROUP BY LIBRARY_COURSE_INSTRUCTOR_KEY, LIBRARY_SUBJECT_OFFERED_KEY
    """

    # 모드 선택: --agent 플래그가 있으면 User Agent 모드
    use_agent = "--agent" in sys.argv

    if use_agent:
        print("=" * 70)
        print("🤖 USER AGENT MODE (LLM2가 Gold SQL 보고 자동 선택)")
        print("=" * 70)
        result = ask_granularity(
            test_question, test_options, test_reason,
            gold_sql=test_gold_sql,
            use_user_agent=True
        )
    else:
        print("=" * 70)
        print("👤 INTERACTIVE MODE (사용자가 선택)")
        print("=" * 70)
        result = ask_granularity(test_question, test_options, test_reason)

    print(result)
