"""
Interactive SQL Debug Tool
- 인덱스 지정하면 프롬프트/스키마/힌트로 SQL 생성
- 채점 후 대화형으로 질문 가능
"""
import os
import sys
import json
import yaml
from openai import OpenAI
from dotenv import load_dotenv

# Windows 콘솔 UTF-8 설정
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from src.utils.skeleton_hint import extract_skeleton_hints, format_skeleton_hint, ROLLUP_REPORT_TEMPLATE
from analysis.sql_grader import grade, execute_sql

client = OpenAI()

# ── ROLLUP Few-shot 예시 (테스트 대상 idx 제외하고 동적으로 선택) ──
ROLLUP_FEWSHOT_POOL = {
    85: {
        "question": "Group all rooms into whether or not they are assignable and the major use descriptions. For each group, state \"ASSIGNABLE\" if the room is assignable and \"NON-ASSIGNABLE\" otherwise, the major use description, the total number of rooms, total area, and average area. Include subtotals for each group and a grand total across all groups. The assignable status and major use descriptions should only be displayed when they differ from the previous row. Do not include rooms whose major use or major use description starts with 'ZUSE.' The results should be sorted by assignable status and major use description. The subtotal and grand total rows should not include assignable status or major use description.",
        "sql": "SELECT CASE WHEN LAG(assign) OVER (ORDER BY assign) = assign THEN NULL ELSE assign END AS assign, CASE WHEN LAG(muse) OVER (ORDER BY assign, muse_sort) = muse THEN NULL ELSE muse END AS muse, rmcnt, area, area_avg FROM (SELECT CASE WHEN GROUPING(ASSIGNABLE) = 1 THEN NULL ELSE CASE WHEN ASSIGNABLE = 1 THEN 'ASSIGNABLE' ELSE 'NON-ASSIGNABLE' END END AS assign, CASE WHEN GROUPING(FCLT_ROOMS.MAJOR_USE_DESC) = 1 AND GROUPING(ASSIGNABLE) = 0 THEN NULL ELSE FCLT_ROOMS.MAJOR_USE_DESC END AS muse, CASE WHEN GROUPING(FCLT_ROOMS.MAJOR_USE_DESC) = 1 AND GROUPING(ASSIGNABLE) = 0 THEN 'zzz' ELSE FCLT_ROOMS.MAJOR_USE_DESC END AS muse_sort, COUNT(ROOM) AS rmcnt, SUM(FCLT_ROOMS.AREA) AS area, AVG(FCLT_ROOMS.AREA) AS area_avg FROM FCLT_ROOMS JOIN FCLT_MAJOR_USE ON FCLT_MAJOR_USE.FCLT_MAJOR_USE_KEY = FCLT_ROOMS.FCLT_MAJOR_USE_KEY WHERE FCLT_MAJOR_USE.MAJOR_USE NOT LIKE 'ZUSE%' AND FCLT_MAJOR_USE.DESCRIPTION NOT LIKE 'ZUSE%' GROUP BY ASSIGNABLE, FCLT_ROOMS.MAJOR_USE_DESC WITH ROLLUP ORDER BY assign, muse_sort) tbltmp;"
    },
    89: {
        "question": "For each mailing list that involves subscribers that work in departments with names starting with 'Computer Science', state the ownership type, the list name, the number of owners, and the number of subscribers. Display the ownership type only if it differs from the previous entry. Include subtotals (the corresponding type field shoud be 'SUBTOTAL') for each ownership type and a grand total (the corresponding type field shoud be 'TOTAL') across all ownership types.",
        "sql": "SELECT CASE WHEN LAG(OWNERSHIP_TYPE) OVER (ORDER BY OWNERSHIP_TYPE_gr DESC, ML) = OWNERSHIP_TYPE THEN NULL ELSE OWNERSHIP_TYPE END AS OWNERSHIP_TYPE, ML, nr_owner, nr_member FROM (SELECT CASE WHEN OWNER_TYPE IS NULL AND MOIRA_LIST_NAME IS NOT NULL THEN CONCAT(OWNER_TYPE, '1') ELSE CONCAT(OWNER_TYPE, '2') END AS OWNERSHIP_TYPE_gr, CASE WHEN MOIRA_LIST_NAME IS NULL AND OWNER_TYPE IS NOT NULL THEN 'SUBTOTAL' WHEN MOIRA_LIST_NAME IS NULL AND OWNER_TYPE IS NULL THEN 'TOTAL' ELSE OWNER_TYPE END AS OWNERSHIP_TYPE, MOIRA_LIST_NAME AS ML, COUNT(DISTINCT OWNER) AS nr_owner, COUNT(DISTINCT MOIRA_LIST_MEMBER_MIT_ID) AS nr_member FROM MOIRA_LIST_OWNER mlo JOIN MOIRA_LIST_DETAIL mld ON mld.MOIRA_LIST_OWNER_KEY = mlo.MOIRA_LIST_OWNER_KEY JOIN MOIRA_LIST ml ON ml.MOIRA_LIST_KEY = mld.MOIRA_LIST_KEY JOIN EMPLOYEE_DIRECTORY ON MIT_ID = MOIRA_LIST_MEMBER_MIT_ID WHERE department_name LIKE 'Computer Science%' GROUP BY OWNER_TYPE, MOIRA_LIST_NAME WITH ROLLUP ORDER BY OWNER_TYPE DESC, MOIRA_LIST_NAME) tbltmp;"
    },
    97: {
        "question": "Group courses in the biology and chemistry department by whether they are current or not and the cluster type. For each group, list the current status ('CURRENT' if yes and 'NON-CURRENT' otherwise), cluster type, the maximum duration of courses (in terms of days), the maximum units, average duration (in terms of days), and average units. Include subtotals for each current status (the corresponding current status field is 'SUBTOTAL') and a grand total across all current status (the corresponding current status field is 'TOTAL'). Do not repeat the current status if it is the same as the previous row. Sort the table by current status and cluster type.",
        "sql": "SELECT CASE WHEN LAG(assign) OVER (ORDER BY IS_CURRENT_TERM DESC, assign, CLUSTER_TYPE ASC) = assign THEN NULL ELSE assign END AS assign, CLUSTER_TYPE, max_duration, max_units, avg_dur, avg_unit FROM ( SELECT CASE WHEN GROUPING(CLUSTER_TYPE) = 1 AND GROUPING(IS_CURRENT_TERM) = 0 THEN 'SUBTOTAL' WHEN GROUPING(CLUSTER_TYPE) = 1 AND GROUPING(IS_CURRENT_TERM) = 1 THEN 'TOTAL' ELSE CASE WHEN IS_CURRENT_TERM = 'Y' THEN 'CURRENT' ELSE 'NON-CURRENT' END END AS assign, IS_CURRENT_TERM, CLUSTER_TYPE, MAX(DATEDIFF(STR_TO_DATE(term_end_date, '%d-%b-%y'), STR_TO_DATE(term_start_date, '%d-%b-%y'))) AS max_duration, MAX(TOTAL_UNITS) AS max_units, AVG(DATEDIFF(STR_TO_DATE(term_end_date, '%d-%b-%y'), STR_TO_DATE(term_start_date, '%d-%b-%y'))) AS avg_dur, AVG(TOTAL_UNITS) AS avg_unit FROM SUBJECT_SUMMARY ss JOIN ACADEMIC_TERMS_ALL ata ON ata.TERM_CODE = ss.TERM_CODE WHERE CLUSTER_TYPE IS NOT NULL AND ss.department_name IN ('Chemistry', 'Biology') GROUP BY IS_CURRENT_TERM, CLUSTER_TYPE WITH ROLLUP ORDER BY IS_CURRENT_TERM DESC, assign, CLUSTER_TYPE ASC ) tbltmp;"
    },
    99: {
        "question": "Group master courses by department. For each group, state the name of the department, the master course, and formatted number of subcourses for each master course. Include subtotals for each department and a grand total across all departments. Display the department only if it differs from the previous entry. Subtotal and grand total rows should not include the department or master course fields.",
        "sql": "SELECT CASE WHEN LAG(dept) OVER (ORDER BY dept, dept_sort, master) = dept THEN NULL ELSE dept END AS dept, CASE WHEN master = 'zzz' THEN NULL ELSE master END AS master, subcnt FROM (SELECT CASE WHEN GROUPING(OFFER_DEPT_NAME) = 1 THEN 'TOTAL' WHEN GROUPING(MASTER_COURSE_NUMBER) = 1 THEN 'SUBTOTAL' ELSE OFFER_DEPT_NAME END AS dept, CASE WHEN GROUPING(OFFER_DEPT_NAME) = 1 THEN 'zzz' ELSE OFFER_DEPT_NAME END AS dept_sort, CASE WHEN GROUPING(MASTER_COURSE_NUMBER) = 1 THEN 'zzz' ELSE MASTER_COURSE_NUMBER END AS master, FORMAT(COUNT(DISTINCT ss.SUBJECT_ID), 0) AS subcnt FROM SUBJECT_SUMMARY ss JOIN MASTER_SUBJECT ms ON ms.MASTER_SUBJECT_KEY = ss.MASTER_SUBJECT_KEY GROUP BY OFFER_DEPT_NAME, MASTER_COURSE_NUMBER WITH ROLLUP ORDER BY dept_sort, master) tbltmp;"
    },
}

# Tool 정의
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_sql",
            "description": "Execute a SQL query against the database and return results. Use this to test your SQL queries before giving final answer.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "submit_final_sql",
            "description": "Submit your final SQL answer for grading. Only call this when you are confident in your answer.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The final SQL query to submit"
                    }
                },
                "required": ["sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_report_template",
            "description": "Get a SQL template for report-style queries that need subtotals, grand totals, and suppressed repeated labels. Call this when the question mentions: subtotals, grand totals, 'display only if differs from previous row', or grouped reports with hierarchical aggregation.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_similar_examples",
            "description": "Get similar solved SQL examples for reference. Call this when you need help with report-style queries involving subtotals/totals, ROLLUP, GROUPING(), or LAG() for label suppression.",
            "parameters": {
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                        "description": "Number of examples to return (1-3)",
                        "default": 2
                    }
                },
                "required": []
            }
        }
    }
]

def handle_tool_call(tool_call, gold_sql: str = None, current_idx: int = None):
    """Tool call 처리"""
    import json
    name = tool_call.function.name
    args = json.loads(tool_call.function.arguments)

    if name == "execute_sql":
        sql = args["sql"]
        print(f"\n🔧 [Tool: execute_sql]")
        print(f"   SQL: {sql[:100]}..." if len(sql) > 100 else f"   SQL: {sql}")

        result = execute_sql(sql)
        if result["success"]:
            rows = result["results"]
            count = result["count"]
            preview = str(rows[:5]) if count <= 5 else str(rows[:5]) + f"... ({count} total rows)"
            response = f"Success: {count} rows returned.\nPreview: {preview}"
        else:
            response = f"Error: {result['error']}"

        print(f"   Result: {response[:200]}")
        return response

    elif name == "submit_final_sql":
        sql = args["sql"]
        print(f"\n📤 [Tool: submit_final_sql]")
        print(f"   Final SQL: {sql}")

        if gold_sql:
            result = grade(sql, gold_sql, verbose=True)
            if result == 1:
                return "✅ CORRECT! Your SQL produces the same results as the gold standard."
            else:
                return "❌ INCORRECT. Your SQL produces different results from the gold standard."
        else:
            return "SQL submitted (no gold SQL to compare)."

    elif name == "get_report_template":
        print(f"\n📋 [Tool: get_report_template]")
        return ROLLUP_REPORT_TEMPLATE

    elif name == "get_similar_examples":
        count = args.get("count", 2)
        count = min(max(count, 1), 3)
        print(f"\n📚 [Tool: get_similar_examples] count={count}")

        # 현재 테스트 중인 idx는 제외
        available = {k: v for k, v in ROLLUP_FEWSHOT_POOL.items() if k != current_idx}
        selected = list(available.items())[:count]

        examples_text = []
        for i, (idx, ex) in enumerate(selected, 1):
            examples_text.append(
                f"Example {i} (idx={idx}):\n"
                f"Question: {ex['question']}\n"
                f"SQL:\n```sql\n{ex['sql']}\n```"
            )
        result = "\n\n---\n\n".join(examples_text)
        print(f"   Returned {len(selected)} examples")
        return result

    return "Unknown tool"

def load_data(config_path: str = "configs/beaver_dw_openai.yaml"):
    """Config와 데이터 로드"""
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    loader = BeaverLoader(config)
    data = loader.load_data(load_views=False)
    return config, data

def build_item_prompt(item, db_type: str = "mysql"):
    """Item에서 프롬프트 빌드"""
    db_id = item['db_id']
    question = item['question']
    schema = item.get('formatted_schema', '')
    evidence = item.get('evidence', '')

    all_hints = []
    if evidence:
        all_hints.append(evidence)

    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' is related to {', '.join(columns)}" for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping Hints:\n" + "\n".join(mapping_texts))

    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Information: {join_str}")

    final_hints = "\n\n".join(all_hints)

    prompt = build_prompt(
        schema=schema,
        question=question,
        db_name=db_id,
        db_type=db_type,
        hints=final_hints,
        use_tools=False
    )
    return prompt, schema, final_hints

def generate_sql(messages: list, model: str = "gpt-4o", use_tools: bool = False, gold_sql: str = None, current_idx: int = None) -> str:
    """대화 히스토리로 SQL 생성 (tool 사용 가능)"""
    import json

    # gpt-5.x 모델은 max_completion_tokens 사용
    is_new_model = "gpt-5" in model or "o1" in model or "o3" in model
    token_param = {"max_completion_tokens": 2000} if is_new_model else {"max_tokens": 2000}

    if use_tools:
        # Tool 사용 모드: LLM이 직접 SQL 실행 가능
        max_iterations = 10
        for i in range(max_iterations):
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
                temperature=0,
                **token_param
            )

            msg = response.choices[0].message

            # Tool call이 있으면 처리
            if msg.tool_calls:
                messages.append(msg)
                for tool_call in msg.tool_calls:
                    result = handle_tool_call(tool_call, gold_sql, current_idx=current_idx)

                    # submit_final_sql이면 종료
                    if tool_call.function.name == "submit_final_sql":
                        args = json.loads(tool_call.function.arguments)
                        return args["sql"]

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": result
                    })
            else:
                # Tool 없이 응답하면 SQL 추출
                content = msg.content.strip() if msg.content else ""
                if "```sql" in content:
                    return content.split("```sql")[1].split("```")[0].strip()
                elif "```" in content:
                    return content.split("```")[1].split("```")[0].strip()
                return content

        return "Error: Max iterations reached"
    else:
        # 기존 모드: 단순 생성
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0,
            **token_param
        )
        content = response.choices[0].message.content.strip()

        if "```sql" in content:
            content = content.split("```sql")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        return content

def interactive_session(idx: int, config: dict, data: list, model: str = "gpt-4o", use_tools: bool = False, override_nlq: str = None, override_gold_sql: str = None):
    """Interactive debugging session"""
    if idx < 0 or idx >= len(data):
        print(f"Invalid index. Range: 0-{len(data)-1}")
        return

    item = data[idx]
    gold_sql = override_gold_sql or item.get('sql', item.get('SQL', item.get('query', '')))

    if override_nlq:
        question = override_nlq
        item = dict(item)  # 원본 수정 방지
        item['question'] = override_nlq
    else:
        question = item['question']

    print("\n" + "=" * 60)
    print(f"📌 Index: {idx}")
    print(f"❓ Question: {question}")
    if override_nlq:
        print(f"   (custom NLQ)")
    if use_tools:
        print(f"🔧 Tool mode: ON (LLM can execute SQL)")
    print("=" * 60)

    # 프롬프트 빌드
    prompt, schema, hints = build_item_prompt(item, config['dataset'].get('db_type', 'mysql'))

    # Tool 모드면 프롬프트에 안내 추가
    if use_tools:
        tool_guide = "\n\n### Core Principle ###"
        tool_guide += "\nThe user has a SPECIFIC expected result set in mind. Your goal is to produce SQL that outputs EXACTLY that result."
        tool_guide += "\n- Stick faithfully to the logical structure described in the NLQ. Do NOT interpret it loosely or creatively."
        tool_guide += "\n- Do NOT add data preprocessing that the NLQ does not ask for (e.g., COALESCE, IFNULL, NULLIF, TRIM). If the NLQ doesn't mention handling NULLs, leave them as-is."
        tool_guide += "\n- Do NOT add extra columns, filters, or transformations beyond what is explicitly requested."
        tool_guide += "\n"
        tool_guide += "\n### Tools ###"
        tool_guide += "\n- execute_sql: Test your SQL query and see results"
        tool_guide += "\n- submit_final_sql: Submit your final answer for grading"
        tool_guide += "\n- get_report_template: Get a SQL template for report-style queries with subtotals/totals and repeated label suppression (ROLLUP + GROUPING + LAG pattern)"
        tool_guide += "\n- get_similar_examples: Get solved examples of similar report-style SQL queries for reference"
        tool_guide += "\n"
        tool_guide += "\n### Guidelines ###"
        tool_guide += "\n1. For any filter value NOT explicitly quoted in the question (e.g., department names, status codes, categories), you MUST use execute_sql to look up actual values in the column BEFORE using them in WHERE/HAVING clauses. Example: SELECT DISTINCT department_name FROM table; Do NOT guess or assume values exist."
        tool_guide += "\n2. If the question mentions subtotals, grand totals, or displaying a field only when it differs from the previous row, FIRST call get_report_template and get_similar_examples before writing SQL."
        tool_guide += "\n3. Use execute_sql to verify your query before submitting."
        prompt += tool_guide

    # 대화 히스토리 시작
    messages = [{"role": "user", "content": prompt}]

    print(f"\n🔄 Generating SQL with {model}...")
    predicted_sql = generate_sql(messages, model, use_tools=use_tools, gold_sql=gold_sql, current_idx=idx)

    # Tool 모드 후 대화용 메시지 정리 (tool_calls 제거)
    # 새로운 대화 히스토리: 원본 프롬프트 + 최종 SQL만 유지
    messages = [
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": f"Here is my SQL query:\n```sql\n{predicted_sql}\n```"}
    ]

    print(f"\n📝 Predicted SQL:\n{predicted_sql}")
    print(f"\n🎯 Gold SQL:\n{gold_sql}")

    # 채점
    print(f"\n⏳ Grading...")
    result = grade(predicted_sql, gold_sql, verbose=True)

    if result == 1:
        print("\n✅ CORRECT!")
    else:
        print("\n❌ INCORRECT")

        # 결과 비교 출력
        pred_res = execute_sql(predicted_sql)
        gold_res = execute_sql(gold_sql)

        if pred_res["success"] and gold_res["success"]:
            print(f"\n📊 Predicted rows: {pred_res['count']}, Gold rows: {gold_res['count']}")
            if pred_res['count'] <= 5:
                print(f"Predicted: {pred_res['results']}")
            if gold_res['count'] <= 5:
                print(f"Gold: {gold_res['results']}")

    # 대화형 모드
    print("\n" + "-" * 60)
    print("💬 대화 모드 명령어:")
    print("  q          - 종료")
    print("  schema     - 스키마 보기")
    print("  hints      - 힌트 보기")
    print("  gold       - 정답 SQL 보기")
    print("  retry      - SQL 재생성")
    print("  run <sql>  - SQL 실행해서 결과 보기")
    print("  grade <sql> - SQL 채점 (정답과 비교)")
    print("-" * 60)

    while True:
        try:
            user_input = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input:
            continue
        if user_input.lower() in ['q', 'quit', 'exit']:
            break
        if user_input.lower() == 'schema':
            print(f"\n📋 Schema:\n{schema[:2000]}..." if len(schema) > 2000 else f"\n📋 Schema:\n{schema}")
            continue
        if user_input.lower() == 'hints':
            print(f"\n💡 Hints:\n{hints}")
            continue
        if user_input.lower() == 'gold':
            print(f"\n🎯 Gold SQL:\n{gold_sql}")
            continue
        if user_input.lower() == 'retry':
            print(f"\n🔄 Re-generating SQL...")
            messages = [{"role": "user", "content": prompt}]
            predicted_sql = generate_sql(messages, model)
            messages.append({"role": "assistant", "content": predicted_sql})
            print(f"\n📝 New Predicted SQL:\n{predicted_sql}")
            result = grade(predicted_sql, gold_sql, verbose=True)
            print("✅ CORRECT!" if result == 1 else "❌ INCORRECT")
            continue

        # run <sql> - SQL 실행
        if user_input.lower().startswith('run '):
            sql = user_input[4:].strip()
            print(f"\n🔧 Executing SQL...")
            result = execute_sql(sql)
            if result["success"]:
                print(f"✅ Success: {result['count']} rows")
                for i, row in enumerate(result['results'][:10]):
                    print(f"  {row}")
                if result['count'] > 10:
                    print(f"  ... ({result['count']} total)")
            else:
                print(f"❌ Error: {result['error']}")
            continue

        # grade <sql> - SQL 채점
        if user_input.lower().startswith('grade '):
            sql = user_input[6:].strip()
            print(f"\n📤 Grading SQL...")
            result = grade(sql, gold_sql, verbose=True)
            if result == 1:
                print("✅ CORRECT!")
            else:
                print("❌ INCORRECT")
                # 결과 비교
                pred_res = execute_sql(sql)
                gold_res = execute_sql(gold_sql)
                if pred_res["success"] and gold_res["success"]:
                    print(f"\n📊 Your rows: {pred_res['count']}, Gold rows: {gold_res['count']}")
                    if pred_res['count'] <= 5:
                        print(f"Your result: {pred_res['results']}")
                    if gold_res['count'] <= 5:
                        print(f"Gold result: {gold_res['results']}")
            continue

        # 일반 대화 (tool 모드면 tool 사용 가능)
        messages.append({"role": "user", "content": user_input})

        # gpt-5.x 모델은 max_completion_tokens 사용
        is_new_model = "gpt-5" in model or "o1" in model or "o3" in model
        conv_token_param = {"max_completion_tokens": 1500} if is_new_model else {"max_tokens": 1500}

        if use_tools:
            # Tool 사용 가능 모드
            max_iter = 5
            for _ in range(max_iter):
                response = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    tools=TOOLS,
                    tool_choice="auto",
                    temperature=0.7,
                    **conv_token_param
                )
                msg = response.choices[0].message

                if msg.tool_calls:
                    messages.append(msg)
                    for tool_call in msg.tool_calls:
                        result = handle_tool_call(tool_call, gold_sql, current_idx=idx)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": result
                        })
                else:
                    reply = msg.content.strip() if msg.content else "(no response)"
                    messages.append({"role": "assistant", "content": reply})
                    print(f"\nAssistant: {reply}")
                    break
        else:
            # 기본 대화 모드
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=0.7,
                **conv_token_param
            )
            reply = response.choices[0].message.content.strip()
            messages.append({"role": "assistant", "content": reply})
            print(f"\nAssistant: {reply}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Interactive SQL Debug Tool")
    parser.add_argument("--idx", type=int, required=True, help="Question index")
    parser.add_argument("--model", type=str, default="gpt-4o", help="Model to use")
    parser.add_argument("--config", type=str, default="configs/beaver_dw_openai.yaml")
    parser.add_argument("--tools", action="store_true", help="Enable tool mode: LLM can execute SQL directly")
    parser.add_argument("--nlq", type=str, default=None, help="Override question with custom NLQ")
    parser.add_argument("--nlq_file", type=str, default=None, help="Load custom NLQ from text file")
    args = parser.parse_args()

    # --nlq_file이 있으면 파일에서 읽기 (--nlq보다 우선)
    # 포맷: NLQ만 있거나, ---SQL--- 구분자 아래에 gold SQL 포함 가능
    override_gold_sql = None
    if args.nlq_file:
        with open(args.nlq_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        if '---SQL---' in content:
            parts = content.split('---SQL---', 1)
            args.nlq = parts[0].strip()
            override_gold_sql = parts[1].strip()
        else:
            args.nlq = content

    print(f"🚀 Loading data from {args.config}...")
    config, data = load_data(args.config)
    print(f"✅ Loaded {len(data)} questions")

    interactive_session(args.idx, config, data, args.model, use_tools=args.tools, override_nlq=args.nlq, override_gold_sql=override_gold_sql)

if __name__ == "__main__":
    main()
