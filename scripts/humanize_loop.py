"""
Humanize Loop: verify loop의 최적화된(biased) NLQ를 실제 사용자 질문처럼 다듬기

verify_loop에서 gold SQL을 보며 최적화된 NLQ는 모델 편향이 있음.
이 스크립트는 해당 NLQ를 자연스럽게 다듬되, SQL 생성 정확도를 유지하는 타협점을 찾음.

5회 시도 모두 실행 (성공해도 계속 → 더 자연스러운 버전 탐색).

Usage:
  python scripts/humanize_loop.py --config configs/beaver_dw_openai.yaml --test_n 3
  python scripts/humanize_loop.py --config configs/beaver_dw_openai.yaml --use_gold_nlq
  python scripts/humanize_loop.py --config configs/beaver_dw_openai.yaml --max_tries 5
"""

import json, os, csv, argparse, yaml, sys
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import create_engine, text
from func_timeout import func_timeout, FunctionTimedOut

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from src.utils.nlq_condition_hints import generate_nlq_condition_hints

MAX_TRIES = 5

# ── 프롬프트 ─────────────────────────────────────────────────────
SYSTEM_HUMANIZE = """You are refining a natural language question (NLQ) to sound like a real user's question.

IMPORTANT CONTEXT:
- This NLQ was created by an AI model that could see the gold SQL answer, then iteratively revised the NLQ until it guided SQL generation to the correct answer.
- As a result, the NLQ contains MODEL BIAS: overly specific hints, unnatural phrasing, or details that a real user would NEVER include in their question.
- Your job: make it sound like a REAL, NON-TECHNICAL USER's question while preserving enough information for correct SQL generation.

{gold_nlq_section}

This is adjustment {attempt}/{max_tries}. {context_message}

Guidelines:
- Real users do NOT specify: exact aggregation functions, sort directions, DISTINCT, window function details, JOIN logic, HAVING conditions
- Real users DO specify: what they want to know, key filters (department, year, status), rough intent (list, count, compare, rank)
- Remove phrases that feel like "hints planted for the SQL generator"
- Find the SWEET SPOT: natural enough to pass as a real question, specific enough for SQL generation
- The NLQ MUST be in English
- Output ONLY the revised question, nothing else"""

SYSTEM_ADJUST_FAIL = """You are adjusting a humanized NLQ that FAILED to produce the correct SQL.

IMPORTANT CONTEXT:
- The original NLQ was created by a model with gold SQL access (contains bias).
- You previously humanized it, but the SQL generator couldn't produce the correct SQL from your version.
- Something critical was lost during humanization.

{gold_nlq_section}

This is adjustment {attempt}/{max_tries}. {context_message}

Your task:
- Compare the Generated SQL (wrong) with the Gold SQL (correct) to identify what info was lost.
- Add back ONLY the minimum critical information to fix the SQL generation.
- Keep the natural, human-like tone — don't revert to the biased original.
- The NLQ MUST be in English
- Output ONLY the revised question, nothing else"""

SYSTEM_ADJUST_SUCCESS = """You are further refining a humanized NLQ that SUCCESSFULLY produced the correct SQL.

IMPORTANT CONTEXT:
- The original NLQ was created by a model with gold SQL access (contains bias).
- Your current version works, but it might still contain traces of model bias or unnatural phrasing.
- Try to make it EVEN MORE natural while keeping it correct.

{gold_nlq_section}

This is adjustment {attempt}/{max_tries}. {context_message}

Your task:
- Try to remove any remaining "planted hints" or overly specific details
- Make it sound more casual and natural
- But be careful: if you remove too much, SQL generation will fail
- If you believe the current NLQ is already perfectly natural, you may return it unchanged
- The NLQ MUST be in English
- Output ONLY the revised question, nothing else"""


def build_hints_text(item):
    parts = []
    evidence = item.get('evidence', '')
    if evidence:
        parts.append(f"Evidence: {evidence}")
    mapping = item.get('mapping', {})
    if mapping:
        lines = [f"- '{k}' → {', '.join(v)}" for k, v in mapping.items()]
        parts.append("Mappings:\n" + "\n".join(lines))
    join_keys = item.get('join_keys', [])
    if join_keys:
        jstr = ", ".join([f"({p[0]} = {p[1]})" for p in join_keys])
        parts.append(f"Join Keys: {jstr}")
    return "\n\n".join(parts)


def build_prompt_hints(item):
    """main.py process_item과 동일"""
    all_hints = []
    evidence = item.get('evidence', '')
    if evidence:
        all_hints.append(evidence)
    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' is related to {', '.join(columns)}"
                         for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping Hints:\n" + "\n".join(mapping_texts))
    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Information: {join_str}")
    return "\n\n".join(all_hints)


def get_mysql_url(conn_info, db_id):
    return (f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}"
            f"@{conn_info['host']}:{conn_info['port']}/{db_id}")


def execute_and_compare(predicted_sql, gold_sql, db_id, conn_info, timeout=90):
    def _run():
        engine = create_engine(get_mysql_url(conn_info, db_id))
        with engine.connect() as conn:
            try:
                pred_res = conn.execute(text(predicted_sql)).fetchall()
            except Exception as e:
                return False, f"Execution error: {str(e)[:200]}"
            gold_res = conn.execute(text(gold_sql)).fetchall()
        return set(pred_res) == set(gold_res), None
    try:
        return func_timeout(timeout, _run)
    except FunctionTimedOut:
        return False, "Timeout"
    except Exception as e:
        return False, f"Error: {str(e)[:200]}"


def generate_sql(client, model_name, prompt):
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = response.choices[0].message.content.strip()
    if content.startswith("```sql"):
        content = content[6:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


def context_message(attempt, max_tries, prev_results):
    remaining = max_tries - attempt
    if attempt == 1:
        return f"This is your first attempt. You have {remaining} adjustments remaining after this."
    successes = sum(1 for r in prev_results if r)
    failures = sum(1 for r in prev_results if not r)
    return (f"Previous results: {successes} success(es), {failures} failure(s). "
            f"You have {remaining} adjustment(s) remaining after this.")


def humanize_or_adjust(client, model_name, attempt, max_tries, biased_nlq, current_nlq,
                        hints_text, gold_sql, prev_sql, prev_match, prev_results,
                        gold_nlq_section):
    ctx = context_message(attempt, max_tries, prev_results)

    if attempt == 1:
        system = SYSTEM_HUMANIZE.format(
            gold_nlq_section=gold_nlq_section,
            attempt=attempt, max_tries=max_tries,
            context_message=ctx
        )
        user_prompt = f"""{hints_text}

Biased NLQ (optimized with gold SQL access — contains model bias):
{biased_nlq}

Rewrite this as a natural user question:"""

    elif prev_match:
        system = SYSTEM_ADJUST_SUCCESS.format(
            gold_nlq_section=gold_nlq_section,
            attempt=attempt, max_tries=max_tries,
            context_message=ctx
        )
        user_prompt = f"""{hints_text}

Biased NLQ (original): {biased_nlq}
Current NLQ (succeeded): {current_nlq}

Try to make it even more natural:"""

    else:
        system = SYSTEM_ADJUST_FAIL.format(
            gold_nlq_section=gold_nlq_section,
            attempt=attempt, max_tries=max_tries,
            context_message=ctx
        )
        user_prompt = f"""{hints_text}

Biased NLQ (original): {biased_nlq}
Current NLQ (failed): {current_nlq}

Generated SQL (wrong):
```sql
{prev_sql}
```

Gold SQL (correct):
```sql
{gold_sql}
```

Adjust the NLQ to fix the SQL generation failure:"""

    resp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )
    return resp.choices[0].message.content.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--verify_results", default="./analysis/verify_loop_results.json")
    parser.add_argument("--model", default=None)
    parser.add_argument("--max_tries", type=int, default=5)
    parser.add_argument("--test_n", type=int, default=None)
    parser.add_argument("--use_gold_nlq", action="store_true",
                        help="Gold NLQ의 길이/말투를 참고하도록 프롬프트에 포함")
    parser.add_argument("--output", default="./analysis/humanize_loop_results.json")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    model_name = args.model or config['model']['name']
    db_type = config['dataset'].get('db_type', 'mysql')

    conn_info = config.get('db_connection', {}).copy()
    if conn_info.get('password') == 'from_env':
        conn_info['password'] = os.getenv('MYSQL_PASSWORD', '')

    data_loader = BeaverLoader(config)
    dataset = data_loader.load_data(load_views=False)

    with open(args.verify_results, 'r', encoding='utf-8') as f:
        vl_data = json.load(f)
    items = vl_data['items']

    if args.test_n:
        items = items[:args.test_n]

    client = OpenAI()
    print(f"Model: {model_name}")
    print(f"Items: {len(items)}, Max tries: {args.max_tries}")
    print(f"Use gold NLQ reference: {args.use_gold_nlq}")

    # 이어하기
    results = []
    done_indices = set()
    if os.path.exists(args.output):
        with open(args.output, 'r', encoding='utf-8') as f:
            prev = json.load(f)
        results = prev.get("items", [])
        done_indices = {r['idx'] for r in results}
        print(f"Resuming: {len(done_indices)} items already done")

    for item_i, vl_item in enumerate(items):
        idx = vl_item['idx']
        if idx in done_indices:
            continue

        biased_nlq = vl_item['final_nlq']
        gold_nlq = vl_item['gold_nlq']
        gold_sql = vl_item['gold_sql']
        data_item = dataset[idx]
        db_id = data_item['db_id']
        hints_text = build_hints_text(data_item)

        # Gold NLQ 참조 섹션
        if args.use_gold_nlq:
            gold_nlq_section = (
                f"STYLE REFERENCE — A real user's original question (mimic this length and tone):\n"
                f'"{gold_nlq}"'
            )
        else:
            gold_nlq_section = ""

        print(f"\n[{item_i+1}/{len(items)}] idx={idx}")

        current_nlq = biased_nlq
        prev_sql = None
        prev_match = None
        prev_results = []
        tries = []

        for attempt in range(1, args.max_tries + 1):
            # Step A: Humanize / Adjust
            current_nlq = humanize_or_adjust(
                client, model_name, attempt, args.max_tries,
                biased_nlq, current_nlq, hints_text, gold_sql,
                prev_sql, prev_match, prev_results,
                gold_nlq_section
            )

            # Step B: Generate SQL
            prompt_hints = build_prompt_hints(data_item)
            nlq_cond = generate_nlq_condition_hints(current_nlq)
            if nlq_cond:
                prompt_hints = f"{prompt_hints}\n\n{nlq_cond}" if prompt_hints else nlq_cond

            prompt = build_prompt(
                schema=data_item.get('formatted_schema', ''),
                question=current_nlq,
                db_name=db_id,
                db_type=db_type,
                hints=prompt_hints
            )

            try:
                gen_sql = generate_sql(client, model_name, prompt)
            except Exception as e:
                gen_sql = f"Error: {str(e)[:200]}"

            # Step C: Compare
            match, error = execute_and_compare(gen_sql, gold_sql, db_id, conn_info)

            tries.append({
                "attempt": attempt,
                "nlq": current_nlq,
                "sql": gen_sql,
                "match": match,
                "error": error
            })

            prev_sql = gen_sql
            prev_match = match
            prev_results.append(match)

            status = "OK" if match else f"FAIL{f' ({error})' if error else ''}"
            print(f"  try {attempt}: {status} | {current_nlq[:100]}")

        # 결과 정리
        success_tries = [t['attempt'] for t in tries if t['match']]
        last_success_sql = ""
        for t in reversed(tries):
            if t['match']:
                last_success_sql = t['sql']
                break

        item_result = {
            "idx": idx,
            "gold_nlq": gold_nlq,
            "gold_sql": gold_sql,
            "biased_nlq": biased_nlq,
            "final_nlq": current_nlq,
            "success_try": " ".join(str(s) for s in success_tries),
            "success_count": len(success_tries),
            "final_sql": last_success_sql,
        }
        for t in tries:
            item_result[f"nlq{t['attempt']}"] = t['nlq']
            item_result[f"sql{t['attempt']}"] = t['sql']

        item_result["tries"] = tries
        results.append(item_result)

        # 중간 저장
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump({
                "config": {
                    "model": model_name,
                    "max_tries": args.max_tries,
                    "use_gold_nlq": args.use_gold_nlq,
                    "n_items": len(items)
                },
                "items": results
            }, f, ensure_ascii=False, indent=2)

    # ── Summary ──
    n = len(items)
    dist = [0] * (args.max_tries + 1)  # dist[k] = k/5 success인 문항 수
    for r in results:
        dist[r['success_count']] += 1

    print(f"\n{'='*60}")
    print(f"HUMANIZE LOOP RESULTS ({len(results)} items, {args.max_tries} tries)")
    print(f"{'='*60}")
    for k in range(args.max_tries, -1, -1):
        label = f"{k}/{args.max_tries} successes"
        bar = "#" * dist[k]
        print(f"  {label:18s} {dist[k]:3d} items  {bar}")

    total_successes = sum(r['success_count'] for r in results)
    total_possible = len(results) * args.max_tries
    print(f"\n  Total: {total_successes}/{total_possible} tries succeeded "
          f"({100*total_successes/total_possible:.1f}%)")
    print(f"  Failed all: {dist[0]} items ({100*dist[0]/len(results):.1f}%)")

    # ── CSV ──
    csv_path = args.output.replace('.json', '.csv')
    csv_fields = ["idx", "gold_nlq", "gold_sql", "biased_nlq", "success_try", "success_count", "final_nlq", "final_sql"]
    for i in range(1, args.max_tries + 1):
        csv_fields.extend([f"nlq{i}", f"sql{i}"])

    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\nSaved to {args.output} and {csv_path}")


if __name__ == "__main__":
    main()
