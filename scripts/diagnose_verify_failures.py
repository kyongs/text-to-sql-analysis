"""
Verify Loop 실패 문항 진단 스크립트

기존 verify_loop_results.json의 iteration 이력을 기반으로,
7회 반복에도 gold SQL 재현에 실패한 문항들의 원인을 한국어로 분석.

Usage:
  python scripts/diagnose_verify_failures.py
  python scripts/diagnose_verify_failures.py --results ./analysis/verify_loop_results_4o.json
  python scripts/diagnose_verify_failures.py --test_n 3
"""
import json, os, argparse, csv
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

SYSTEM_DIAGNOSE = """You are an expert SQL analyst.
You will be shown the full history of attempts to generate a correct SQL query from a natural language question (NLQ).

Each attempt includes:
- The NLQ used (revised each iteration)
- The SQL generated from that NLQ
- Whether it matched the gold SQL execution result

Despite multiple revisions, ALL attempts failed to reproduce the gold SQL.

Your task: analyze WHY the NLQ revisions repeatedly failed, and identify the root cause.

Consider:
1. Is there a fundamental mismatch between what the NLQ describes and what the gold SQL does?
2. Are there specific SQL constructs (subqueries, window functions, CASE WHEN, etc.) that the NLQ cannot capture naturally?
3. Did the revisions keep fixing one thing but breaking another?
4. Is there ambiguity in the schema/mappings that makes it impossible for the NLQ to guide the SQL generator correctly?
5. Is the gold SQL itself unusual or overly complex?

Reply in Korean (한국어).
Structure your answer as:
1. **근본 원인** (1-2문장): 왜 실패했는지 핵심 이유
2. **반복 패턴** (2-3문장): iteration을 거치며 어떤 패턴으로 실패가 반복되었는지
3. **NLQ 한계** (1-2문장): 이 문항에서 NLQ만으로는 표현하기 어려운 부분이 무엇인지"""


def format_iteration_history(item):
    """iteration 이력을 프롬프트용 텍스트로 포맷"""
    lines = []
    for it in item['iterations']:
        status = "MATCH" if it['match'] else "FAIL"
        error = f" ({it['error']})" if it.get('error') else ""
        lines.append(f"--- Attempt {it['attempt']} [{status}{error}] ---")
        lines.append(f"NLQ: {it['nlq']}")
        lines.append(f"Generated SQL:\n{it['generated_sql']}")
        lines.append("")
    return "\n".join(lines)


def build_diagnosis_prompt(item, hints_text):
    history = format_iteration_history(item)
    return f"""Schema Hints:
{hints_text}

Gold SQL (correct answer):
```sql
{item['gold_sql']}
```

Gold NLQ (original human question):
{item['gold_nlq']}

=== Iteration History ({len(item['iterations'])} attempts, ALL FAILED) ===
{history}

위 반복 이력을 분석하여, 왜 NLQ 수정을 거듭해도 gold SQL 재현에 실패했는지 한국어로 설명해주세요."""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="./analysis/verify_loop_results.json")
    parser.add_argument("--data", default="./data/beaver/dw/dw.json")
    parser.add_argument("--model", default="gpt-5.2-2025-12-11")
    parser.add_argument("--test_n", type=int, default=None)
    parser.add_argument("--output", default="./analysis/verify_loop_diagnosis.csv")
    args = parser.parse_args()

    client = OpenAI()

    # 원본 데이터 (hints용)
    with open(args.data, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    # verify loop 결과
    with open(args.results, 'r', encoding='utf-8') as f:
        vl_data = json.load(f)

    # 실패 문항만 필터
    failed_items = [item for item in vl_data['items'] if item['success_iter'] == -1]
    print(f"Total failed items: {len(failed_items)}")

    if args.test_n:
        failed_items = failed_items[:args.test_n]

    # hints 텍스트 빌드
    def build_hints(item):
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

    rows = []
    for i, item in enumerate(failed_items):
        idx = item['idx']
        data_item = dataset[idx]
        hints_text = build_hints(data_item)

        print(f"\n[{i+1}/{len(failed_items)}] idx={idx}")

        prompt = build_diagnosis_prompt(item, hints_text)

        resp = client.chat.completions.create(
            model=args.model,
            messages=[
                {"role": "system", "content": SYSTEM_DIAGNOSE},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        diagnosis = resp.choices[0].message.content.strip()

        print(f"  {diagnosis[:150]}...")

        rows.append({
            "idx": idx,
            "gold_nlq": item['gold_nlq'],
            "gold_sql": item['gold_sql'],
            "final_nlq": item['final_nlq'],
            "num_iters": len(item['iterations']),
            "diagnosis": diagnosis
        })

    # CSV 저장
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # JSON 저장
    json_path = args.output.replace('.csv', '.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(rows)} diagnoses to {args.output}")


if __name__ == "__main__":
    main()
