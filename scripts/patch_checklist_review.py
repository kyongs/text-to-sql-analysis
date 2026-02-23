"""
기존 nlq_analysis.json에 체크리스트 기반 gold_nlq_rev_v2 컬럼 추가

기존 gold_nlq_rev와 차이:
  - SQL을 절(clause)별로 분해하여 각 절이 NLQ에 반영됐는지 명시적 체크
  - 누락된 절만 자연어로 변환하여 NLQ에 삽입

Usage:
  python scripts/patch_checklist_review.py
  python scripts/patch_checklist_review.py --test_n 3
"""
import json, os, argparse
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

SYSTEM_REVIEW_CHECKLIST = """You are given a SQL query, schema hints, and a human-written natural language question (NLQ).

**Step 1 — Clause-by-clause checklist.**
Examine each SQL clause and mark whether the NLQ covers it:

  [SELECT]  columns / aggregation functions (COUNT, SUM, AVG, MIN, MAX) / DISTINCT
  [WHERE]   every filter condition and its literal value
  [JOIN]    all tables being joined and the relationship implied
  [GROUP BY] grouping key(s)
  [HAVING]  post-aggregation filter
  [ORDER BY] sort column(s) and direction (ASC/DESC)
  [LIMIT]   row limit / top-N
  [WINDOW]  window functions (OVER, PARTITION BY, ROW_NUMBER, RANK …)
  [SUBQUERY] nested conditions or derived tables
  [SET OPS] UNION / INTERSECT / EXCEPT

**Step 2 — Decision.**
- If EVERY clause present in the SQL is adequately covered by the NLQ → reply exactly: PASS
- If ANY clause is missing or unclear in the NLQ → go to Step 3.

**Step 3 — Minimal revision.**
- Copy the original NLQ verbatim as your base.
- Only INSERT the missing details for the uncovered clauses.
- Use natural, casual language — NO SQL jargon (do not write DISTINCT, GROUP BY, HAVING, etc.).
- Do NOT rephrase, reorganize, or rewrite parts that are already correct.
- The revised NLQ MUST be in English.
- Output ONLY the revised question, nothing else."""


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


def prompt_review(item, nlq):
    return f"""{build_hints(item)}

SQL:
```sql
{item['sql']}
```

NLQ: {nlq}

Run the clause-by-clause checklist, then reply PASS or the revised NLQ:"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="./data/beaver/dw/dw.json")
    parser.add_argument("--nlq_json", default="./analysis/nlq_analysis.json")
    parser.add_argument("--model", default="gpt-5.2-2025-12-11")
    parser.add_argument("--test_n", type=int, default=None)
    args = parser.parse_args()

    client = OpenAI()
    print(f"Model: {args.model}")

    with open(args.data, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    with open(args.nlq_json, 'r', encoding='utf-8') as f:
        nlq_data = json.load(f)

    if args.test_n:
        nlq_data = nlq_data[:args.test_n]

    print(f"Items: {len(nlq_data)}")

    for i, nlq_item in enumerate(nlq_data):
        idx = nlq_item['idx']
        gold_nlq = nlq_item['gold_nlq']
        item = dataset[idx]

        print(f"[{i+1}/{len(nlq_data)}] idx={idx}...", end=" ")

        resp = client.chat.completions.create(
            model=args.model,
            messages=[
                {"role": "system", "content": SYSTEM_REVIEW_CHECKLIST},
                {"role": "user", "content": prompt_review(item, gold_nlq)}
            ],
            temperature=0
        )
        raw = resp.choices[0].message.content.strip()
        is_pass = (raw == "PASS")

        nlq_item['gold_nlq_rev_v2'] = "" if is_pass else raw
        nlq_item['gold_nlq_rev_v2_pass'] = is_pass

        print(f"{'[PASS]' if is_pass else raw[:150]}")

    # 통계
    n = len(nlq_data)
    v1_pass = sum(1 for r in nlq_data if r.get('gold_nlq_rev_pass', False))
    v2_pass = sum(1 for r in nlq_data if r.get('gold_nlq_rev_v2_pass', False))

    print(f"\n=== gold_nlq_rev 비교 ({n} items) ===")
    print(f"  gold_nlq_rev    (v1, 기존)     PASS: {v1_pass}/{n} ({100*v1_pass/n:.1f}%)")
    print(f"  gold_nlq_rev_v2 (checklist)    PASS: {v2_pass}/{n} ({100*v2_pass/n:.1f}%)")

    # 저장
    with open(args.nlq_json, 'w', encoding='utf-8') as f:
        json.dump(nlq_data, f, ensure_ascii=False, indent=2)

    csv_path = args.nlq_json.replace('.json', '.csv')
    if os.path.exists(csv_path):
        import csv
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=nlq_data[0].keys())
            writer.writeheader()
            writer.writerows(nlq_data)

    print(f"\nSaved (columns appended to existing file)")


if __name__ == "__main__":
    main()
