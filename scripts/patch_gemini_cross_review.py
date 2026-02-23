"""
기존 nlq_analysis.json에 Gemini cross-review 컬럼 추가 (기존 결과 보존)

Usage:
  python scripts/patch_gemini_cross_review.py
  python scripts/patch_gemini_cross_review.py --gemini_model gemini-2.5-flash
  python scripts/patch_gemini_cross_review.py --test_n 3
"""
import json, os, argparse
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

SYSTEM_REVIEW = """You are given a SQL query, schema hints, and a natural language question (NLQ).
Your task: judge whether the NLQ contains enough information to reconstruct the SQL query.

Check:
- Are all WHERE filters mentioned in the NLQ?
- Are aggregation details clear (GROUP BY keys, aggregate functions)?
- Are JOIN relationships implied by the NLQ?
- Are sorting, DISTINCT, LIMIT, HAVING conditions reflected?
- Are window functions (OVER, PARTITION BY) described?

If the NLQ is sufficient to reconstruct the SQL:
  Reply with exactly: PASS

If the NLQ is missing information:
  Reply with the revised NLQ that adds the missing details.
  - Add missing information naturally into the question
  - Do NOT mention table/column names — use natural language from the Mappings
  - The revised NLQ MUST be in English
  - Output ONLY the revised question, nothing else"""


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

Can the SQL be fully reconstructed from this NLQ + Mappings + Join Keys?
If yes, reply: PASS
If no, reply with the revised NLQ (in English):"""


def call_gemini(model, system, user):
    full_prompt = f"{system}\n\n{user}"
    response = model.generate_content(full_prompt)
    if not response.parts:
        return "Error: Blocked or Empty Response"
    return response.text.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="./data/beaver/dw/dw.json")
    parser.add_argument("--nlq_json", default="./analysis/nlq_analysis.json")
    parser.add_argument("--gemini_model", default="gemini-2.5-flash", help="Gemini model name")
    parser.add_argument("--test_n", type=int, default=None)
    args = parser.parse_args()

    # Gemini 초기화
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY not set")
        return
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(args.gemini_model)
    print(f"Gemini model: {args.gemini_model}")

    # 원본 데이터 로드 (hints용)
    with open(args.data, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    # 기존 결과 로드
    with open(args.nlq_json, 'r', encoding='utf-8') as f:
        nlq_data = json.load(f)

    if args.test_n:
        nlq_data = nlq_data[:args.test_n]

    print(f"Items: {len(nlq_data)}")

    for i, nlq_item in enumerate(nlq_data):
        idx = nlq_item['idx']
        llm_nlq = nlq_item['llm_nlq']
        item = dataset[idx]

        print(f"[{i+1}/{len(nlq_data)}] idx={idx}...", end=" ")

        raw = call_gemini(model, SYSTEM_REVIEW, prompt_review(item, llm_nlq))
        is_pass = (raw == "PASS")
        nlq_item['llm_nlq_rev_cross_gemini'] = "" if is_pass else raw
        nlq_item['llm_nlq_rev_cross_gemini_pass'] = is_pass

        print(f"{'[PASS]' if is_pass else raw[:100]}")

    # 통계
    n = len(nlq_data)
    gemini_pass = sum(1 for r in nlq_data if r.get('llm_nlq_rev_cross_gemini_pass', False))
    existing_pass = sum(1 for r in nlq_data if r.get('llm_nlq_rev_cross_pass', False))

    print(f"\n=== Cross-Review 비교 ({n} items) ===")
    print(f"  llm_nlq_rev_cross (기존, same model)   PASS: {existing_pass}/{n} ({100*existing_pass/n:.1f}%)")
    print(f"  llm_nlq_rev_cross_gemini (new)          PASS: {gemini_pass}/{n} ({100*gemini_pass/n:.1f}%)")

    # 저장 (기존 파일 덮어쓰기)
    with open(args.nlq_json, 'w', encoding='utf-8') as f:
        json.dump(nlq_data, f, ensure_ascii=False, indent=2)

    # CSV도 갱신
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
