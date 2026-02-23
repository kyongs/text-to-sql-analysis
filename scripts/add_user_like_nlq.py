"""
verify_loop_results.json의 final_nlq를 1회 humanize하여 user_like_nlq 필드 추가.

정보는 보존하되, 모델 편향(over-specific hints, SQL 용어 등)을 제거하고
실제 사용자가 물어볼 법한 자연스러운 질문으로 변환.

Usage:
  python scripts/add_user_like_nlq.py
  python scripts/add_user_like_nlq.py --input ./analysis/verify_loop_results.json --model gpt-4o
  python scripts/add_user_like_nlq.py --test_n 3
"""
import json, os, argparse
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

SYSTEM_HUMANIZE = """You are rewriting an AI-optimized question into a natural, user-like question.

CONTEXT:
- The input NLQ was iteratively refined by an AI that could see the correct SQL answer.
- As a result, it contains MODEL BIAS: overly specific aggregation details, unnatural phrasing, SQL-like hints.
- Your job: rewrite it as a question a REAL, NON-TECHNICAL USER would ask.

Rules:
- PRESERVE all key information: what data they want, filters (department, year, status), output columns, grouping intent
- REMOVE: exact aggregation function names (SUM, COUNT DISTINCT, etc.), window function hints, JOIN instructions, HAVING conditions, sort directions, DISTINCT mentions, subquery/CTE hints
- Real users say things like "how many", "total", "list", "for each", "broken down by", "top N"
- Keep it concise — real users don't write paragraphs
- The NLQ MUST be in English
- Output ONLY the revised question, nothing else"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="./analysis/verify_loop_results.json")
    parser.add_argument("--model", default="gpt-4o")
    parser.add_argument("--test_n", type=int, default=None)
    args = parser.parse_args()

    client = OpenAI()

    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    items = data['items']
    total = len(items)
    if args.test_n:
        items = items[:args.test_n]

    skipped = 0
    processed = 0

    for i, item in enumerate(items):
        # 이미 있으면 스킵
        if item.get('user_like_nlq'):
            skipped += 1
            continue

        final_nlq = item.get('final_nlq', '')
        if not final_nlq:
            continue

        print(f"[{i+1}/{len(items)}] idx={item['idx']}")

        resp = client.chat.completions.create(
            model=args.model,
            messages=[
                {"role": "system", "content": SYSTEM_HUMANIZE},
                {"role": "user", "content": f"Rewrite this as a natural user question:\n\n{final_nlq}"}
            ],
            temperature=0
        )
        user_like = resp.choices[0].message.content.strip()
        item['user_like_nlq'] = user_like
        processed += 1

        print(f"  IN:  {final_nlq[:120]}")
        print(f"  OUT: {user_like[:120]}")

        # 중간 저장
        with open(args.input, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Processed: {processed}, Skipped (already had): {skipped}")
    print(f"Saved to {args.input}")


if __name__ == "__main__":
    main()
