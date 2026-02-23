"""
Self-Consistency 실험
- LLM에게 최대 3개의 서로 다른 쿼리 구조를 구상하게 하고
- 각각 SQL을 생성하여 실행 결과 비교
- pass@1, pass@3 측정
"""
import os
import sys
import json
import re
import yaml
import argparse
from datetime import datetime

# Windows UTF-8 + unbuffered
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import functools
print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from analysis.sql_grader import grade, execute_sql

client = OpenAI()


def load_data(config_path: str = "configs/beaver_dw_openai.yaml"):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    loader = BeaverLoader(config)
    data = loader.load_data(load_views=False)
    return config, data


def build_hints(item: dict) -> str:
    all_hints = []
    evidence = item.get('evidence', '')
    if evidence:
        all_hints.append(evidence)

    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' -> {', '.join(columns)}"
                        for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping:\n" + "\n".join(mapping_texts))

    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Keys: {join_str}")

    return "\n\n".join(all_hints)


def call_llm(messages: list, model: str, temperature: float = 0, max_tokens: int = 4000) -> str:
    is_new_model = "gpt-5" in model or "o1" in model or "o3" in model
    token_param = {"max_completion_tokens": max_tokens} if is_new_model else {"max_tokens": max_tokens}

    params = {
        "model": model,
        "messages": messages,
        **token_param
    }
    if not is_new_model:
        params["temperature"] = temperature

    response = client.chat.completions.create(**params)
    return response.choices[0].message.content.strip()


def extract_sql(text: str) -> str:
    if "```sql" in text:
        return text.split("```sql")[1].split("```")[0].strip()
    elif "```" in text:
        parts = text.split("```")
        if len(parts) >= 2:
            return parts[1].split("```")[0].strip()
    select_match = re.search(r'(SELECT\s+.+?;)', text, re.DOTALL | re.IGNORECASE)
    if select_match:
        return select_match.group(1).strip()
    return text.strip()


def generate_candidates(schema: str, question: str, hints: str, model: str, db_type: str = "mysql", n: int = 3) -> list:
    """
    LLM에게 서로 다른 쿼리 구조를 N개 구상하게 하고 각각 SQL 생성

    Returns:
        [{"approach": str, "sql": str}, ...]
    """
    sql_dialect = "MySQL" if db_type == "mysql" else "SQLite"

    plan_prompt = f"""You are a {sql_dialect} SQL expert.
Given the schema, question, and hints below, propose up to {n} **structurally different** SQL approaches to answer the question.

Each approach should differ in query structure, for example:
- GROUP BY vs Window Function
- Subquery vs JOIN
- CTE vs inline subquery
- Different aggregation strategies
- Different JOIN orders or filtering strategies

If the question is simple and only one reasonable approach exists, just provide 1.

For each approach, write the complete SQL query.

### Schema ###
{schema}

### Question ###
{question}

(Hints: {hints})

### Output Format ###
Return exactly in this format (no extra text):

=== Approach 1: [brief description] ===
```sql
[SQL query]
```

=== Approach 2: [brief description] ===
```sql
[SQL query]
```

=== Approach 3: [brief description] ===
```sql
[SQL query]
```
"""

    system_msg = f"""You are a {sql_dialect} SQL expert. Generate structurally different SQL queries.
Follow the Hints/Mappings faithfully: use the exact columns specified rather than inventing alternatives.
Prefer INNER JOIN over LEFT JOIN unless the question explicitly requires it.
Only SELECT columns that the question asks for."""

    response = call_llm(
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": plan_prompt}
        ],
        model=model,
        temperature=0
    )

    # 파싱: === Approach N: description === 패턴으로 분리
    candidates = []
    approach_blocks = re.split(r'===\s*Approach\s*\d+\s*:', response)

    for block in approach_blocks[1:]:  # 첫 번째는 빈 문자열
        # description 추출
        desc_match = re.match(r'\s*([^=]+?)\s*===', block)
        description = desc_match.group(1).strip() if desc_match else "Unknown"

        # SQL 추출
        sql = extract_sql(block)
        if sql and sql.upper().startswith("SELECT"):
            candidates.append({
                "approach": description,
                "sql": sql
            })

    # 파싱 실패 시 전체를 하나의 SQL로
    if not candidates:
        sql = extract_sql(response)
        if sql:
            candidates.append({
                "approach": "Single approach",
                "sql": sql
            })

    return candidates[:n]


def run_experiment(data: list, config: dict, model: str, indices: list = None, n_candidates: int = 3):
    results = []
    db_type = config['dataset'].get('db_type', 'mysql')

    items = [(i, item) for i, item in enumerate(data)]
    if indices:
        items = [(idx, data[idx]) for idx in indices if idx < len(data)]

    for idx, item in items:
        question = item['question']
        gold_sql = item.get('sql', item.get('SQL', ''))
        schema = item.get('formatted_schema', '')
        hints = build_hints(item)

        print(f"\n{'='*60}")
        print(f"[{idx}] {question[:60]}...")

        # 후보 생성
        candidates = generate_candidates(
            schema=schema,
            question=question,
            hints=hints,
            model=model,
            db_type=db_type,
            n=n_candidates
        )

        print(f"  Generated {len(candidates)} candidate(s)")

        # 각 후보 채점
        candidate_results = []
        any_correct = False
        first_correct = False

        for i, cand in enumerate(candidates):
            score = grade(cand['sql'], gold_sql, verbose=False, analyze=False)
            candidate_results.append({
                "approach": cand['approach'],
                "sql": cand['sql'],
                "correct": bool(score)
            })
            if score:
                any_correct = True
                if i == 0:
                    first_correct = True

            status = "O" if score else "X"
            print(f"  [{i+1}] {status} {cand['approach'][:50]}")

        result = {
            'idx': idx,
            'question': question,
            'gold_sql': gold_sql,
            'candidates': candidate_results,
            'n_candidates': len(candidates),
            'pass_at_1': first_correct,
            'pass_at_n': any_correct
        }
        results.append(result)

    return results


def print_summary(results: list, n: int):
    total = len(results)
    if total == 0:
        print("No results")
        return

    pass_1 = sum(1 for r in results if r['pass_at_1'])
    pass_n = sum(1 for r in results if r['pass_at_n'])
    avg_candidates = sum(r['n_candidates'] for r in results) / total

    print(f"\n{'='*60}")
    print("SELF-CONSISTENCY EXPERIMENT SUMMARY")
    print(f"{'='*60}")
    print(f"Total queries: {total}")
    print(f"Avg candidates per query: {avg_candidates:.1f}")
    print(f"pass@1: {pass_1}/{total} ({pass_1/total*100:.1f}%)")
    print(f"pass@{n}: {pass_n}/{total} ({pass_n/total*100:.1f}%)")
    print(f"Gain from self-consistency: +{pass_n - pass_1} ({(pass_n - pass_1)/total*100:.1f}%)")

    # pass@n에서만 맞은 케이스 (self-consistency가 살린 케이스)
    saved = [r for r in results if r['pass_at_n'] and not r['pass_at_1']]
    if saved:
        print(f"\n--- Saved by self-consistency ({len(saved)}) ---")
        for r in saved:
            correct_approaches = [c['approach'] for c in r['candidates'] if c['correct']]
            print(f"  [{r['idx']}] {r['question'][:50]}...")
            print(f"    Correct approach(es): {correct_approaches}")

    # pass@1에서 맞았지만 다른 후보는 틀린 케이스
    fragile = [r for r in results if r['pass_at_1'] and
               any(not c['correct'] for c in r['candidates'][1:])]
    if fragile:
        print(f"\n--- Fragile (pass@1 correct but other candidates wrong): {len(fragile)} ---")


def main():
    parser = argparse.ArgumentParser(description="Self-Consistency SQL Experiment")
    parser.add_argument("--model", type=str, default="gpt-4o")
    parser.add_argument("--config", type=str, default="configs/beaver_dw_openai.yaml")
    parser.add_argument("--indices", type=int, nargs="+", help="Specific indices to test")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of queries")
    parser.add_argument("--n", type=int, default=3, help="Number of candidate approaches")
    parser.add_argument("--save", type=str, help="Save results to JSON file")
    args = parser.parse_args()

    print(f"Loading data from {args.config}...")
    config, data = load_data(args.config)
    print(f"Loaded {len(data)} questions")
    print(f"Model: {args.model}, Candidates: {args.n}")

    dataset = data[:args.limit] if args.limit else data

    results = run_experiment(
        data=dataset,
        config=config,
        model=args.model,
        indices=args.indices,
        n_candidates=args.n
    )

    print_summary(results, args.n)

    if args.save:
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {args.save}")


if __name__ == "__main__":
    main()
