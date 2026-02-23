"""
Aggregation Hint 실험
- Baseline vs Aggregation Hint 비교
- GROUP BY가 필요한 쿼리에서 성능 차이 측정
"""
import os
import sys
import json
import yaml
import argparse
from datetime import datetime

# Windows UTF-8 + unbuffered
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Force flush on every print
import functools
print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from src.utils.agg_hint import generate_agg_hint_from_item
from analysis.sql_grader import grade, execute_sql

client = OpenAI()


def load_data(config_path: str = "configs/beaver_dw_openai.yaml"):
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    loader = BeaverLoader(config)
    data = loader.load_data(load_views=False)
    return config, data


def build_hints(item: dict, include_agg_hint: bool = False) -> str:
    """힌트 조합"""
    all_hints = []

    # 기존 evidence
    evidence = item.get('evidence', '')
    if evidence:
        all_hints.append(evidence)

    # Schema mapping
    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' -> {', '.join(columns)}"
                        for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping:\n" + "\n".join(mapping_texts))

    # Join keys
    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Keys: {join_str}")

    # Aggregation hint (옵션)
    if include_agg_hint:
        agg_hint = generate_agg_hint_from_item(item)
        if agg_hint:
            all_hints.append(agg_hint)

    return "\n\n".join(all_hints)


def generate_sql(prompt: str, model: str = "gpt-4o") -> str:
    """SQL 생성"""
    is_new_model = "gpt-5" in model or "o1" in model or "o3" in model
    token_param = {"max_completion_tokens": 2000} if is_new_model else {"max_tokens": 2000}

    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        **token_param
    )
    content = response.choices[0].message.content.strip()

    if "```sql" in content:
        content = content.split("```sql")[1].split("```")[0].strip()
    elif "```" in content:
        content = content.split("```")[1].split("```")[0].strip()

    return content


def run_experiment(
    data: list,
    config: dict,
    model: str = "gpt-4o",
    indices: list = None,
    filter_groupby: bool = True
):
    """실험 실행"""
    results = []
    db_type = config['dataset'].get('db_type', 'mysql')

    # 필터링: GROUP BY가 필요한 쿼리만
    if filter_groupby:
        filtered_data = []
        for idx, item in enumerate(data):
            gold_sql = item.get('sql', item.get('SQL', ''))
            if 'GROUP BY' in gold_sql.upper():
                filtered_data.append((idx, item))
        print(f"Filtered to {len(filtered_data)} queries with GROUP BY")
    else:
        filtered_data = [(i, item) for i, item in enumerate(data)]

    # 인덱스 지정 시
    if indices:
        filtered_data = [(idx, data[idx]) for idx in indices if idx < len(data)]

    for idx, item in filtered_data:
        question = item['question']
        gold_sql = item.get('sql', item.get('SQL', ''))
        schema = item.get('formatted_schema', '')

        print(f"\n{'='*60}")
        print(f"[{idx}] {question[:60]}...")

        # Baseline (without agg hint)
        hints_baseline = build_hints(item, include_agg_hint=False)
        prompt_baseline = build_prompt(
            schema=schema,
            question=question,
            db_name=item['db_id'],
            db_type=db_type,
            hints=hints_baseline
        )

        print("  Generating baseline SQL...")
        sql_baseline = generate_sql(prompt_baseline, model)

        # With aggregation hint
        hints_agg = build_hints(item, include_agg_hint=True)
        prompt_agg = build_prompt(
            schema=schema,
            question=question,
            db_name=item['db_id'],
            db_type=db_type,
            hints=hints_agg
        )

        print("  Generating SQL with agg hint...")
        sql_agg = generate_sql(prompt_agg, model)

        # 채점
        score_baseline = grade(sql_baseline, gold_sql, verbose=False, analyze=False)
        score_agg = grade(sql_agg, gold_sql, verbose=False, analyze=False)

        result = {
            'idx': idx,
            'question': question,
            'gold_sql': gold_sql,
            'sql_baseline': sql_baseline,
            'sql_agg': sql_agg,
            'score_baseline': score_baseline,
            'score_agg': score_agg,
            'improved': score_agg > score_baseline,
            'regressed': score_agg < score_baseline
        }
        results.append(result)

        # 결과 출력
        status_base = "✓" if score_baseline else "✗"
        status_agg = "✓" if score_agg else "✗"
        change = ""
        if result['improved']:
            change = " ⬆️ IMPROVED"
        elif result['regressed']:
            change = " ⬇️ REGRESSED"

        print(f"  Baseline: {status_base} | With Hint: {status_agg}{change}")

    return results


def print_summary(results: list):
    """결과 요약"""
    total = len(results)
    if total == 0:
        print("No results")
        return

    correct_baseline = sum(r['score_baseline'] for r in results)
    correct_agg = sum(r['score_agg'] for r in results)
    improved = sum(1 for r in results if r['improved'])
    regressed = sum(1 for r in results if r['regressed'])

    print("\n" + "=" * 60)
    print("EXPERIMENT SUMMARY")
    print("=" * 60)
    print(f"Total queries: {total}")
    print(f"Baseline accuracy: {correct_baseline}/{total} ({correct_baseline/total*100:.1f}%)")
    print(f"With Agg Hint:     {correct_agg}/{total} ({correct_agg/total*100:.1f}%)")
    print(f"Improved: {improved} | Regressed: {regressed} | No change: {total - improved - regressed}")

    # 개선/퇴보 케이스 상세
    if improved > 0:
        print("\n--- Improved Cases ---")
        for r in results:
            if r['improved']:
                print(f"  [{r['idx']}] {r['question'][:50]}...")

    if regressed > 0:
        print("\n--- Regressed Cases ---")
        for r in results:
            if r['regressed']:
                print(f"  [{r['idx']}] {r['question'][:50]}...")


def main():
    parser = argparse.ArgumentParser(description="Aggregation Hint Experiment")
    parser.add_argument("--model", type=str, default="gpt-4o")
    parser.add_argument("--config", type=str, default="configs/beaver_dw_openai.yaml")
    parser.add_argument("--indices", type=int, nargs="+", help="Specific indices to test")
    parser.add_argument("--all", action="store_true", help="Test all queries (not just GROUP BY)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of queries")
    parser.add_argument("--save", type=str, help="Save results to JSON file")
    args = parser.parse_args()

    print(f"Loading data from {args.config}...")
    config, data = load_data(args.config)
    print(f"Loaded {len(data)} questions")

    results = run_experiment(
        data=data[:args.limit] if args.limit else data,
        config=config,
        model=args.model,
        indices=args.indices,
        filter_groupby=not args.all
    )

    print_summary(results)

    if args.save:
        with open(args.save, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {args.save}")


if __name__ == "__main__":
    main()
