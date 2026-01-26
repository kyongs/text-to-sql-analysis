"""
틀린 문제들을 분석하여 오류 패턴 파악
"""

import json
import re
from pathlib import Path
from collections import Counter


def analyze_errors(output_dir: str, num_samples: int = 20):
    """틀린 문제들의 패턴 분석"""
    output_path = Path(output_dir)

    # Load exec results
    with open(output_path / 'exec_results_detail.json', 'r') as f:
        exec_results = json.load(f)

    # Load predictions
    with open(output_path / 'predictions.json', 'r') as f:
        predictions = json.load(f)

    # Find incorrect ones
    incorrect_indices = [i for i, r in enumerate(exec_results) if r['res'] == 0]
    correct_indices = [i for i, r in enumerate(exec_results) if r['res'] == 1]

    print(f"=" * 80)
    print(f"Total: {len(exec_results)}, Correct: {len(correct_indices)}, Incorrect: {len(incorrect_indices)}")
    print(f"=" * 80)

    # 오류 패턴 분류
    error_patterns = Counter()

    for idx in incorrect_indices[:num_samples]:
        gold = exec_results[idx]['ground_truth'].upper()
        pred = predictions[idx]['predicted_sql'].upper()
        question = predictions[idx]['question']

        # 패턴 분석
        patterns = []

        # 1. Window Function vs GROUP BY
        if 'OVER' in gold and 'OVER' not in pred:
            patterns.append('MISSING_WINDOW_FUNCTION')
        if 'OVER' not in gold and 'OVER' in pred:
            patterns.append('UNNECESSARY_WINDOW_FUNCTION')

        # 2. COUNT DISTINCT 차이
        if 'COUNT(DISTINCT' in gold and 'COUNT(DISTINCT' not in pred:
            patterns.append('MISSING_COUNT_DISTINCT')
        if 'COUNT(DISTINCT' not in gold and 'COUNT(DISTINCT' in pred:
            patterns.append('UNNECESSARY_COUNT_DISTINCT')

        # 3. SUM 차이
        if 'SUM(' in gold and 'SUM(' not in pred:
            patterns.append('MISSING_SUM')

        # 4. GROUP BY 차이
        if 'GROUP BY' in gold and 'GROUP BY' not in pred:
            patterns.append('MISSING_GROUP_BY')
        if 'GROUP BY' not in gold and 'GROUP BY' in pred:
            patterns.append('UNNECESSARY_GROUP_BY')

        # 5. JOIN 개수 차이
        gold_joins = gold.count(' JOIN ')
        pred_joins = pred.count(' JOIN ')
        if gold_joins > pred_joins:
            patterns.append(f'MISSING_JOINS({gold_joins - pred_joins})')

        # 6. DISTINCT 차이
        if 'SELECT DISTINCT' in gold and 'SELECT DISTINCT' not in pred:
            patterns.append('MISSING_DISTINCT')
        if 'SELECT DISTINCT' not in gold and 'SELECT DISTINCT' in pred:
            patterns.append('UNNECESSARY_DISTINCT')

        # 7. Subquery 차이
        gold_subqueries = gold.count('SELECT') - 1
        pred_subqueries = pred.count('SELECT') - 1
        if gold_subqueries > 0 and pred_subqueries == 0:
            patterns.append('MISSING_SUBQUERY')

        # 8. ORDER BY 차이
        if 'ORDER BY' in gold and 'ORDER BY' not in pred:
            patterns.append('MISSING_ORDER_BY')

        # 9. CASE WHEN 차이
        if 'CASE WHEN' in gold and 'CASE WHEN' not in pred:
            patterns.append('MISSING_CASE_WHEN')

        # 10. 집계함수 전반
        agg_funcs = ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN(']
        gold_agg = sum(1 for f in agg_funcs if f in gold)
        pred_agg = sum(1 for f in agg_funcs if f in pred)
        if gold_agg > pred_agg + 1:
            patterns.append('MISSING_AGGREGATIONS')

        if not patterns:
            patterns.append('OTHER')

        for p in patterns:
            error_patterns[p] += 1

        # 상세 출력 (처음 5개만)
        if idx in incorrect_indices[:5]:
            print(f"\n{'='*80}")
            print(f"[Question #{idx}] {question[:100]}...")
            print(f"\nPatterns: {patterns}")
            print(f"\n--- Gold SQL (first 300 chars) ---")
            print(gold[:300])
            print(f"\n--- Predicted SQL (first 300 chars) ---")
            print(pred[:300])

    print(f"\n{'='*80}")
    print("ERROR PATTERN SUMMARY")
    print(f"{'='*80}")
    for pattern, count in error_patterns.most_common():
        pct = count / len(incorrect_indices) * 100
        print(f"  {pattern}: {count} ({pct:.1f}%)")


if __name__ == "__main__":
    import sys
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "outputs/20260121_4o_note_take"
    num_samples = int(sys.argv[2]) if len(sys.argv) > 2 else 88
    analyze_errors(output_dir, num_samples)
