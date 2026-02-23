"""
Query Plans 3회 실행 결과를 피벗 형태로 변환
- 같은 질문의 run1, run2, run3 결과를 가로로 나열
- mismatch, method, pattern 세 항목을 연달아 비교
"""

import csv
import json
import sys
from pathlib import Path
from collections import defaultdict


def pivot_query_plans(input_csv: str, output_csv: str = None):
    """
    세로로 쌓인 3 runs 데이터를 가로 피벗 형태로 변환
    """
    if output_csv is None:
        input_path = Path(input_csv)
        output_csv = input_path.parent / f"{input_path.stem}_pivoted.csv"

    # CSV 로드
    with open(input_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # index별로 그룹핑
    by_index = defaultdict(dict)
    questions = {}

    for row in rows:
        idx = int(row['index'])
        run_id = row.get('run_id', 'run1')  # run1, run2, run3

        by_index[idx][run_id] = {
            'mismatch': row.get('mismatch', ''),
            'method': row.get('method', ''),
            'pattern': row.get('pattern', ''),
        }

        # question은 한 번만 저장
        if idx not in questions:
            questions[idx] = row.get('question', '')[:100]

    # 피벗 테이블 생성
    pivoted_rows = []

    for idx in sorted(by_index.keys()):
        runs_data = by_index[idx]

        row = {
            'index': idx,
            'question': questions.get(idx, ''),
        }

        # 실제 run_id 감지 (run0/run1/run2 또는 run1/run2/run3)
        run_ids = sorted(runs_data.keys())
        if not run_ids:
            run_ids = ['run1', 'run2', 'run3']

        # mismatch 비교 (run1, run2, run3 연달아)
        for i, run in enumerate(run_ids):
            data = runs_data.get(run, {})
            row[f'mismatch_r{i+1}'] = data.get('mismatch', '')

        # method 비교 (run1, run2, run3 연달아)
        for i, run in enumerate(run_ids):
            data = runs_data.get(run, {})
            row[f'method_r{i+1}'] = data.get('method', '')

        # pattern 비교 (run1, run2, run3 연달아)
        for i, run in enumerate(run_ids):
            data = runs_data.get(run, {})
            row[f'pattern_r{i+1}'] = data.get('pattern', '')[:80]  # 패턴은 길어서 자름

        # 일관성 체크
        mismatches = [runs_data.get(r, {}).get('mismatch', '') for r in run_ids]
        methods = [runs_data.get(r, {}).get('method', '') for r in run_ids]

        # 모든 run이 같은 결과인지
        mismatch_consistent = len(set(mismatches)) == 1
        method_consistent = len(set(methods)) == 1

        row['mismatch_consistent'] = '✓' if mismatch_consistent else '✗'
        row['method_consistent'] = '✓' if method_consistent else '✗'
        row['all_consistent'] = '✓' if (mismatch_consistent and method_consistent) else '✗'

        pivoted_rows.append(row)

    # CSV 저장
    fieldnames = [
        'index', 'question',
        'mismatch_r1', 'mismatch_r2', 'mismatch_r3', 'mismatch_consistent',
        'method_r1', 'method_r2', 'method_r3', 'method_consistent',
        'pattern_r1', 'pattern_r2', 'pattern_r3',
        'all_consistent'
    ]

    with open(output_csv, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(pivoted_rows)

    # 통계 출력
    total = len(pivoted_rows)
    mismatch_consistent_count = sum(1 for r in pivoted_rows if r['mismatch_consistent'] == '✓')
    method_consistent_count = sum(1 for r in pivoted_rows if r['method_consistent'] == '✓')
    all_consistent_count = sum(1 for r in pivoted_rows if r['all_consistent'] == '✓')

    print(f"Pivoted CSV saved to: {output_csv}")
    print(f"\n=== Consistency Statistics ===")
    print(f"Total questions: {total}")
    print(f"Mismatch consistent: {mismatch_consistent_count}/{total} ({mismatch_consistent_count/total*100:.1f}%)")
    print(f"Method consistent: {method_consistent_count}/{total} ({method_consistent_count/total*100:.1f}%)")
    print(f"All consistent: {all_consistent_count}/{total} ({all_consistent_count/total*100:.1f}%)")

    # 불일치 항목 출력
    inconsistent = [r for r in pivoted_rows if r['all_consistent'] == '✗']
    if inconsistent:
        print(f"\n=== Inconsistent Items ({len(inconsistent)}) ===")
        for r in inconsistent[:10]:  # 최대 10개만
            print(f"[{r['index']}] mismatch: {r['mismatch_r1']}/{r['mismatch_r2']}/{r['mismatch_r3']}, "
                  f"method: {r['method_r1']}/{r['method_r2']}/{r['method_r3']}")

    return output_csv


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # 기본값 사용
        input_csv = "outputs/query_plans_3runs_all_runs.csv"
    else:
        input_csv = sys.argv[1]

    output_csv = sys.argv[2] if len(sys.argv) > 2 else None

    pivot_query_plans(input_csv, output_csv)
