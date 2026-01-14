"""
단일 Baseline 결과 파일 분석
하나의 baseline 결과 파일 내 여러 run들의 교집합을 분석합니다.

Usage:
    python analyze_single_baseline.py 4o_baseline_3.json
"""

import json
import sys
from collections import defaultdict
from typing import Dict, List, Set


def analyze_single_file(file_path: str):
    """단일 파일 내 여러 run 분석"""

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    num_runs = data.get('num_runs', 1)
    all_runs = data.get('all_runs', [])

    print(f"\n📂 파일: {file_path}")
    print(f"   Run 횟수: {num_runs}")
    print(f"   총 문항: {len(all_runs[0]) if all_runs else 0}")

    if num_runs < 2 or len(all_runs) < 2:
        print("\n⚠️ 2회 이상 run이 필요합니다. 단일 run 결과입니다.")
        # 단일 run이면 그냥 요약만 표시
        summary = data.get('analysis', {}).get('summary', {})
        print(f"\n📊 요약:")
        for key, val in summary.items():
            print(f"   {key}: {val}")
        return

    # sql_idx별로 각 run의 결과 수집
    by_idx = defaultdict(lambda: {'question': '', 'results': []})

    for run_id, run_results in enumerate(all_runs):
        for item in run_results:
            idx = item['sql_idx']
            by_idx[idx]['question'] = item.get('question', '')
            by_idx[idx]['results'].append({
                'run_id': run_id,
                'error_type': item.get('error_type'),
                'success': item.get('success', False),
                'row_count': item.get('row_count', 0),
                'error': item.get('error', '')
            })

    # 분류
    all_syntax = set()      # 모든 run에서 syntax error
    all_empty = set()       # 모든 run에서 empty result
    all_timeout = set()     # 모든 run에서 timeout
    all_success = set()     # 모든 run에서 성공
    inconsistent = set()    # run마다 결과가 다름
    any_problem = set()     # 하나 이상 run에서 문제

    # N-1 이상
    syntax_count = defaultdict(int)
    empty_count = defaultdict(int)
    timeout_count = defaultdict(int)
    problem_count = defaultdict(int)

    for idx, data_item in by_idx.items():
        results = data_item['results']

        # 각 run별 상태 확인
        syntax_runs = sum(1 for r in results if r['error_type'] == 'syntax_error')
        empty_runs = sum(1 for r in results if r['error_type'] == 'empty_result')
        timeout_runs = sum(1 for r in results if r['error_type'] == 'timeout')
        success_runs = sum(1 for r in results if r['success'] and r['row_count'] > 0)

        # 카운트 누적
        syntax_count[idx] = syntax_runs
        empty_count[idx] = empty_runs
        timeout_count[idx] = timeout_runs

        # 문제 발생 여부 (syntax, empty, timeout 중 하나라도)
        problem_runs = sum(1 for r in results if r['error_type'] in ['syntax_error', 'empty_result', 'timeout'])
        problem_count[idx] = problem_runs

        # 모든 run에서 동일한 결과
        if syntax_runs == num_runs:
            all_syntax.add(idx)
        elif empty_runs == num_runs:
            all_empty.add(idx)
        elif timeout_runs == num_runs:
            all_timeout.add(idx)
        elif success_runs == num_runs:
            all_success.add(idx)
        else:
            inconsistent.add(idx)

        # 하나 이상 run에서 문제
        if problem_runs > 0:
            any_problem.add(idx)

    # N-1 이상 교집합
    n_minus_1 = num_runs - 1
    syntax_n_minus_1 = {idx for idx, cnt in syntax_count.items() if cnt >= n_minus_1}
    empty_n_minus_1 = {idx for idx, cnt in empty_count.items() if cnt >= n_minus_1}
    timeout_n_minus_1 = {idx for idx, cnt in timeout_count.items() if cnt >= n_minus_1}
    problem_n_minus_1 = {idx for idx, cnt in problem_count.items() if cnt >= n_minus_1}

    # ===== 출력 =====
    print(f"\n{'='*60}")
    print("🔴 SYNTAX ERROR 분석")
    print('='*60)
    print(f"\n✅ 모든 run({num_runs}회)에서 syntax error: {len(all_syntax)}개")
    if all_syntax:
        print(f"   sql_idx: {sorted(all_syntax)}")

    if num_runs >= 3:
        only_n_minus_1 = syntax_n_minus_1 - all_syntax
        print(f"\n✅ {n_minus_1}회 이상에서 syntax error: {len(syntax_n_minus_1)}개")
        if only_n_minus_1:
            print(f"   (전체 제외, {n_minus_1}회만): {sorted(only_n_minus_1)}")

    print(f"\n{'='*60}")
    print("🟡 EMPTY RESULT 분석")
    print('='*60)
    print(f"\n✅ 모든 run({num_runs}회)에서 empty result: {len(all_empty)}개")
    if all_empty:
        print(f"   sql_idx: {sorted(all_empty)}")

    if num_runs >= 3:
        only_n_minus_1 = empty_n_minus_1 - all_empty
        print(f"\n✅ {n_minus_1}회 이상에서 empty result: {len(empty_n_minus_1)}개")
        if only_n_minus_1:
            print(f"   (전체 제외, {n_minus_1}회만): {sorted(only_n_minus_1)}")

    print(f"\n{'='*60}")
    print("⏱️ TIMEOUT 분석")
    print('='*60)
    print(f"\n✅ 모든 run({num_runs}회)에서 timeout: {len(all_timeout)}개")
    if all_timeout:
        print(f"   sql_idx: {sorted(all_timeout)}")

    if num_runs >= 3:
        only_n_minus_1 = timeout_n_minus_1 - all_timeout
        print(f"\n✅ {n_minus_1}회 이상에서 timeout: {len(timeout_n_minus_1)}개")
        if only_n_minus_1:
            print(f"   (전체 제외, {n_minus_1}회만): {sorted(only_n_minus_1)}")

    print(f"\n{'='*60}")
    print("⚠️ ANY PROBLEM (Syntax OR Empty OR Timeout) 분석")
    print('='*60)

    all_problem = all_syntax | all_empty | all_timeout
    print(f"\n✅ 모든 run({num_runs}회)에서 문제: {len(all_problem)}개")
    if all_problem:
        print(f"   sql_idx: {sorted(all_problem)}")

        # 세부 분류
        if all_syntax:
            print(f"\n   - 항상 syntax error: {sorted(all_syntax)}")
        if all_empty:
            print(f"   - 항상 empty result: {sorted(all_empty)}")
        if all_timeout:
            print(f"   - 항상 timeout: {sorted(all_timeout)}")

    if num_runs >= 3:
        only_n_minus_1 = problem_n_minus_1 - all_problem
        print(f"\n✅ {n_minus_1}회 이상에서 문제: {len(problem_n_minus_1)}개")
        if only_n_minus_1:
            print(f"   (전체 제외, {n_minus_1}회만): {sorted(only_n_minus_1)}")

    # ===== 요약 =====
    print(f"\n{'='*60}")
    print("🔀 INCONSISTENT (run마다 결과 다름) 분석")
    print('='*60)
    print(f"\n✅ 일관되지 않은 결과: {len(inconsistent)}개")
    if inconsistent:
        print(f"   sql_idx: {sorted(inconsistent)}")

    print(f"\n{'='*60}")
    print("📊 요약")
    print('='*60)
    print(f"총 문항: {len(by_idx)}개")
    print(f"Run 횟수: {num_runs}회")
    print(f"")
    print(f"모든 run에서 일관된 결과:")
    print(f"  - Syntax Error: {len(all_syntax)}개")
    print(f"  - Empty Result: {len(all_empty)}개")
    print(f"  - Timeout: {len(all_timeout)}개")
    print(f"  - 모든 성공: {len(all_success)}개")
    print(f"  - Inconsistent: {len(inconsistent)}개")
    print(f"  합계: {len(all_syntax) + len(all_empty) + len(all_timeout) + len(all_success) + len(inconsistent)}개")

    if num_runs >= 3:
        print(f"")
        print(f"{n_minus_1}회 이상에서 반복:")
        print(f"  - Syntax Error: {len(syntax_n_minus_1)}개")
        print(f"  - Empty Result: {len(empty_n_minus_1)}개")
        print(f"  - Timeout: {len(timeout_n_minus_1)}개")
        print(f"  - 문제 합계: {len(problem_n_minus_1)}개")

    # 반환값
    return {
        'all_runs': {
            'syntax_errors': sorted(all_syntax),
            'empty_results': sorted(all_empty),
            'timeouts': sorted(all_timeout),
            'all_problem': sorted(all_problem),
            'all_success': sorted(all_success),
            'inconsistent': sorted(inconsistent)
        },
        'n_minus_1': {
            'syntax_errors': sorted(syntax_n_minus_1),
            'empty_results': sorted(empty_n_minus_1),
            'timeouts': sorted(timeout_n_minus_1),
            'all_problem': sorted(problem_n_minus_1)
        } if num_runs >= 3 else None
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_single_baseline.py <baseline_file.json>")
        sys.exit(1)

    file_path = sys.argv[1]
    analyze_single_file(file_path)


if __name__ == "__main__":
    main()
