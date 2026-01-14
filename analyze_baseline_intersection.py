"""
Baseline 결과 교집합 분석
여러 baseline 결과 파일에서 반복적으로 문제가 발생하는 문항들을 추출합니다.

Usage:
    python analyze_baseline_intersection.py file1.json file2.json file3.json
    python analyze_baseline_intersection.py baseline_analysis_*.json
"""

import json
import sys
import glob
from collections import defaultdict
from typing import List, Dict, Set


def load_problem_cases(file_path: str) -> Dict[str, Set[int]]:
    """
    JSON 파일에서 문제 케이스들의 sql_idx를 추출

    Returns:
        {
            'syntax_errors': {idx1, idx2, ...},
            'empty_results': {idx3, idx4, ...},
            'any_problem': {idx1, idx2, idx3, idx4, ...}  # syntax OR empty
        }
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    problem_cases = data.get('analysis', {}).get('problem_cases', {})

    syntax_errors = set()
    empty_results = set()

    for case in problem_cases.get('syntax_errors', []):
        syntax_errors.add(case['sql_idx'])

    for case in problem_cases.get('empty_results', []):
        empty_results.add(case['sql_idx'])

    any_problem = syntax_errors | empty_results

    return {
        'syntax_errors': syntax_errors,
        'empty_results': empty_results,
        'any_problem': any_problem
    }


def analyze_intersection(file_paths: List[str]):
    """여러 파일의 교집합 분석"""

    if len(file_paths) < 2:
        print("❌ 최소 2개 이상의 파일이 필요합니다.")
        return

    print(f"\n📂 분석 대상 파일: {len(file_paths)}개")
    for fp in file_paths:
        print(f"   - {fp}")

    # 각 파일에서 문제 케이스 로드
    all_problems = []
    for fp in file_paths:
        try:
            problems = load_problem_cases(fp)
            all_problems.append({
                'file': fp,
                'problems': problems
            })
            print(f"\n   {fp}:")
            print(f"      Syntax errors: {len(problems['syntax_errors'])}개 - {sorted(problems['syntax_errors'])}")
            print(f"      Empty results: {len(problems['empty_results'])}개 - {sorted(problems['empty_results'])}")
        except Exception as e:
            print(f"   ⚠️ {fp} 로드 실패: {e}")

    if len(all_problems) < 2:
        print("\n❌ 유효한 파일이 2개 미만입니다.")
        return

    n_files = len(all_problems)

    # ===== 1. Syntax Error 교집합 =====
    print(f"\n{'='*60}")
    print("🔴 SYNTAX ERROR 교집합 분석")
    print('='*60)

    # 모든 파일에서 발생 (N개 교집합)
    syntax_all = all_problems[0]['problems']['syntax_errors'].copy()
    for ap in all_problems[1:]:
        syntax_all &= ap['problems']['syntax_errors']

    print(f"\n✅ 모든 파일({n_files}개)에서 syntax error: {len(syntax_all)}개")
    if syntax_all:
        print(f"   sql_idx: {sorted(syntax_all)}")

    # N-1개 이상 파일에서 발생
    if n_files >= 3:
        syntax_count = defaultdict(int)
        for ap in all_problems:
            for idx in ap['problems']['syntax_errors']:
                syntax_count[idx] += 1

        syntax_n_minus_1 = {idx for idx, cnt in syntax_count.items() if cnt >= n_files - 1}
        syntax_only_n_minus_1 = syntax_n_minus_1 - syntax_all

        print(f"\n✅ {n_files - 1}개 이상 파일에서 syntax error: {len(syntax_n_minus_1)}개")
        if syntax_only_n_minus_1:
            print(f"   (모든 파일 제외, {n_files - 1}개만): {sorted(syntax_only_n_minus_1)}")

    # ===== 2. Empty Result 교집합 =====
    print(f"\n{'='*60}")
    print("🟡 EMPTY RESULT 교집합 분석")
    print('='*60)

    empty_all = all_problems[0]['problems']['empty_results'].copy()
    for ap in all_problems[1:]:
        empty_all &= ap['problems']['empty_results']

    print(f"\n✅ 모든 파일({n_files}개)에서 empty result: {len(empty_all)}개")
    if empty_all:
        print(f"   sql_idx: {sorted(empty_all)}")

    if n_files >= 3:
        empty_count = defaultdict(int)
        for ap in all_problems:
            for idx in ap['problems']['empty_results']:
                empty_count[idx] += 1

        empty_n_minus_1 = {idx for idx, cnt in empty_count.items() if cnt >= n_files - 1}
        empty_only_n_minus_1 = empty_n_minus_1 - empty_all

        print(f"\n✅ {n_files - 1}개 이상 파일에서 empty result: {len(empty_n_minus_1)}개")
        if empty_only_n_minus_1:
            print(f"   (모든 파일 제외, {n_files - 1}개만): {sorted(empty_only_n_minus_1)}")

    # ===== 3. Any Problem (Syntax OR Empty) 교집합 =====
    print(f"\n{'='*60}")
    print("⚠️ ANY PROBLEM (Syntax OR Empty) 교집합 분석")
    print('='*60)

    any_all = all_problems[0]['problems']['any_problem'].copy()
    for ap in all_problems[1:]:
        any_all &= ap['problems']['any_problem']

    print(f"\n✅ 모든 파일({n_files}개)에서 문제 발생: {len(any_all)}개")
    if any_all:
        print(f"   sql_idx: {sorted(any_all)}")

        # 세부 분류
        always_syntax = any_all & syntax_all
        always_empty = any_all & empty_all
        mixed = any_all - always_syntax - always_empty

        if always_syntax:
            print(f"\n   - 항상 syntax error: {sorted(always_syntax)}")
        if always_empty:
            print(f"   - 항상 empty result: {sorted(always_empty)}")
        if mixed:
            print(f"   - 혼합 (때때로 다른 유형): {sorted(mixed)}")

    if n_files >= 3:
        any_count = defaultdict(int)
        for ap in all_problems:
            for idx in ap['problems']['any_problem']:
                any_count[idx] += 1

        any_n_minus_1 = {idx for idx, cnt in any_count.items() if cnt >= n_files - 1}
        any_only_n_minus_1 = any_n_minus_1 - any_all

        print(f"\n✅ {n_files - 1}개 이상 파일에서 문제 발생: {len(any_n_minus_1)}개")
        if any_only_n_minus_1:
            print(f"   (모든 파일 제외, {n_files - 1}개만): {sorted(any_only_n_minus_1)}")

    # ===== 4. 요약 =====
    print(f"\n{'='*60}")
    print("📊 요약")
    print('='*60)
    print(f"분석 파일 수: {n_files}개")
    print(f"")
    print(f"모든 파일에서 반복:")
    print(f"  - Syntax Error: {len(syntax_all)}개")
    print(f"  - Empty Result: {len(empty_all)}개")
    print(f"  - 둘 중 하나 이상: {len(any_all)}개")

    if n_files >= 3:
        print(f"")
        print(f"{n_files - 1}개 이상 파일에서 반복:")
        print(f"  - Syntax Error: {len(syntax_n_minus_1)}개")
        print(f"  - Empty Result: {len(empty_n_minus_1)}개")
        print(f"  - 둘 중 하나 이상: {len(any_n_minus_1)}개")

    # 리스트 반환 (다른 스크립트에서 사용 가능)
    return {
        'all_files': {
            'syntax_errors': sorted(syntax_all),
            'empty_results': sorted(empty_all),
            'any_problem': sorted(any_all)
        },
        'n_minus_1': {
            'syntax_errors': sorted(syntax_n_minus_1) if n_files >= 3 else [],
            'empty_results': sorted(empty_n_minus_1) if n_files >= 3 else [],
            'any_problem': sorted(any_n_minus_1) if n_files >= 3 else []
        } if n_files >= 3 else None
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_baseline_intersection.py file1.json file2.json ...")
        print("       python analyze_baseline_intersection.py baseline_analysis_*.json")
        sys.exit(1)

    # glob 패턴 확장
    file_paths = []
    for arg in sys.argv[1:]:
        if '*' in arg:
            file_paths.extend(glob.glob(arg))
        else:
            file_paths.append(arg)

    # 중복 제거 및 정렬
    file_paths = sorted(set(file_paths))

    analyze_intersection(file_paths)


if __name__ == "__main__":
    main()
