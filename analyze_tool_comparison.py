#!/usr/bin/env python3
"""
Analyze the intersection of incorrect predictions across different tool configurations
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Set

def load_evaluation_results(result_path: str) -> Dict:
    """Load evaluation results from JSON file"""
    with open(result_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_incorrect_questions(eval_results: Dict, base_path: str) -> Set[int]:
    """Extract question IDs that were answered incorrectly"""
    incorrect = set()

    # Check if results have per-question breakdown
    if 'per_item' in eval_results:
        for item in eval_results['per_item']:
            if not item.get('exec_match', False):  # execution accuracy
                incorrect.add(item.get('question_id', item.get('index')))
    # Check if we need to load exec_results_detail.json
    elif 'exec_results_path' in eval_results:
        detail_path = eval_results['exec_results_path']
        # Convert relative path to absolute
        if not os.path.isabs(detail_path):
            detail_path = os.path.join(os.path.dirname(base_path), os.path.basename(detail_path))

        if os.path.exists(detail_path):
            with open(detail_path, 'r', encoding='utf-8') as f:
                details = json.load(f)
                for item in details:
                    # res=1 means correct, res=0 means incorrect
                    if item.get('res', 0) == 0:
                        incorrect.add(item.get('sql_idx', item.get('index')))

    return incorrect

def analyze_tool_comparison(base_dir: str, model_prefix: str = "4o"):
    """
    Analyze incorrect predictions across three tool configurations

    Args:
        base_dir: Base output directory
        model_prefix: Model prefix ("4omini" or "4o")
    """

    # Define the three configurations
    configs = {
        'path_only': f'20260106_{model_prefix}_path',
        'insp_only': f'20260106_{model_prefix}_insp',
        'both': f'20260106_{model_prefix}_both'
    }

    print(f"\n{'='*80}")
    print(f"Tool Comparison Analysis - {model_prefix.upper()}")
    print(f"{'='*80}\n")

    # Load results for each configuration
    results = {}
    incorrect_sets = {}

    for name, folder in configs.items():
        eval_file = os.path.join(base_dir, folder, 'evaluation_results.json')

        if not os.path.exists(eval_file):
            print(f"[!] {name}: evaluation_results.json not found")
            continue

        results[name] = load_evaluation_results(eval_file)
        incorrect_sets[name] = get_incorrect_questions(results[name], eval_file)

        total = results[name].get('total', results[name].get('count', 0))
        correct = results[name].get('correct', results[name].get('exec', 0))
        incorrect = total - correct
        accuracy = (correct / total * 100) if total > 0 else 0

        print(f"[*] {name:15s}: {correct:3d}/{total:3d} correct ({accuracy:5.2f}%) | {incorrect:3d} incorrect")

    if len(incorrect_sets) < 3:
        print("\n[!] Not all three configurations have results. Aborting.")
        return

    # Calculate intersections
    print(f"\n{'-'*80}")
    print("Incorrect Question Analysis")
    print(f"{'-'*80}\n")

    path_only_set = incorrect_sets['path_only']
    insp_only_set = incorrect_sets['insp_only']
    both_set = incorrect_sets['both']

    # All three wrong
    all_three_wrong = path_only_set & insp_only_set & both_set

    # Pairwise intersections (excluding all_three)
    path_insp_only = (path_only_set & insp_only_set) - both_set
    path_both_only = (path_only_set & both_set) - insp_only_set
    insp_both_only = (insp_only_set & both_set) - path_only_set

    # Unique to each
    path_unique = path_only_set - insp_only_set - both_set
    insp_unique = insp_only_set - path_only_set - both_set
    both_unique = both_set - path_only_set - insp_only_set

    # Union of all incorrect
    all_incorrect = path_only_set | insp_only_set | both_set

    # All three correct (total - all_incorrect)
    total = results['path_only'].get('total', results['path_only'].get('count', 121))
    all_questions = set(range(total))
    all_three_correct = all_questions - all_incorrect

    print(f"Total Unique Questions with Errors: {len(all_incorrect)}")
    print(f"Total Questions All Three Correct: {len(all_three_correct)}")
    if all_three_correct:
        print(f"   IDs: {sorted(list(all_three_correct))}")
    print()

    # Venn diagram style output
    print("[ALL 3] All Three Wrong (Common Hard Cases):")
    print(f"   {len(all_three_wrong)} questions")
    if all_three_wrong:
        print(f"   Question IDs: {sorted(list(all_three_wrong))[:10]}{'...' if len(all_three_wrong) > 10 else ''}")
    print()

    print("[PAIR] Pairwise Intersections:")
    print(f"   path & insp (but both correct): {len(path_insp_only)} questions")
    if path_insp_only:
        print(f"      IDs: {sorted(list(path_insp_only))}")
    print(f"   path & both (but insp correct): {len(path_both_only)} questions")
    if path_both_only:
        print(f"      IDs: {sorted(list(path_both_only))}")
    print(f"   insp & both (but path correct): {len(insp_both_only)} questions")
    if insp_both_only:
        print(f"      IDs: {sorted(list(insp_both_only))}")
    print()

    print("[UNIQUE] Unique Errors (Only one config wrong):")
    print(f"   Only path_only wrong: {len(path_unique)} questions")
    if path_unique:
        print(f"      IDs: {sorted(list(path_unique))}")
    print(f"   Only insp_only wrong: {len(insp_unique)} questions")
    if insp_unique:
        print(f"      IDs: {sorted(list(insp_unique))}")
    print(f"   Only both wrong:      {len(both_unique)} questions")
    if both_unique:
        print(f"      IDs: {sorted(list(both_unique))}")
    print()

    # Calculate percentages
    total_incorrect_instances = len(path_only_set) + len(insp_only_set) + len(both_set)

    print(f"{'-'*80}")
    print("Overlap Statistics")
    print(f"{'-'*80}\n")

    overlap_rate = (len(all_three_wrong) / len(all_incorrect) * 100) if all_incorrect else 0
    print(f"Overlap Rate (all three wrong): {overlap_rate:.1f}%")
    print(f"  → {len(all_three_wrong)} out of {len(all_incorrect)} unique errors")
    print()

    # Tool effectiveness
    print("Tool Effectiveness (Questions fixed by using tools):")
    baseline = path_only_set  # Assuming path_only as baseline

    fixed_by_insp = len(path_only_set - insp_only_set)
    fixed_by_both = len(path_only_set - both_set)

    print(f"  path_only → insp_only: {fixed_by_insp:3d} questions fixed ({fixed_by_insp/len(baseline)*100:.1f}%)")
    print(f"  path_only → both:      {fixed_by_both:3d} questions fixed ({fixed_by_both/len(baseline)*100:.1f}%)")
    print()

    # New errors introduced
    new_by_insp = len(insp_only_set - path_only_set)
    new_by_both = len(both_set - path_only_set)

    print(f"New Errors Introduced:")
    print(f"  path_only → insp_only: {new_by_insp:3d} new errors")
    print(f"  path_only → both:      {new_by_both:3d} new errors")
    print()

    # Net improvement
    net_insp = fixed_by_insp - new_by_insp
    net_both = fixed_by_both - new_by_both

    print(f"Net Improvement:")
    print(f"  insp_only vs path_only: {net_insp:+3d} questions")
    print(f"  both vs path_only:      {net_both:+3d} questions")
    print()

    print(f"{'='*80}\n")

    # Save detailed results
    output = {
        'model': model_prefix,
        'configs': configs,
        'summary': {
            config: {
                'total': results[config].get('total', results[config].get('count', 0)),
                'correct': results[config].get('correct', results[config].get('exec', 0)),
                'incorrect': len(incorrect_sets[config]),
                'accuracy': results[config].get('correct', results[config].get('exec', 0)) / results[config].get('total', results[config].get('count', 1)) * 100
            }
            for config in results.keys()
        },
        'intersection': {
            'all_three_wrong': list(all_three_wrong),
            'all_three_wrong_count': len(all_three_wrong),
            'unique_errors_total': len(all_incorrect),
            'overlap_rate': overlap_rate,
            'path_insp_only': list(path_insp_only),
            'path_both_only': list(path_both_only),
            'insp_both_only': list(insp_both_only),
            'path_unique': list(path_unique),
            'insp_unique': list(insp_unique),
            'both_unique': list(both_unique)
        },
        'effectiveness': {
            'fixed_by_insp': fixed_by_insp,
            'fixed_by_both': fixed_by_both,
            'new_by_insp': new_by_insp,
            'new_by_both': new_by_both,
            'net_insp': net_insp,
            'net_both': net_both
        }
    }

    output_file = os.path.join(base_dir, f'tool_comparison_{model_prefix}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] Detailed analysis saved to: {output_file}\n")


if __name__ == "__main__":
    base_dir = "./outputs"

    # Analyze both 4omini and 4o if available
    for model in ["4omini", "4o"]:
        try:
            analyze_tool_comparison(base_dir, model)
        except Exception as e:
            print(f"[!] Could not analyze {model}: {e}\n")
