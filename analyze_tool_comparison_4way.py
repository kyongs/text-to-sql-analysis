#!/usr/bin/env python3
"""
Analyze the intersection of incorrect predictions across FOUR tool configurations including base
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

def analyze_tool_comparison_4way(base_dir: str, model_prefix: str = "4o"):
    """
    Analyze incorrect predictions across FOUR tool configurations (base + 3 tool variants)

    Args:
        base_dir: Base output directory
        model_prefix: Model prefix ("4omini" or "4o")
    """

    # Define the four configurations
    configs = {
        'base': f'20260106_{model_prefix}_base',
        'path_only': f'20260106_{model_prefix}_path',
        'insp_only': f'20260106_{model_prefix}_insp',
        'both': f'20260106_{model_prefix}_both'
    }

    print(f"\n{'='*80}")
    print(f"4-Way Tool Comparison Analysis - {model_prefix.upper()}")
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

    if len(incorrect_sets) < 4:
        print(f"\n[!] Only {len(incorrect_sets)}/4 configurations have results. Aborting.")
        return

    # Calculate intersections
    print(f"\n{'-'*80}")
    print("Incorrect Question Analysis")
    print(f"{'-'*80}\n")

    base_set = incorrect_sets['base']
    path_only_set = incorrect_sets['path_only']
    insp_only_set = incorrect_sets['insp_only']
    both_set = incorrect_sets['both']

    # All four wrong
    all_four_wrong = base_set & path_only_set & insp_only_set & both_set

    # Union of all incorrect
    all_incorrect = base_set | path_only_set | insp_only_set | both_set

    # All four correct
    total = results['base'].get('total', results['base'].get('count', 121))
    all_questions = set(range(total))
    all_four_correct = all_questions - all_incorrect

    print(f"Total Unique Questions with Errors: {len(all_incorrect)}")
    print(f"Total Questions All Four Correct: {len(all_four_correct)}")
    if all_four_correct:
        print(f"   IDs: {sorted(list(all_four_correct))}")
    print()

    # All four wrong
    print("[ALL 4] All Four Wrong (Hardest Cases):")
    print(f"   {len(all_four_wrong)} questions")
    if all_four_wrong:
        print(f"   Question IDs: {sorted(list(all_four_wrong))[:10]}{'...' if len(all_four_wrong) > 10 else ''}")
    print()

    # Three-way intersections (excluding all_four)
    print("[3-WAY] Three Wrong (but one correct):")

    base_path_insp = (base_set & path_only_set & insp_only_set) - both_set
    base_path_both = (base_set & path_only_set & both_set) - insp_only_set
    base_insp_both = (base_set & insp_only_set & both_set) - path_only_set
    path_insp_both = (path_only_set & insp_only_set & both_set) - base_set

    print(f"   base & path & insp (both correct): {len(base_path_insp)} questions")
    if base_path_insp:
        print(f"      IDs: {sorted(list(base_path_insp))}")
    print(f"   base & path & both (insp correct): {len(base_path_both)} questions")
    if base_path_both:
        print(f"      IDs: {sorted(list(base_path_both))}")
    print(f"   base & insp & both (path correct): {len(base_insp_both)} questions")
    if base_insp_both:
        print(f"      IDs: {sorted(list(base_insp_both))}")
    print(f"   path & insp & both (base correct): {len(path_insp_both)} questions")
    if path_insp_both:
        print(f"      IDs: {sorted(list(path_insp_both))}")
    print()

    # Pairwise intersections (excluding 3-way and 4-way)
    print("[PAIR] Exactly Two Wrong (two correct):")

    base_path = (base_set & path_only_set) - insp_only_set - both_set
    base_insp = (base_set & insp_only_set) - path_only_set - both_set
    base_both = (base_set & both_set) - path_only_set - insp_only_set
    path_insp = (path_only_set & insp_only_set) - base_set - both_set
    path_both = (path_only_set & both_set) - base_set - insp_only_set
    insp_both = (insp_only_set & both_set) - base_set - path_only_set

    print(f"   base & path: {len(base_path)} questions")
    if base_path:
        print(f"      IDs: {sorted(list(base_path))}")
    print(f"   base & insp: {len(base_insp)} questions")
    if base_insp:
        print(f"      IDs: {sorted(list(base_insp))}")
    print(f"   base & both: {len(base_both)} questions")
    if base_both:
        print(f"      IDs: {sorted(list(base_both))}")
    print(f"   path & insp: {len(path_insp)} questions")
    if path_insp:
        print(f"      IDs: {sorted(list(path_insp))}")
    print(f"   path & both: {len(path_both)} questions")
    if path_both:
        print(f"      IDs: {sorted(list(path_both))}")
    print(f"   insp & both: {len(insp_both)} questions")
    if insp_both:
        print(f"      IDs: {sorted(list(insp_both))}")
    print()

    # Unique to each (only one wrong)
    print("[UNIQUE] Unique Errors (Only one config wrong):")

    base_unique = base_set - path_only_set - insp_only_set - both_set
    path_unique = path_only_set - base_set - insp_only_set - both_set
    insp_unique = insp_only_set - base_set - path_only_set - both_set
    both_unique = both_set - base_set - path_only_set - insp_only_set

    print(f"   Only base wrong:      {len(base_unique)} questions")
    if base_unique:
        print(f"      IDs: {sorted(list(base_unique))}")
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

    # Statistics
    print(f"{'-'*80}")
    print("Overlap Statistics")
    print(f"{'-'*80}\n")

    overlap_rate = (len(all_four_wrong) / len(all_incorrect) * 100) if all_incorrect else 0
    print(f"Overlap Rate (all four wrong): {overlap_rate:.1f}%")
    print(f"  → {len(all_four_wrong)} out of {len(all_incorrect)} unique errors")
    print()

    # Tool effectiveness (compared to base)
    print("Tool Effectiveness vs BASE (Questions fixed by using tools):")
    baseline = base_set

    fixed_by_path = len(base_set - path_only_set)
    fixed_by_insp = len(base_set - insp_only_set)
    fixed_by_both = len(base_set - both_set)

    print(f"  base → path_only: {fixed_by_path:3d} questions fixed ({fixed_by_path/len(baseline)*100:.1f}%)")
    print(f"  base → insp_only: {fixed_by_insp:3d} questions fixed ({fixed_by_insp/len(baseline)*100:.1f}%)")
    print(f"  base → both:      {fixed_by_both:3d} questions fixed ({fixed_by_both/len(baseline)*100:.1f}%)")
    print()

    # New errors introduced
    new_by_path = len(path_only_set - base_set)
    new_by_insp = len(insp_only_set - base_set)
    new_by_both = len(both_set - base_set)

    print(f"New Errors Introduced vs BASE:")
    print(f"  base → path_only: {new_by_path:3d} new errors")
    print(f"  base → insp_only: {new_by_insp:3d} new errors")
    print(f"  base → both:      {new_by_both:3d} new errors")
    print()

    # Net improvement
    net_path = fixed_by_path - new_by_path
    net_insp = fixed_by_insp - new_by_insp
    net_both = fixed_by_both - new_by_both

    print(f"Net Improvement vs BASE:")
    print(f"  path_only: {net_path:+3d} questions")
    print(f"  insp_only: {net_insp:+3d} questions")
    print(f"  both:      {net_both:+3d} questions")
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
            'all_four_wrong': sorted(list(all_four_wrong)),
            'all_four_wrong_count': len(all_four_wrong),
            'all_four_correct': sorted(list(all_four_correct)),
            'all_four_correct_count': len(all_four_correct),
            'unique_errors_total': len(all_incorrect),
            'overlap_rate': overlap_rate,
            'three_way': {
                'base_path_insp': sorted(list(base_path_insp)),
                'base_path_both': sorted(list(base_path_both)),
                'base_insp_both': sorted(list(base_insp_both)),
                'path_insp_both': sorted(list(path_insp_both))
            },
            'pairwise': {
                'base_path': sorted(list(base_path)),
                'base_insp': sorted(list(base_insp)),
                'base_both': sorted(list(base_both)),
                'path_insp': sorted(list(path_insp)),
                'path_both': sorted(list(path_both)),
                'insp_both': sorted(list(insp_both))
            },
            'unique': {
                'base_unique': sorted(list(base_unique)),
                'path_unique': sorted(list(path_unique)),
                'insp_unique': sorted(list(insp_unique)),
                'both_unique': sorted(list(both_unique))
            }
        },
        'effectiveness_vs_base': {
            'fixed_by_path': fixed_by_path,
            'fixed_by_insp': fixed_by_insp,
            'fixed_by_both': fixed_by_both,
            'new_by_path': new_by_path,
            'new_by_insp': new_by_insp,
            'new_by_both': new_by_both,
            'net_path': net_path,
            'net_insp': net_insp,
            'net_both': net_both
        }
    }

    output_file = os.path.join(base_dir, f'tool_comparison_4way_{model_prefix}.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] Detailed 4-way analysis saved to: {output_file}\n")


if __name__ == "__main__":
    base_dir = "./outputs"

    # Analyze both 4omini and 4o if available
    for model in ["4omini", "4o"]:
        try:
            analyze_tool_comparison_4way(base_dir, model)
        except Exception as e:
            print(f"[!] Could not analyze {model}: {e}\n")
