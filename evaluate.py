# evaluate.py

import os
import yaml
import argparse
import json

# Import necessary components
from src.evaluator import BeaverEvaluator

EVALUATORS = {"beaver": BeaverEvaluator}

def main():
    parser = argparse.ArgumentParser(description="Run Evaluation on existing predictions.")
    parser.add_argument("--prediction_path", required=True, help="Path to the predictions.json file.")
    parser.add_argument("--config", required=True, help="Path to the configuration file for evaluation settings.")
    parser.add_argument("--error_analysis", action='store_true',
                       help="Update error_analysis.json with evaluation results (result, res)")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    print("Evaluation config loaded.")

    # Load config file
    try:
        with open(args.prediction_path, 'r', encoding='utf-8') as f:
            predictions = json.load(f)
        print(f"Predictions loaded from: {args.prediction_path}")
    except FileNotFoundError:
        print(f"Error: Prediction file not found at {args.prediction_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {args.prediction_path}")
        return

    # Initialize the correct evaluator based on the config
    config['prediction_path'] = args.prediction_path
    dataset_name = config['dataset']['name']
    if dataset_name not in EVALUATORS:
        print(f"Error: No evaluator found for dataset '{dataset_name}'")
        return
    
    evaluator = EVALUATORS[dataset_name](config)
    
    # Run evaluation
    eval_results = evaluator.evaluate(predictions)
    
    print("\n" + "="*30)
    print("FINAL EVALUATION RESULTS")
    print("="*30)
    print(json.dumps(eval_results, indent=4))
    print("="*30)

    # Save results to a file in the same directory as the predictions
    output_dir = os.path.dirname(args.prediction_path)
    results_file = os.path.join(output_dir, "evaluation_results.json")
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(eval_results, f, indent=4)
    print(f"\n Evaluation results also saved to: {results_file}")

    # Update error_analysis.json with evaluation results if flag is set
    if args.error_analysis:
        error_analysis_path = os.path.join(output_dir, "error_analysis.json")
        exec_results_path = os.path.join(output_dir, "exec_results_detail.json")

        if os.path.exists(error_analysis_path) and os.path.exists(exec_results_path):
            # Load files
            with open(error_analysis_path, 'r', encoding='utf-8') as f:
                error_analysis = json.load(f)
            with open(exec_results_path, 'r', encoding='utf-8') as f:
                exec_results = json.load(f)

            # exec_results를 idx로 매핑 (sql_idx -> result)
            exec_map = {}
            for r in exec_results:
                sql_idx = r.get('sql_idx')
                exec_map[sql_idx] = {
                    "res": r.get('res', 0),
                    "ground_truth": r.get('ground_truth', '')
                }

            # 최종 iter (predictions.json에 있는 SQL)의 res만 채움
            # result는 그대로 유지 (error, empty, executed - 실행 상태)
            # 최종 iter 찾기: iter_1이 가장 마지막 (refine 후 최종 결과)
            if "iter_1" in error_analysis:
                for item in error_analysis["iter_1"]:
                    idx = item.get("idx", -1)
                    if idx in exec_map:
                        item["res"] = exec_map[idx]["res"]
                        # result는 덮어쓰지 않음 - 원래 실행 상태 유지

            # 저장
            with open(error_analysis_path, 'w', encoding='utf-8') as f:
                json.dump(error_analysis, f, indent=2, ensure_ascii=False)

            print(f"✅ Error analysis updated with evaluation results: {error_analysis_path}")
        else:
            if not os.path.exists(error_analysis_path):
                print(f"⚠️ error_analysis.json not found at {error_analysis_path}")
            if not os.path.exists(exec_results_path):
                print(f"⚠️ exec_results_detail.json not found at {exec_results_path}")

if __name__ == "__main__":
    main()
