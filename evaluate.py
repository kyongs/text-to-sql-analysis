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

if __name__ == "__main__":
    main()
