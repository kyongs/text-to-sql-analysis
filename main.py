# main.py

import os
import yaml
import argparse
from tqdm import tqdm
from datetime import datetime
import concurrent.futures
import json
from dotenv import load_dotenv
load_dotenv()

from src.data_loader import BeaverLoader
from src.model import OpenAIModel
from src.evaluator import BeaverEvaluator
from src.prompt_builder import build_prompt
from src.utils.logger import TxtLogger
from src.data_loader.preprocess import run_grand_preprocessing

DATA_LOADERS = {"beaver": BeaverLoader}
MODELS = {"openai": OpenAIModel}
EVALUATORS = {"beaver": BeaverEvaluator}


def check_and_run_preprocessing(config: dict):
    dataset_config = config.get('dataset', {})
    dataset_name = dataset_config.get('name')
    if not dataset_name: return

    dataset_path = dataset_config.get('path')
    split = dataset_config.get('split')
    
    db_config = {"db_type": dataset_config.get("db_type", "sqlite")}
    
    if db_config["db_type"] == "mysql":
        conn_info = config.get("db_connection", {})
        db_config['host'] = conn_info.get('host', os.getenv("MYSQL_HOST"))
        db_config['port'] = int(conn_info.get('port', os.getenv("MYSQL_PORT", 3306)))
        db_config['user'] = conn_info.get('user', os.getenv("MYSQL_USER"))
        db_config['password'] = conn_info.get('password', os.getenv("MYSQL_PASSWORD"))
    else: 
        db_config['db_dir'] = dataset_config.get('db_dir')

    preprocessed_path = os.path.join(dataset_path, f"{split}_preprocessed_schemas.json")

    if not os.path.exists(preprocessed_path):
        print(f"Preprocessed file not found...")
        print(f"Starting automatic preprocessing for '{dataset_name}' dataset...")
        try:
            run_grand_preprocessing(dataset_name, dataset_path, split, db_config)
        except Exception as e:
            print(f"Preprocessing failed: {e}")
            exit(1)
    else:
        print(f"Found preprocessed schema file.")


def process_item(item, model, db_type: str):
    """Process a single item and return all relevant data for logging and evaluation."""
    db_id = item['db_id']
    question = item['question']
    gold_query = item.get('SQL', item.get('query', ''))
    schema = item.get('formatted_schema', 'Error: Schema not available.')
    
    evidence = item.get('evidence', '')
    join_keys = item.get('join_keys', {})
    all_hints = []
    
    # 1. evidence
    if evidence: all_hints.append(evidence)
    
    # 2. mapping
    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' is related to {', '.join(columns)}" for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping Hints:\n" + "\n".join(mapping_texts))

    # join keys
    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Information: {join_str}")

    final_hints = "\n\n".join(all_hints)
    prompt = build_prompt(schema=schema, question=question, db_name=db_id, db_type=db_type, hints=final_hints)
    model_response = model.generate(prompt)

    predicted_sql = "Error: API call failed or returned empty response."
    if model_response and model_response.choices:
        predicted_sql = model_response.choices[0].message.content.strip()
        
        if predicted_sql.startswith("```sql"):
            predicted_sql = predicted_sql[6:]
        if predicted_sql.endswith("```"):
            predicted_sql = predicted_sql[:-3]
        predicted_sql = predicted_sql.strip()

    return {
        "db_id": db_id,
        "question": question,
        "predicted_sql": predicted_sql,
        "gold_query": gold_query,
        "prompt": prompt,
        "model_response": model_response, 
    }

def main():
    parser = argparse.ArgumentParser(description="Text-to-SQL Framework - Experiment Runner")
    parser.add_argument("--config", required=True, help="Path to the configuration file.")
    parser.add_argument("--max_workers", type=int, default=8, help="Maximum number of threads.")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    check_and_run_preprocessing(config)

    timestamp = datetime.now().strftime("%Y%m%d")
    exp_name = f"{timestamp}_{config['experiment_name']}"
    output_dir = os.path.join(config['output_dir'], exp_name)
    log_dir = os.path.join("logs", exp_name)
    

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    print(f"Outputs will be saved in: {output_dir}")
    print(f"Logs will be saved in: {log_dir}")

    # Initialization
    experiment_mode = config.get('mode', 'baseline')
    print(f"Running in '{experiment_mode}' mode.")

    dataset_name = config['dataset']['name']
    db_type = config['dataset'].get('db_type', 'sqlite')
    data_loader = DATA_LOADERS[dataset_name](config)
    model = MODELS[config['model']['provider']](config)
    if experiment_mode == 'view':
        dataset = data_loader.load_data(load_views=True)
    else:
        dataset = data_loader.load_data(load_views=False)
    if not dataset: return

    log_file_path = os.path.join(log_dir, "run_log.txt") 
    logger = TxtLogger(log_file_path, len(dataset)) # log file 정렬되지는 않고 그냥 무작위로 적힘

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        print(f"Generating SQL Queries in parallel with {args.max_workers} workers...")
        
        futures = [executor.submit(process_item, item, model, db_type) for item in dataset]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(dataset), desc="Overall Progress"):
            try:
                result = future.result()
                all_results.append(result)
                logger.format_and_log(result) 
            except Exception as e:
                print(f"An error occurred while processing an item: {e}")

    # Sort results for consistent output
    all_results.sort(key=lambda x: [item['question'] for item in dataset].index(x['question']))
    
    # Save predictions.json for evaluation
    predictions_for_eval = [
        {"db_id": r["db_id"], "question": r["question"], "predicted_sql": r["predicted_sql"]}
        for r in all_results
    ]
    output_pred_file = os.path.join(output_dir, "predictions.json")
    with open(output_pred_file, 'w', encoding='utf-8') as f:
        json.dump(predictions_for_eval, f, indent=4)
    print(f"\nPredictions for evaluation saved to: {output_pred_file}")

if __name__ == "__main__":
    main()
