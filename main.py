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
from src.model import OpenAIModel, GeminiModel, DeepSeekModel
from src.evaluator import BeaverEvaluator
from src.prompt_builder import build_prompt
from src.utils.logger import TxtLogger
from src.data_loader.preprocess import run_grand_preprocessing

DATA_LOADERS = {"beaver": BeaverLoader}
MODELS = {
    "openai": OpenAIModel,
    "google": GeminiModel,
    "deepseek": DeepSeekModel
}
EVALUATORS = {"beaver": BeaverEvaluator}


def check_and_run_preprocessing(config: dict):
    dataset_config = config.get('dataset', {})
    dataset_name = dataset_config.get('name')
    if not dataset_name:
        return

    dataset_path = dataset_config.get('path')
    split = dataset_config.get('split')
    schema_style = dataset_config.get('schema_representation', 'basic')
    mode = config.get('mode', 'baseline')

    if schema_style != 'm_schema' or mode != 'baseline':
        return

    db_config = {"db_type": dataset_config.get("db_type", "sqlite")}
    
    if db_config["db_type"] == "mysql":
        conn_info = config.get("db_connection", {})
        db_config['host'] = os.getenv("MYSQL_HOST", "127.0.0.1") if conn_info.get('host') == 'from_env' else conn_info.get('host', "127.0.0.1")
        db_config['port'] = int(os.getenv("MYSQL_PORT", 3306) if conn_info.get('port') == 'from_env' else conn_info.get('port', 3306))
        db_config['user'] = os.getenv("MYSQL_USER", "root") if conn_info.get('user') == 'from_env' else conn_info.get('user', "root")
        db_config['password'] = os.getenv("MYSQL_PASSWORD", "") if conn_info.get('password') == 'from_env' else conn_info.get('password', "")
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


def process_item(item, model, db_type: str, analyze_sql: bool = False, conn_info: dict = None, use_tools: bool = False, enabled_tools: list = None):
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
    prompt = build_prompt(schema=schema, question=question, db_name=db_id, db_type=db_type, hints=final_hints, use_tools=use_tools, enabled_tools=enabled_tools)
    
    # 모델 호출 (OpenAIModel은 tool flag 있으면 자동으로 tool calling 사용)
    tool_call_log = None
    if type(model).__name__ == 'OpenAIModel':
        model_response = model.generate(prompt, db_id=db_id)
        if model_response and hasattr(model_response, 'tool_call_log'):
            tool_call_log = model_response.tool_call_log
    else:
        model_response = model.generate(prompt)

    predicted_sql = "Error: API call failed or returned empty response."
    sql_analysis_report = None
    
    if model_response and model_response.choices:
        content = model_response.choices[0].message.content
        
        # content가 None인 경우 처리 (max_iterations 초과 등)
        if content is None:
            # Tool call 로그가 있으면 표시
            if hasattr(model_response, 'tool_call_log') and model_response.tool_call_log:
                predicted_sql = f"Error: Max iterations reached - LLM only called tools without final answer. Last tool calls: {len(model_response.tool_call_log)} iterations"
            else:
                predicted_sql = "Error: LLM returned empty content"
        else:
            predicted_sql = content.strip()
            
            if predicted_sql.startswith("```sql"):
                predicted_sql = predicted_sql[6:]
            if predicted_sql.endswith("```"):
                predicted_sql = predicted_sql[:-3]
            predicted_sql = predicted_sql.strip()
        
        # SQL 분석 수행 (옵션)
        if analyze_sql and conn_info and not predicted_sql.startswith("Error:"):
            try:
                sql_analysis_report = analyze_predicted_sql(predicted_sql, conn_info, db_id)
            except Exception as e:
                sql_analysis_report = f"⚠️  SQL Analysis Failed: {str(e)}"

    return {
        "db_id": db_id,
        "question": question,
        "predicted_sql": predicted_sql,
        "gold_query": gold_query,
        "prompt": prompt,
        "model_response": model_response,
        "sql_analysis": sql_analysis_report,
        "tool_call_log": tool_call_log,
        "original_index": item.get('original_index'),  # test_set 사용 시 원본 인덱스
    }

def main():
    parser = argparse.ArgumentParser(description="Text-to-SQL Framework - Experiment Runner")
    parser.add_argument("--config", required=True, help="Path to the configuration file.")
    parser.add_argument("--max_workers", type=int, default=8, help="Maximum number of threads.")
    parser.add_argument("--analyze_sql", action='store_true', 
                       help="Enable SQL analysis (execution and JOIN cardinality analysis)")
    parser.add_argument("--test_n", type=int, default=None,
                       help="Run only first N questions (for testing)")
    parser.add_argument("--test_set", type=str, default=None,
                       help="Path to txt file containing question indices to run (one index per line)")

    # Individual tool flags
    parser.add_argument("--join_inspector", action='store_true',
                       help="Enable inspect_join_relationship tool")
    parser.add_argument("--join_path_finder", action='store_true',
                       help="Enable find_join_path tool")
    parser.add_argument("--lookup_val", action='store_true',
                       help="Enable lookup_column_values tool for checking actual column values")
    
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
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
    
    # DB 연결 정보 (SQL 분석용)
    conn_info = None
    if args.analyze_sql and db_type == 'mysql':
        conn_info = config.get('db_connection', {})
        # 환경 변수에서 비밀번호 가져오기
        if conn_info.get('password') == 'from_env':
            conn_info['password'] = os.getenv('MYSQL_PASSWORD', '')
        print(f"✅ SQL Analysis enabled for MySQL database")
    
    data_loader = DATA_LOADERS[dataset_name](config)
    
    # Pass tool flags to config (OpenAIModel will handle tool calling internally)
    config['enabled_tools'] = {
        'join_inspector': args.join_inspector,
        'join_path_finder': args.join_path_finder,
        'lookup_column_values': args.lookup_val
    }

    model = MODELS[config['model']['provider']](config)
    if experiment_mode == 'view':
        dataset = data_loader.load_data(load_views=True)
    else:
        dataset = data_loader.load_data(load_views=False)
    if not dataset: return
    
    # 테스트 모드: --test_set 또는 --test_n 옵션 처리
    if args.test_set is not None:
        # 파일명만 주어지면 test_sets/ 폴더에서 찾기
        test_set_path = args.test_set
        if not os.path.exists(test_set_path):
            test_set_path = os.path.join("test_sets", args.test_set)

        # txt 파일에서 문항 인덱스 로드
        try:
            with open(test_set_path, 'r', encoding='utf-8') as f:
                test_indices = set()
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):  # 빈 줄, 주석 무시
                        test_indices.add(int(line))

            # 인덱스로 dataset 필터링 (원본 인덱스 저장)
            original_len = len(dataset)
            filtered_dataset = []
            for i, item in enumerate(dataset):
                if i in test_indices:
                    item['original_index'] = i  # 원본 인덱스 저장
                    filtered_dataset.append(item)
            dataset = filtered_dataset
            print(f"Test set mode: Running {len(dataset)}/{original_len} questions from {test_set_path}")
            print(f"   Indices: {sorted(test_indices)}")
        except FileNotFoundError:
            print(f"Error: Test set file not found: {args.test_set} (also tried test_sets/{args.test_set})")
            return
        except ValueError as e:
            print(f"Error: Invalid index in test set file: {e}")
            return
    elif args.test_n is not None:
        dataset = dataset[:args.test_n]
        print(f"Test mode: Running only first {args.test_n} questions")

    log_file_path = os.path.join(log_dir, "run_log.txt") 
    logger = TxtLogger(log_file_path, len(dataset)) # log file 정렬되지는 않고 그냥 무작위로 적힘

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(args.max_workers) as executor:
        print(f"Generating SQL Queries in parallel with {args.max_workers} workers...")
        
        # Tool 사용 여부 결정
        any_tool_enabled = args.join_inspector or args.join_path_finder or args.lookup_val
        use_tools = (config['model']['provider'] == 'openai' and any_tool_enabled)
        enabled_tool_names = []
        if use_tools:
            enabled_tools_dict = config.get('enabled_tools', {})
            enabled_list = []
            if enabled_tools_dict.get('join_inspector'):
                enabled_list.append("inspect_join_relationship (Analyze JOIN cardinality)")
                enabled_tool_names.append('join_inspector')
            if enabled_tools_dict.get('join_path_finder'):
                enabled_list.append("find_join_path (Find optimal JOIN paths)")
                enabled_tool_names.append('join_path_finder')
            if enabled_tools_dict.get('lookup_column_values'):
                enabled_list.append("lookup_column_values (Check actual column values)")
                enabled_tool_names.append('lookup_column_values')

            if enabled_list:
                print(f"Tool calling enabled - {len(enabled_list)} tool(s):")
                for i, tool in enumerate(enabled_list, 1):
                    print(f"   {i}. {tool}")
            else:
                print(f"openai_with_tools model loaded but no tools enabled")
                print(f"   Use --join_inspector, --join_path_finder, --lookup_val to enable tools")

        futures = {executor.submit(process_item, item, model, db_type, args.analyze_sql, conn_info, use_tools, enabled_tool_names): item for item in dataset}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(dataset), desc="Overall Progress"):
            try:
                result = future.result()
                all_results.append(result)
                logger.format_and_log(result) 
            except Exception as e:
                # 예외 발생 시에도 에러 결과로 저장
                item = futures[future]
                error_result = {
                    "db_id": item.get('db_id', 'unknown'),
                    "question": item.get('question', 'unknown'),
                    "predicted_sql": f"Error: {str(e)[:200]}"
                }
                all_results.append(error_result)
                print(f"Error processing question '{item.get('question_id', 'unknown')}': {str(e)[:100]}")

    # Sort results for consistent output
    all_results.sort(key=lambda x: [item['question'] for item in dataset].index(x['question']))
    
    # Save predictions.json for evaluation
    predictions_for_eval = []
    for r in all_results:
        pred = {"db_id": r["db_id"], "question": r["question"], "predicted_sql": r["predicted_sql"]}
        if r.get("original_index") is not None:
            pred["original_index"] = r["original_index"]
        predictions_for_eval.append(pred)
    output_pred_file = os.path.join(output_dir, "predictions.json")
    with open(output_pred_file, 'w', encoding='utf-8') as f:
        json.dump(predictions_for_eval, f, indent=4)
    print(f"\nPredictions for evaluation saved to: {output_pred_file}")
    
    # Save SQL analysis reports if enabled
    if args.analyze_sql:
        analysis_reports = [r for r in all_results if r.get('sql_analysis')]
        if analysis_reports:
            analysis_file = os.path.join(output_dir, "sql_analysis.txt")
            with open(analysis_file, 'w', encoding='utf-8') as f:
                for r in analysis_reports:
                    f.write(f"\n{'='*80}\n")
                    f.write(f"Question: {r['question']}\n")
                    f.write(f"{'='*80}\n")
                    f.write(r['sql_analysis'])
                    f.write("\n\n")
            print(f"SQL Analysis reports saved to: {analysis_file}")



if __name__ == "__main__":
    main()
