# eval_scripts/beaver/evaluation.py

import os
import sys
import json
import argparse
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut
from sqlalchemy import create_engine, text
import pymysql
from dotenv import load_dotenv

# Load .env file for environment variables
load_dotenv()

# --- 전역 변수 ---
exec_result = []

def load_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def result_callback(result):
    exec_result.append(result)

def get_mysql_connection_url(conn_info, db_id):
    return (
        f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}"
        f"@{conn_info['host']}:{conn_info['port']}/{db_id}"
    )

def execute_sql(predicted_sql, ground_truth, db_id, conn_info):
    engine = create_engine(get_mysql_connection_url(conn_info, db_id))
    with engine.connect() as conn:
        try:
            predicted_res = conn.execute(text(predicted_sql)).fetchall()
        except Exception:
            return 0
        
        ground_truth_res = conn.execute(text(ground_truth)).fetchall()

    # print("PREDICTED:")
    # print(predicted_res)
    # print("GROUND TRUTH:")
    # print(ground_truth_res)
    # print()

    return 1 if set(predicted_res) == set(ground_truth_res) else 0

def execute_model(predicted_sql, ground_truth, db_id, conn_info, idx, meta_time_out):
    """타임아웃과 예외 처리를 포함하여 쿼리를 실행합니다."""
    res = 0
    try:
        res = func_timeout(meta_time_out, execute_sql,
                           args=(predicted_sql, ground_truth, db_id, conn_info))
    except (FunctionTimedOut, Exception):
        pass
        
    return {'sql_idx': idx, 'ground_truth': ground_truth, 'res': res}

def package_sqls(data_path, data_mode='dev', is_gt=False, original_indices=None):
    """
    Load SQL queries from files.

    Args:
        data_path: Path to data directory
        data_mode: Dataset mode (e.g., 'dev', 'dw')
        is_gt: Whether loading ground truth
        original_indices: List of original indices to filter ground truth (for test_set mode)
    """
    sqls, db_ids = [], []

    if is_gt:
        gt_data = load_json(os.path.join(data_path, f'{data_mode}.json'))
        if original_indices is not None:
            # test_set mode: only load ground truth for specified indices
            for idx in original_indices:
                if idx < len(gt_data):
                    sqls.append(gt_data[idx]['sql'])
                    db_ids.append(gt_data[idx]['db_id'])
        else:
            for item in gt_data:
                sqls.append(item['sql'])
                db_ids.append(item['db_id'])
    else:
        pred_data = load_json(os.path.join(data_path, f'predict_{data_mode}.json'))
        sorted_keys = sorted(pred_data.keys(), key=int)
        for key in sorted_keys:
            sql_str = pred_data[key]
            sql, db_name = sql_str.split('\t----- bird -----\t')
            sqls.append(sql)
            db_ids.append(db_name)

    return sqls, db_ids

def run_sqls_parallel(sqls, db_ids, conn_info, num_cpus, meta_time_out):
    pool = mp.Pool(processes=num_cpus)
    for i, sql_pair in enumerate(sqls):
        predicted_sql, ground_truth = sql_pair
        pool.apply_async(execute_model, args=(predicted_sql, ground_truth, db_ids[i], conn_info, i, meta_time_out), callback=result_callback)
    pool.close()
    pool.join()
    
def sort_results(list_of_dicts):
    return sorted(list_of_dicts, key=lambda x: x['sql_idx'])

def compute_acc(exec_results):
    num_queries = len(exec_results)
    if num_queries == 0: return 0.0
    
    total_correct = sum(res['res'] for res in exec_results)
    accuracy = total_correct / num_queries
    return accuracy * 100

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--predicted_sql_path', type=str, required=True, help="Directory containing predict_{data_mode}.json")
    parser.add_argument('--ground_truth_path', type=str, required=True, help="Directory containing {data_mode}.json")
    parser.add_argument('--data_mode', type=str, required=True)
    parser.add_argument('--db_host', type=str, required=True)
    parser.add_argument('--db_port', type=int, required=True)
    parser.add_argument('--db_user', type=str, required=True)
    parser.add_argument('--db_password', type=str, required=True)
    parser.add_argument('--num_cpus', type=int, default=1)
    parser.add_argument('--meta_time_out', type=float, default=30.0)
    args = parser.parse_args()

    # Handle "from_env" password
    db_password = args.db_password
    if db_password == "from_env":
        db_password = os.getenv("MYSQL_PASSWORD", "")

    conn_info = {
        "host": args.db_host, "port": args.db_port,
        "user": args.db_user, "password": db_password
    }

    # Load predictions and check for original_indices (test_set mode)
    predictions_file = os.path.join(args.predicted_sql_path, 'predictions.json')
    original_indices = None
    if os.path.exists(predictions_file):
        predictions = load_json(predictions_file)
        # Check if any prediction has original_index (test_set mode)
        if predictions and 'original_index' in predictions[0]:
            original_indices = [p['original_index'] for p in predictions]
            print(f"Test set mode detected: evaluating indices {original_indices}")

    pred_queries, pred_db_ids = package_sqls(args.predicted_sql_path, data_mode=args.data_mode, is_gt=False)
    gt_queries, gt_db_ids = package_sqls(args.ground_truth_path, data_mode=args.data_mode, is_gt=True, original_indices=original_indices)

    # 경고: DB ID 길이 체크
    if len(pred_queries) != len(gt_queries):
        print(f"[WARNING] Prediction count ({len(pred_queries)}) != Ground truth count ({len(gt_queries)})")
        print(f"   Only evaluating {len(pred_queries)} predictions against ground truth")
    
    # DB ID 일치 확인 (길이가 같은 범위까지만)
    min_len = min(len(pred_db_ids), len(gt_db_ids))
    assert pred_db_ids[:min_len] == gt_db_ids[:min_len], f"DB ID mismatch: {pred_db_ids[:min_len]} vs {gt_db_ids[:min_len]}"

    # 최소 길이만큼만 비교
    query_pairs = list(zip(pred_queries[:min_len], gt_queries[:min_len]))
    pred_db_ids = pred_db_ids[:min_len]
    run_sqls_parallel(query_pairs, pred_db_ids, conn_info, args.num_cpus, args.meta_time_out)
    
    exec_result = sort_results(exec_result)
    for res in exec_result:
        if res['res'] == 1:
            print(res)
    accuracy = compute_acc(exec_result)

    print("\n==================== EXECUTION ACCURACY ====================")
    print(f"Total Queries: {len(exec_result)}")
    print(f"Correct: {sum(res['res'] for res in exec_result)}")
    print(f"Accuracy: {accuracy:.2f}%")
    print("============================================================")
    
    # Save detailed results
    output_dir = args.predicted_sql_path
    exec_results_path = os.path.join(output_dir, 'exec_results_detail.json')
    with open(exec_results_path, 'w') as f:
        json.dump(exec_result, f, indent=2)
    print(f"✅ Detailed results saved to: {exec_results_path}")
    