# preprocess.py

import os
import json
import sqlite3
import mysql.connector 
from tqdm import tqdm
import argparse
from src.prompt_builder import schema_formatter

def _restructure_beaver_schema(beaver_schema_info: dict) -> dict:
    """Beaver의 플랫한 스키마를 Spider/BIRD와 유사한 중첩 구조로 변환합니다."""
    restructured = {}
    col_index_map = {}
    pending_keys = {}

    # 1) 테이블/컬럼 정보 수집
    for key, table_info in beaver_schema_info.items():
        if '#sep#' not in key:
            continue
        db_id, table_name = key.split('#sep#', 1)
        if db_id not in restructured:
            restructured[db_id] = {
                "db_id": db_id,
                "table_names_original": [],
                "column_names_original": [],
                "column_types": [],
                "primary_keys": [],
                "foreign_keys": []
            }

        restructured[db_id]["table_names_original"].append(table_name)
        tbl_idx = len(restructured[db_id]["table_names_original"]) - 1

        columns = table_info.get("column_names_original", [])
        column_types = table_info.get("column_types", [])
        for idx, col_name in enumerate(columns):
            col_type = column_types[idx] if idx < len(column_types) else "TEXT"
            restructured[db_id]["column_names_original"].append([tbl_idx, col_name])
            restructured[db_id]["column_types"].append(col_type)
            global_col_idx = len(restructured[db_id]["column_names_original"]) - 1
            col_index_map[(db_id, table_name, col_name)] = global_col_idx

        pending_keys[(db_id, table_name)] = {
            "primary_key": table_info.get("primary_key", []),
            "foreign_key": table_info.get("foreign_key", [])
        }

    # 2) PK/FK 인덱스 계산
    for (db_id, table_name), meta in pending_keys.items():
        if db_id not in restructured:
            continue
        # Primary Keys
        for pk_col in meta.get("primary_key", []):
            pk_idx = col_index_map.get((db_id, table_name, pk_col))
            if pk_idx is not None:
                restructured[db_id]["primary_keys"].append(pk_idx)

        # Foreign Keys
        for fk in meta.get("foreign_key", []):
            col_name = fk.get("column_name")
            ref_table_full = fk.get("referenced_table_name", "")
            ref_col_name = fk.get("referenced_column_name", "")
            if '#sep#' in ref_table_full:
                ref_db_id, ref_table = ref_table_full.split('#sep#', 1)
            else:
                ref_db_id, ref_table = db_id, ref_table_full

            col_idx = col_index_map.get((db_id, table_name, col_name))
            ref_idx = col_index_map.get((ref_db_id, ref_table, ref_col_name))
            if col_idx is not None and ref_idx is not None:
                restructured[db_id]["foreign_keys"].append([col_idx, ref_idx])

    # list of dicts 형태로 최종 변환
    return list(restructured.values())

def run_grand_preprocessing(dataset_name: str, dataset_path: str, split: str, db_config: dict):
    tables_json_path = os.path.join(dataset_path, "tables.json")
    output_path = os.path.join(dataset_path, f"{split}_preprocessed_schemas.json")

    with open(tables_json_path, 'r', encoding='utf-8') as f:
        schema_info_list = json.load(f)

    if dataset_name == 'beaver':
        schema_info_list = _restructure_beaver_schema(schema_info_list)
    
    if dataset_name == 'beaver' and split == 'dw':
        print("Beaver 'dw' split detected. Filtering to process only the 'dw' database.")
        schema_info_list = [db for db in schema_info_list if db['db_id'] == 'dw']

    all_preprocessed_schemas = {}
    
    print(f"Starting grand preprocessing for '{dataset_name}'...")
    for db_info in tqdm(schema_info_list, desc="Preprocessing Schemas"):
        db_id = db_info['db_id']
        all_preprocessed_schemas[db_id] = {}
        
        all_examples = {}
        conn = None
        db_type = db_config.get("db_type", "sqlite")
        
        try:
            tables_data = schema_formatter._get_schema_details(db_info)
            if db_type == "mysql":
                conn = mysql.connector.connect(
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    password=db_config['password'],
                    database=db_id
                )
                cursor = conn.cursor()
                quote_char = '`'
            else: # sqlite
                db_path = os.path.join(db_config['db_dir'], db_id, f"{db_id}.sqlite")
                if not os.path.exists(db_path): continue
                conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
                cursor = conn.cursor()
                quote_char = '"'

            for tbl_name, cols in tables_data.items():
                for col in cols:
                    col_name = col['name']
                    try:
                        query = f'SELECT DISTINCT {quote_char}{col_name}{quote_char} FROM {quote_char}{tbl_name}{quote_char} WHERE {quote_char}{col_name}{quote_char} IS NOT NULL LIMIT 3'
                        cursor.execute(query)
                        results = [row[0] for row in cursor.fetchall()]
                        all_examples[(tbl_name, col_name)] = results
                    except Exception:
                         all_examples[(tbl_name, col_name)] = []
        except Exception as e:
            print(f"DB connection/query failed for {db_id}: {e}")
        finally:
            if conn:
                conn.close()

        for style in schema_formatter.SCHEMA_FORMATTERS.keys():
            formatted_string = schema_formatter.format_schema(
                db_info=db_info,
                style=style,
                db_examples=all_examples
            )
            all_preprocessed_schemas[db_id][style] = formatted_string

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_preprocessed_schemas, f, indent=2)
    print(f"\n✅ Grand preprocessing complete! Schemas saved to {output_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run grand preprocessing for schema formats.")
    parser.add_argument("--dataset_name", type=str, required=True, choices=['beaver'])
    parser.add_argument("--dataset_path", type=str, required=True)
    parser.add_argument("--split", type=str, required=True)
    # 수동 실행을 위해 db_dir 인자 추가
    parser.add_argument("--db_dir", type=str, required=True, help="Path to the database directory.")
    args = parser.parse_args()
    
    db_config = {"db_type": "sqlite", "db_dir": args.db_dir}
    run_grand_preprocessing(args.dataset_name, args.dataset_path, args.split, db_config)
