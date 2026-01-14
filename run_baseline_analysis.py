"""
Baseline Analysis Script
4o baseline을 N회 실행하고 syntax error / empty result 케이스를 분석합니다.

Usage:
    python run_baseline_analysis.py --config configs/beaver_dw_openai.yaml --runs 3 --output baseline_analysis.json
"""

import os
import sys
import yaml
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any
from decimal import Decimal
from datetime import date
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()


class CustomJSONEncoder(json.JSONEncoder):
    """MySQL 데이터 타입을 JSON으로 변환"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        return super().default(obj)


def execute_sql(db_config: Dict[str, Any], sql: str, db_id: str = "dw", timeout: int = 30) -> Dict[str, Any]:
    """
    SQL을 실행하고 결과를 반환합니다.

    Args:
        db_config: DB 연결 정보
        sql: 실행할 SQL
        db_id: 데이터베이스 ID
        timeout: SQL 실행 타임아웃 (초)

    Returns:
        {
            'success': bool,
            'row_count': int,
            'error': str or None,
            'error_type': 'syntax_error' | 'empty_result' | 'timeout' | None
        }
    """
    connection = None
    cursor = None

    try:
        password = os.getenv('MYSQL_PASSWORD') if db_config.get('password') == 'from_env' else db_config.get('password')

        connection = mysql.connector.connect(
            host=db_config.get('host', '127.0.0.1'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=password,
            database=db_id,
            connection_timeout=timeout
        )

        cursor = connection.cursor()

        # 쿼리 타임아웃 설정 (MySQL)
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {timeout * 1000}")

        # LIMIT 없으면 추가 (무한 결과 방지)
        sql_upper = sql.upper()
        if 'LIMIT' not in sql_upper and 'SELECT' in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT 10000"

        cursor.execute(sql)
        rows = cursor.fetchall()
        row_count = len(rows)

        cursor.close()
        connection.close()

        if row_count == 0:
            return {
                'success': True,
                'row_count': 0,
                'error': None,
                'error_type': 'empty_result'
            }

        return {
            'success': True,
            'row_count': row_count,
            'error': None,
            'error_type': None
        }

    except Error as e:
        error_msg = str(e)

        # 타임아웃 에러 구분
        if 'timeout' in error_msg.lower() or 'max_execution_time' in error_msg.lower():
            error_type = 'timeout'
        else:
            error_type = 'syntax_error'

        return {
            'success': False,
            'row_count': 0,
            'error': error_msg[:500],  # 에러 메시지 길이 제한
            'error_type': error_type
        }

    except Exception as e:
        # 기타 예외 처리
        return {
            'success': False,
            'row_count': 0,
            'error': f"Unexpected error: {str(e)[:300]}",
            'error_type': 'syntax_error'
        }

    finally:
        # 리소스 정리
        try:
            if cursor:
                cursor.close()
            if connection and connection.is_connected():
                connection.close()
        except:
            pass


def run_single_baseline(config: Dict, dataset: List[Dict], run_id: int) -> List[Dict]:
    """단일 baseline 실행"""
    from src.model.openai_model import OpenAIModel
    from src.prompt_builder.builder import build_prompt

    # 기존 enabled_tools 제거 (baseline이므로 tool 없이)
    config['enabled_tools'] = {}

    model = OpenAIModel(config)
    db_config = config.get('db_connection', {})
    db_type = config['dataset'].get('db_type', 'mysql')

    results = []
    total = len(dataset)

    print(f"\n=== Run {run_id + 1} ===")

    for i, item in enumerate(dataset):
        question = item.get('question', '')
        db_id = item.get('db_id', 'dw')
        schema = item.get('formatted_schema', '')  # beaver loader가 생성한 스키마
        hints = item.get('evidence', '')
        sql_idx = item.get('sql_idx', i)

        # 프롬프트 생성
        prompt = build_prompt(
            schema=schema,
            question=question,
            db_name=db_id,
            db_type=db_type,
            hints=hints,
            use_tools=False
        )

        # SQL 생성
        try:
            response = model.generate(prompt, db_id=db_id)
            if response and response.choices:
                predicted_sql = response.choices[0].message.content
                # SQL 추출 (```sql ... ``` 형식 처리)
                if '```sql' in predicted_sql:
                    predicted_sql = predicted_sql.split('```sql')[1].split('```')[0].strip()
                elif '```' in predicted_sql:
                    predicted_sql = predicted_sql.split('```')[1].split('```')[0].strip()
                predicted_sql = predicted_sql.strip()
            else:
                predicted_sql = ""
        except Exception as e:
            predicted_sql = f"-- Generation Error: {str(e)}"

        # SQL 실행
        exec_result = execute_sql(db_config, predicted_sql, db_id)

        result_entry = {
            'sql_idx': sql_idx,
            'question': question,
            'predicted_sql': predicted_sql,
            'success': exec_result['success'],
            'row_count': exec_result['row_count'],
            'error': exec_result['error'],
            'error_type': exec_result['error_type']
        }

        results.append(result_entry)

        # 진행상황 출력
        status = "✅" if exec_result['success'] and exec_result['row_count'] > 0 else "❌"
        if exec_result['error_type'] == 'syntax_error':
            status = "🔴 SYNTAX"
        elif exec_result['error_type'] == 'empty_result':
            status = "🟡 EMPTY"
        elif exec_result['error_type'] == 'timeout':
            status = "⏱️ TIMEOUT"

        print(f"  [{i+1}/{total}] sql_idx={sql_idx} {status}")

    return results


def analyze_results(all_runs: List[List[Dict]]) -> Dict:
    """모든 실행 결과를 분석하여 요약"""

    # sql_idx별로 결과 집계
    by_idx = {}

    for run_id, run_results in enumerate(all_runs):
        for item in run_results:
            idx = item['sql_idx']
            if idx not in by_idx:
                by_idx[idx] = {
                    'sql_idx': idx,
                    'question': item['question'],
                    'runs': []
                }

            by_idx[idx]['runs'].append({
                'run_id': run_id,
                'predicted_sql': item['predicted_sql'],
                'success': item['success'],
                'row_count': item['row_count'],
                'error': item['error'],
                'error_type': item['error_type']
            })

    # 문제 케이스 분류
    problem_cases = {
        'syntax_errors': [],      # 모든 run에서 syntax error
        'empty_results': [],      # 모든 run에서 empty result
        'timeouts': [],           # 모든 run에서 timeout
        'inconsistent': [],       # run마다 결과가 다름
        'all_success': []         # 모든 run 성공 (row > 0)
    }

    for idx, data in by_idx.items():
        runs = data['runs']

        all_syntax_error = all(r['error_type'] == 'syntax_error' for r in runs)
        all_empty = all(r['error_type'] == 'empty_result' for r in runs)
        all_timeout = all(r['error_type'] == 'timeout' for r in runs)
        all_ok = all(r['success'] and r['row_count'] > 0 for r in runs)

        case_info = {
            'sql_idx': idx,
            'question': data['question'],
            'runs': runs
        }

        if all_syntax_error:
            problem_cases['syntax_errors'].append(case_info)
        elif all_empty:
            problem_cases['empty_results'].append(case_info)
        elif all_timeout:
            problem_cases['timeouts'].append(case_info)
        elif all_ok:
            problem_cases['all_success'].append(case_info)
        else:
            problem_cases['inconsistent'].append(case_info)

    return {
        'summary': {
            'total_questions': len(by_idx),
            'syntax_errors': len(problem_cases['syntax_errors']),
            'empty_results': len(problem_cases['empty_results']),
            'timeouts': len(problem_cases['timeouts']),
            'inconsistent': len(problem_cases['inconsistent']),
            'all_success': len(problem_cases['all_success'])
        },
        'problem_cases': problem_cases
    }


def get_next_output_path(base_name: str = "baseline_analysis") -> str:
    """
    자동으로 다음 번호의 출력 파일명 생성
    baseline_analysis_1.json, baseline_analysis_2.json, ...
    """
    import glob

    # 기존 파일들 찾기
    pattern = f"{base_name}_*.json"
    existing_files = glob.glob(pattern)

    if not existing_files:
        return f"{base_name}_1.json"

    # 가장 큰 번호 찾기
    max_num = 0
    for f in existing_files:
        try:
            # baseline_analysis_3.json -> 3
            num_str = f.replace(f"{base_name}_", "").replace(".json", "")
            num = int(num_str)
            max_num = max(max_num, num)
        except ValueError:
            continue

    return f"{base_name}_{max_num + 1}.json"


def main():
    parser = argparse.ArgumentParser(description='Run baseline analysis')
    parser.add_argument('--config', type=str, required=True, help='Config YAML file path')
    parser.add_argument('--runs', type=int, default=3, help='Number of runs (default: 3)')
    parser.add_argument('--output', type=str, default=None, help='Output JSON file (auto-numbered if not specified)')
    parser.add_argument('--name', type=str, default='baseline_analysis', help='Base name for auto-numbered output')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of questions (for testing)')

    args = parser.parse_args()

    # 출력 파일명 결정
    if args.output:
        output_path = args.output
    else:
        output_path = get_next_output_path(args.name)

    # Config 로드
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # 데이터셋 로드
    from src.data_loader import BeaverLoader

    data_loader = BeaverLoader(config)
    dataset = data_loader.load_data(load_views=False)

    if args.limit:
        dataset = dataset[:args.limit]

    print(f"Loaded {len(dataset)} questions")
    print(f"Running {args.runs} baseline runs...")

    # N회 실행
    all_runs = []
    for run_id in range(args.runs):
        run_results = run_single_baseline(config, dataset, run_id)
        all_runs.append(run_results)

    # 결과 분석
    analysis = analyze_results(all_runs)

    # 결과 저장
    output_data = {
        'config': args.config,
        'num_runs': args.runs,
        'timestamp': datetime.now().isoformat(),
        'analysis': analysis,
        'all_runs': all_runs
    }

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)

    print(f"\n=== Analysis Summary ===")
    print(f"Total questions: {analysis['summary']['total_questions']}")
    print(f"Syntax errors (all runs): {analysis['summary']['syntax_errors']}")
    print(f"Empty results (all runs): {analysis['summary']['empty_results']}")
    print(f"Timeouts (all runs): {analysis['summary']['timeouts']}")
    print(f"Inconsistent across runs: {analysis['summary']['inconsistent']}")
    print(f"All success: {analysis['summary']['all_success']}")
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
