"""
Error Analysis Script
오답인 예측들에 대해 SQL 실행 결과를 보여주고 LLM에게 reasoning과 classification을 요청합니다.
"""

import os
import yaml
import argparse
import json
from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime, date
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

from src.model.openai_model import OpenAIModel
from src.model.gemini_model import GeminiModel
from src.model.deepseek_model import DeepSeekModel


class CustomJSONEncoder(json.JSONEncoder):
    """MySQL 데이터 타입을 JSON으로 변환하는 커스텀 인코더"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        return super().default(obj)


def get_model(model_config: Dict[str, Any]):
    """설정에 따라 적절한 모델 인스턴스 생성"""
    provider = model_config['provider']
    
    # OpenAIModel 등은 전체 config를 받아서 config['model']로 접근
    # 그래서 {'model': model_config} 형태로 전달
    wrapped_config = {'model': model_config}
    
    if provider == 'openai' or provider == 'openai_with_tools':
        return OpenAIModel(wrapped_config)
    elif provider == 'gemini':
        return GeminiModel(wrapped_config)
    elif provider == 'deepseek':
        return DeepSeekModel(wrapped_config)
    else:
        raise ValueError(f"Unsupported model provider: {provider}")


def execute_sql(db_config: Dict[str, Any], sql: str, limit: int = 5) -> Dict[str, Any]:
    """
    SQL을 실행하고 결과를 반환합니다.
    
    Returns:
        {
            'success': bool,
            'rows': List[tuple] or None,
            'columns': List[str] or None,
            'error': str or None,
            'row_count': int
        }
    """
    try:
        # 비밀번호 환경변수에서 가져오기
        password = os.getenv('MYSQL_PASSWORD') if db_config.get('password') == 'from_env' else db_config.get('password')
        
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=password,
            database='dw'  # beaver dw 데이터베이스
        )
        
        cursor = connection.cursor()
        
        # LIMIT 추가 (이미 있으면 추가 안함)
        sql_to_execute = sql
        if 'LIMIT' not in sql.upper():
            sql_to_execute = f"{sql.rstrip(';')} LIMIT {limit}"
        
        cursor.execute(sql_to_execute)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # 전체 row count를 위해 원본 쿼리 실행 (COUNT만)
        count_sql = f"SELECT COUNT(*) FROM ({sql.rstrip(';')}) as count_query"
        cursor.execute(count_sql)
        total_count = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        return {
            'success': True,
            'rows': rows,
            'columns': columns,
            'error': None,
            'row_count': total_count
        }
        
    except Error as e:
        return {
            'success': False,
            'rows': None,
            'columns': None,
            'error': str(e),
            'row_count': 0
        }


def format_sql_result(result: Dict[str, Any]) -> str:
    """SQL 실행 결과를 읽기 쉬운 텍스트로 포맷팅"""
    if not result['success']:
        return f"❌ SQL Execution Error: {result['error']}"
    
    output = []
    output.append(f"✅ Query executed successfully")
    output.append(f"📊 Total rows: {result['row_count']}")
    
    if result['rows']:
        output.append(f"\n🔍 Sample results (first {len(result['rows'])} rows):")
        output.append("Columns: " + " | ".join(result['columns']))
        output.append("-" * 80)
        
        for row in result['rows']:
            output.append(" | ".join(str(val) if val is not None else "NULL" for val in row))
    else:
        output.append("\n(No rows returned)")
    
    return "\n".join(output)


def load_evaluation_results(exec_results_path: str) -> Dict[int, bool]:
    """
    exec_results_detail.json에서 각 예측의 정답 여부를 로드합니다.
    
    Returns:
        {index: is_correct} 딕셔너리
    """
    if not os.path.exists(exec_results_path):
        raise FileNotFoundError(f"exec_results_detail.json not found at {exec_results_path}")
    
    with open(exec_results_path, 'r', encoding='utf-8') as f:
        exec_results = json.load(f)
    
    # exec_results format: [{'sql_idx': 0, 'res': 1}, ...]
    result_map = {}
    for item in exec_results:
        idx = item['sql_idx']
        is_correct = (item['res'] == 1)
        result_map[idx] = is_correct
    
    return result_map


def execute_and_compare(db_config: Dict, gold_sql: str, predicted_sql: str) -> bool:
    """
    gold와 predicted SQL을 실행하고 결과를 비교합니다.
    
    Returns:
        True if results match (correct prediction), False otherwise
    """
    gold_result = execute_sql(db_config, gold_sql, limit=None)  # 전체 실행
    pred_result = execute_sql(db_config, predicted_sql, limit=None)
    
    # 둘 다 성공했을 때만 비교
    if gold_result['success'] and pred_result['success']:
        gold_set = set(tuple(row) for row in gold_result['rows'])
        pred_set = set(tuple(row) for row in pred_result['rows'])
        return gold_set == pred_set
    elif not gold_result['success'] and not pred_result['success']:
        # 둘 다 에러면... 일단 틀린 걸로
        return False
    else:
        # 하나만 에러면 명백히 틀림
        return False


def create_analysis_prompt(question: str, gold_sql: str, predicted_sql: str, 
                          gold_result: Dict[str, Any], pred_result: Dict[str, Any]) -> str:
    """오답 분석을 위한 프롬프트 생성"""
    
    prompt = f"""You are an expert SQL analyst. Analyze why the predicted SQL query is incorrect.

**Question:**
{question}

**Gold (Correct) SQL:**
```sql
{gold_sql}
```

**Gold SQL Execution Result:**
{format_sql_result(gold_result)}

---

**Predicted (Incorrect) SQL:**
```sql
{predicted_sql}
```

**Predicted SQL Execution Result:**
{format_sql_result(pred_result)}

---

**Task:**
Please provide:

1. **Error Reasoning**: Explain in detail WHY the predicted SQL is incorrect. What is the fundamental difference from the gold SQL? What logic error was made?

2. **Error Classification**: Classify the error into ONE of the following categories:
   - **COLUMN_VALUE_ERROR**: The LLM guessed the wrong literal value for a column because it doesn't know the actual data in the database. Examples:
     * Question says "supervisor" but actual DB value is 'Activity leader'
     * Question says "Computer Science" but actual DB value is 'Electrical Eng & Computer Sci'
     * LLM used LIKE '%center for international studies%' when the table itself (CIS_COURSE_CATALOG) already contains only that department's data
     * FLOOR column contains non-numeric values like 'B1', 'G' that LLM didn't anticipate
   - **JOIN_ERROR**: Wrong JOIN type (INNER vs LEFT), missing JOIN, or incorrect JOIN condition
   - **CARDINALITY_ERROR**: M:N relationship not handled properly (missing DISTINCT, GROUP BY, etc.)
   - **FILTER_ERROR**: Wrong WHERE condition or missing/extra filters (but the value itself is correct)
   - **COLUMN_ERROR**: Wrong columns selected or aggregation issues
   - **LOGIC_ERROR**: Fundamental misunderstanding of the question
   - **SYNTAX_ERROR**: SQL syntax error preventing execution
   - **OTHER**: Other types of errors

   **IMPORTANT**: If the predicted SQL would be correct IF the LLM knew the actual column value in the database, classify it as COLUMN_VALUE_ERROR. This is a data knowledge issue, not a SQL logic issue.

3. **Suggested Fix**: Briefly describe what changes are needed to fix the predicted SQL.

Please format your response as JSON:
```json
{{
    "reasoning": "Detailed explanation here...",
    "error_category": "CATEGORY_NAME",
    "suggested_fix": "Brief description of fix..."
}}
```
"""
    
    return prompt


def analyze_errors(predictions_path: str, predict_dw_path: str, config_path: str, 
                  output_path: str, max_samples: int = None):
    """
    오답들을 분석하여 reasoning과 classification을 생성합니다.
    
    Args:
        predictions_path: predictions.json 경로
        predict_dw_path: predict_dw.json 경로 (정답 여부 포함)
        config_path: 설정 파일 경로
        output_path: 분석 결과 저장 경로
        max_samples: 분석할 최대 샘플 수 (None이면 전체)
    """
    
    # 설정 로드
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Predictions 로드
    with open(predictions_path, 'r', encoding='utf-8') as f:
        predictions = json.load(f)
    
    # exec_results_detail.json에서 정답 여부 로드
    output_dir = os.path.dirname(predict_dw_path)
    exec_results_path = os.path.join(output_dir, 'exec_results_detail.json')
    correctness_map = load_evaluation_results(exec_results_path)
    
    # Gold SQL 로드
    gold_data_path = os.path.join(config['dataset']['path'], 'formatted_data.json')
    with open(gold_data_path, 'r', encoding='utf-8') as f:
        gold_data = json.load(f)
    
    # Question을 key로 하는 gold SQL 맵 생성
    gold_sql_map = {item['question']: item['sql'] for item in gold_data}
    
    # LLM 모델 초기화
    model = get_model(config['model'])
    
    # 오답만 필터링 (파일에서 읽음 - 빠름!)
    incorrect_predictions = []
    for idx, pred in enumerate(predictions):
        if idx not in correctness_map:
            continue
        if not correctness_map[idx]:  # 오답인 경우만
            incorrect_predictions.append((idx, pred))
    
    print(f"📊 Total predictions: {len(predictions)}")
    print(f"❌ Incorrect predictions: {len(incorrect_predictions)}")
    
    if max_samples:
        incorrect_predictions = incorrect_predictions[:max_samples]
        print(f"🔬 Analyzing first {max_samples} incorrect predictions")
    
    # 분석 결과 저장
    analysis_results = []
    
    for i, (idx, pred) in enumerate(incorrect_predictions, 1):
        question = pred['question']
        predicted_sql = pred['predicted_sql']
        gold_sql = gold_sql_map.get(question)
        
        if not gold_sql:
            print(f"⚠️ [{i}/{len(incorrect_predictions)}] No gold SQL found for question: {question[:50]}...")
            continue
        
        print(f"\n{'='*80}")
        print(f"🔍 [{i}/{len(incorrect_predictions)}] Analyzing question #{idx}")
        print(f"Q: {question[:80]}...")
        print(f"Predicted SQL type: {type(predicted_sql)}")
        if predicted_sql.startswith("Error:"):
            print(f"⚠️ Predicted SQL is an error message: {predicted_sql}")
        
        # SQL 실행
        print("\n  📊 Executing Gold SQL...")
        gold_result = execute_sql(config['db_connection'], gold_sql)
        print(f"     Gold result: {'✅ Success' if gold_result['success'] else '❌ Error'} - {gold_result['row_count']} rows")
        
        print("\n  📊 Executing Predicted SQL...")
        pred_result = execute_sql(config['db_connection'], predicted_sql)
        print(f"     Predicted result: {'✅ Success' if pred_result['success'] else '❌ Error'}")
        if not pred_result['success']:
            print(f"     Error: {pred_result['error'][:100]}...")
        
        # LLM에게 분석 요청
        print("\n  🤖 Requesting LLM analysis...")
        analysis_prompt = create_analysis_prompt(
            question, gold_sql, predicted_sql, gold_result, pred_result
        )
        print(f"     Prompt length: {len(analysis_prompt)} chars")
        
        try:
            llm_response = model.generate(analysis_prompt)
            
            print(f"     LLM response type: {type(llm_response)}")
            print(f"     LLM response preview: {str(llm_response)[:200]}...")
            
            # ChatCompletion 객체인 경우 content 추출
            if hasattr(llm_response, 'choices'):
                response_text = llm_response.choices[0].message.content
                print(f"     Extracted from ChatCompletion object")
            elif isinstance(llm_response, str):
                response_text = llm_response
            else:
                response_text = str(llm_response)
            
            # JSON 파싱 시도
            response_text = response_text.strip()
            # ```json ... ``` 제거
            if response_text.startswith('```json'):
                response_text = response_text[7:]
            if response_text.startswith('```'):
                response_text = response_text[3:]
            if response_text.endswith('```'):
                response_text = response_text[:-3]
            response_text = response_text.strip()
            
            analysis = json.loads(response_text)
            
            print(f"  ✅ Category: {analysis['error_category']}")
            
        except json.JSONDecodeError as e:
            print(f"  ⚠️ Failed to parse LLM response as JSON: {e}")
            print(f"     Response text: {response_text[:500]}...")
            analysis = {
                "reasoning": response_text if 'response_text' in locals() else str(llm_response),
                "error_category": "UNKNOWN",
                "suggested_fix": "N/A"
            }
        except Exception as e:
            print(f"  ❌ Error during analysis: {type(e).__name__}: {str(e)}")
            print(f"     Full traceback available if needed")
            analysis = {
                "reasoning": f"Error: {str(e)}",
                "error_category": "ERROR",
                "suggested_fix": "N/A"
            }
        
        # 결과 저장
        analysis_results.append({
            "question": question,
            "gold_sql": gold_sql,
            "predicted_sql": predicted_sql,
            "gold_execution": {
                "success": gold_result['success'],
                "row_count": gold_result['row_count'],
                "sample_rows": gold_result['rows'][:3] if gold_result['rows'] else [],
                "columns": gold_result['columns'],
                "error": gold_result['error']
            },
            "predicted_execution": {
                "success": pred_result['success'],
                "row_count": pred_result['row_count'],
                "sample_rows": pred_result['rows'][:3] if pred_result['rows'] else [],
                "columns": pred_result['columns'],
                "error": pred_result['error']
            },
            "analysis": analysis
        })
    
    # 결과 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analysis_results, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)
    
    print(f"\n{'='*80}")
    print(f"✅ Analysis complete! Results saved to: {output_path}")
    
    # 통계 출력
    category_counts = {}
    for result in analysis_results:
        category = result['analysis']['error_category']
        category_counts[category] = category_counts.get(category, 0) + 1
    
    print(f"\n📊 Error Category Distribution:")
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(analysis_results)) * 100
        print(f"  {category}: {count} ({percentage:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Analyze incorrect SQL predictions using LLM")
    parser.add_argument("--predictions", required=True, help="Path to predictions.json")
    parser.add_argument("--predict_dw", required=True, help="Path to predict_dw.json")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--output", required=True, help="Path to save analysis results")
    parser.add_argument("--max_samples", type=int, default=None, 
                       help="Maximum number of samples to analyze (default: all)")
    
    args = parser.parse_args()
    
    analyze_errors(
        predictions_path=args.predictions,
        predict_dw_path=args.predict_dw,
        config_path=args.config,
        output_path=args.output,
        max_samples=args.max_samples
    )


if __name__ == "__main__":
    main()
