"""
Refine Analysis Script
error_analysis.json의 SQL 변천사를 분석하고, LLM으로 개선 여부를 판단합니다.
"""

import os
import yaml
import argparse
import json
import csv
from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime, date
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

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
    wrapped_config = {'model': model_config}

    if provider == 'openai' or provider == 'openai_with_tools':
        return OpenAIModel(wrapped_config)
    elif provider == 'gemini' or provider == 'google':
        return GeminiModel(wrapped_config)
    elif provider == 'deepseek':
        return DeepSeekModel(wrapped_config)
    else:
        raise ValueError(f"Unsupported model provider: {provider}")


def load_gold_data(dataset_path: str) -> Dict[int, Dict[str, str]]:
    """
    formatted_data.json에서 NLQ와 gold SQL 로드
    Returns: {idx: {"question": ..., "gold_sql": ...}}
    """
    gold_data_path = os.path.join(dataset_path, 'formatted_data.json')
    with open(gold_data_path, 'r', encoding='utf-8') as f:
        gold_data = json.load(f)

    return {i: {"question": item['question'], "gold_sql": item['sql']}
            for i, item in enumerate(gold_data)}


def create_analysis_prompt(question: str, gold_sql: str, sql_evolution: List[Dict],
                          show_gold: bool = True) -> str:
    """SQL 변천사 분석을 위한 프롬프트 생성"""

    # SQL 변천사 문자열 생성
    evolution_str = ""
    for i, item in enumerate(sql_evolution):
        iter_num = item.get('iter', i + 1)
        sql = item.get('sql', '')
        result = item.get('result', 'unknown')
        refine_feedback = item.get('refine_feedback', '')

        evolution_str += f"\n### Iteration {iter_num}\n"
        evolution_str += f"**Result:** {result}\n"
        evolution_str += f"```sql\n{sql}\n```\n"

        if refine_feedback:
            evolution_str += f"\n**Refine Feedback (trigger for next iteration):**\n{refine_feedback}\n"

    if show_gold:
        prompt = f"""You are an expert SQL analyst. Analyze the SQL query evolution and determine if the refinements improved or degraded the query.

**Natural Language Question:**
{question}

**Gold (Correct) SQL:**
```sql
{gold_sql}
```

---

**SQL Evolution (from initial to final):**
{evolution_str}

---

**Task:**
Please analyze:

1. **Evolution Assessment**: Did the SQL improve, stay the same, or get worse across iterations? Compare each iteration to the gold SQL.

2. **Best Iteration**: Which iteration produced the SQL closest to the gold SQL? (iter_1, iter_2, etc.)

3. **Error Analysis**: If the final SQL is still incorrect, explain why. Classify the error:
   - COLUMN_VALUE_ERROR: Wrong literal value (LLM doesn't know actual DB data)
   - JOIN_ERROR: Wrong JOIN type or condition
   - CARDINALITY_ERROR: M:N relationship issues (missing DISTINCT, GROUP BY)
   - FILTER_ERROR: Wrong WHERE condition
   - COLUMN_ERROR: Wrong columns selected
   - LOGIC_ERROR: Fundamental misunderstanding of the question
   - SYNTAX_ERROR: SQL syntax error
   - REFINE_REGRESSION: Refinement made it worse
   - OTHER: Other issues

4. **Refinement Quality**: Was the refine feedback helpful? Did the model respond correctly to it?

Please format your response as JSON:
```json
{{
    "evolution_assessment": "improved/same/worse",
    "best_iteration": "iter_X",
    "final_correct": true/false,
    "error_category": "CATEGORY_NAME or null if correct",
    "reasoning": "한국어로 상세히 설명해주세요 (Explain in Korean)",
    "refinement_quality": "helpful/unhelpful/counterproductive",
    "suggested_fix": "Brief description if still incorrect, or null"
}}
```

**IMPORTANT: The "reasoning" field MUST be written in Korean (한국어).**
"""
    else:
        # --no_gold 모드: gold SQL 없이 분석
        prompt = f"""You are an expert SQL analyst. Analyze the SQL query evolution and determine if the refinements improved the query.

**Natural Language Question:**
{question}

---

**SQL Evolution (from initial to final):**
{evolution_str}

---

**Task:**
Please analyze WITHOUT seeing the gold SQL:

1. **Evolution Assessment**: Based on the question and the SQL changes, did the refinements seem to improve the query logic?

2. **Query Quality**: Does the final SQL look like it would correctly answer the question?

3. **Potential Issues**: What potential issues do you see in the final SQL?

4. **Refinement Response**: Did the model respond correctly to the refine feedback?

Please format your response as JSON:
```json
{{
    "evolution_assessment": "improved/same/worse",
    "query_looks_correct": true/false,
    "confidence": "high/medium/low",
    "potential_issues": ["issue1", "issue2"],
    "reasoning": "한국어로 상세히 설명해주세요 (Explain in Korean)",
    "refinement_quality": "helpful/unhelpful/counterproductive"
}}
```

**IMPORTANT: The "reasoning" field MUST be written in Korean (한국어).**
"""

    return prompt


def analyze_refine_evolution(error_analysis_path: str, config_path: str,
                            output_path: str, no_gold: bool = False,
                            max_samples: int = None):
    """
    error_analysis.json의 SQL 변천사를 분석합니다.
    """

    # 설정 로드
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    # error_analysis.json 로드
    with open(error_analysis_path, 'r', encoding='utf-8') as f:
        error_analysis = json.load(f)

    # Gold data 로드 (no_gold가 아닐 때만)
    gold_data = {}
    if not no_gold:
        gold_data = load_gold_data(config['dataset']['path'])

    # LLM 모델 초기화
    model = get_model(config['model'])

    # iter별 데이터를 idx 기준으로 그룹핑
    idx_evolution = {}  # {idx: [iter_1_data, iter_2_data, ...]}

    for iter_key, items in error_analysis.items():
        if not iter_key.startswith('iter_'):
            continue
        iter_num = int(iter_key.split('_')[1])

        for item in items:
            idx = item.get('idx')
            if idx is None:
                continue

            if idx not in idx_evolution:
                idx_evolution[idx] = []

            idx_evolution[idx].append({
                'iter': iter_num,
                'sql': item.get('sql', ''),
                'result': item.get('result', ''),
                'res': item.get('res', ''),
                'refine_feedback': item.get('refine_feedback', '')
            })

    # iter 순서대로 정렬
    for idx in idx_evolution:
        idx_evolution[idx].sort(key=lambda x: x['iter'])

    # 2개 이상의 iteration이 있는 것만 분석 (refine이 발생한 경우)
    items_to_analyze = [(idx, evol) for idx, evol in idx_evolution.items()
                        if len(evol) >= 2]

    print(f"📊 Total items with evolution: {len(items_to_analyze)}")

    if max_samples:
        items_to_analyze = items_to_analyze[:max_samples]
        print(f"🔬 Analyzing first {max_samples} items")

    # 분석 결과 저장
    analysis_results = []

    for i, (idx, evolution) in enumerate(items_to_analyze, 1):
        # Gold data에서 NLQ와 gold SQL 가져오기
        if no_gold:
            question = f"[Question not shown - idx {idx}]"
            gold_sql = None
        else:
            if idx in gold_data:
                question = gold_data[idx]['question']
                gold_sql = gold_data[idx]['gold_sql']
            else:
                print(f"⚠️ [{i}/{len(items_to_analyze)}] No gold data for idx {idx}")
                continue

        print(f"\n{'='*80}")
        print(f"🔍 [{i}/{len(items_to_analyze)}] Analyzing idx #{idx}")
        print(f"   Iterations: {len(evolution)}")

        # 각 iteration의 결과 출력
        for ev in evolution:
            result_str = ev.get('result', 'unknown')
            if result_str == '':
                result_str = 'not evaluated'
            print(f"   - iter_{ev['iter']}: {result_str}")

        # LLM 분석 요청
        print(f"\n  🤖 Requesting LLM analysis...")
        analysis_prompt = create_analysis_prompt(
            question, gold_sql, evolution, show_gold=not no_gold
        )

        try:
            llm_response = model.generate(analysis_prompt)

            # ChatCompletion 객체인 경우 content 추출
            if hasattr(llm_response, 'choices'):
                response_text = llm_response.choices[0].message.content
            elif isinstance(llm_response, str):
                response_text = llm_response
            else:
                response_text = str(llm_response)

            # 응답이 비어있는 경우 처리
            if not response_text or response_text.strip() == "":
                print(f"  ⚠️ Empty response from LLM")
                analysis = {
                    "reasoning": "Empty response from LLM",
                    "evolution_assessment": "unknown",
                    "error_category": "EMPTY_RESPONSE"
                }
            else:
                # JSON 파싱
                response_text = response_text.strip()

                # JSON 블록 추출 (```json ... ``` 또는 ``` ... ```)
                import re
                json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response_text)
                if json_match:
                    response_text = json_match.group(1).strip()

                # 그래도 JSON이 아니면 { } 사이만 추출
                if not response_text.startswith('{'):
                    brace_match = re.search(r'\{[\s\S]*\}', response_text)
                    if brace_match:
                        response_text = brace_match.group(0)

                analysis = json.loads(response_text)

            print(f"  ✅ Assessment: {analysis.get('evolution_assessment', 'unknown')}")
            if 'best_iteration' in analysis:
                print(f"     Best iteration: {analysis['best_iteration']}")
            if 'error_category' in analysis and analysis['error_category']:
                print(f"     Error category: {analysis['error_category']}")

        except json.JSONDecodeError as e:
            print(f"  ⚠️ Failed to parse LLM response as JSON: {e}")
            analysis = {
                "reasoning": response_text if 'response_text' in locals() else "Parse error",
                "evolution_assessment": "unknown",
                "error_category": "PARSE_ERROR"
            }
        except Exception as e:
            print(f"  ❌ Error during analysis: {type(e).__name__}: {str(e)}")
            analysis = {
                "reasoning": f"Error: {str(e)}",
                "evolution_assessment": "error",
                "error_category": "ANALYSIS_ERROR"
            }

        # 결과 저장
        result_item = {
            "idx": idx,
            "question": question if not no_gold else None,
            "gold_sql": gold_sql,
            "sql_evolution": evolution,
            "analysis": analysis
        }
        analysis_results.append(result_item)

    # 결과 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analysis_results, f, indent=2, ensure_ascii=False, cls=CustomJSONEncoder)

    print(f"\n{'='*80}")
    print(f"✅ Analysis complete! Results saved to: {output_path}")

    # 통계 출력
    if analysis_results:
        print(f"\n📊 Summary Statistics:")

        # Evolution assessment 통계
        assessment_counts = {}
        for result in analysis_results:
            assessment = result['analysis'].get('evolution_assessment', 'unknown')
            assessment_counts[assessment] = assessment_counts.get(assessment, 0) + 1

        print(f"\n  Evolution Assessment:")
        for assessment, count in sorted(assessment_counts.items(), key=lambda x: x[1], reverse=True):
            pct = (count / len(analysis_results)) * 100
            print(f"    {assessment}: {count} ({pct:.1f}%)")

        # Error category 통계 (no_gold가 아닐 때만)
        if not no_gold:
            category_counts = {}
            for result in analysis_results:
                category = result['analysis'].get('error_category')
                if category:
                    category_counts[category] = category_counts.get(category, 0) + 1

            if category_counts:
                print(f"\n  Error Categories:")
                for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / len(analysis_results)) * 100
                    print(f"    {category}: {count} ({pct:.1f}%)")

        # Refinement quality 통계
        quality_counts = {}
        for result in analysis_results:
            quality = result['analysis'].get('refinement_quality', 'unknown')
            quality_counts[quality] = quality_counts.get(quality, 0) + 1

        print(f"\n  Refinement Quality:")
        for quality, count in sorted(quality_counts.items(), key=lambda x: x[1], reverse=True):
            pct = (count / len(analysis_results)) * 100
            print(f"    {quality}: {count} ({pct:.1f}%)")

    # CSV 파일 생성
    csv_output_path = output_path.replace('.json', '.csv')
    with open(csv_output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        # 헤더 작성
        writer.writerow(['idx', 'question', 'gold_sql', 'evolution_assessment', 'error_category', 'reasoning'])

        # 데이터 작성
        for result in analysis_results:
            idx = result.get('idx', '')
            question = result.get('question', '') or ''
            gold_sql = result.get('gold_sql', '') or ''
            analysis = result.get('analysis', {})
            evolution_assessment = analysis.get('evolution_assessment', '')
            error_category = analysis.get('error_category', '') or ''
            reasoning = analysis.get('reasoning', '')

            writer.writerow([idx, question, gold_sql, evolution_assessment, error_category, reasoning])

    print(f"📊 CSV file saved to: {csv_output_path}")


def main():
    parser = argparse.ArgumentParser(description="Analyze SQL refinement evolution")
    parser.add_argument("--error_analysis", required=True,
                       help="Path to error_analysis.json")
    parser.add_argument("--config", required=True,
                       help="Path to config YAML file")
    parser.add_argument("--output", required=True,
                       help="Path to save analysis results")
    parser.add_argument("--no_gold", action='store_true',
                       help="Don't show gold SQL in analysis (only NLQ)")
    parser.add_argument("--max_samples", type=int, default=None,
                       help="Maximum number of samples to analyze")

    args = parser.parse_args()

    analyze_refine_evolution(
        error_analysis_path=args.error_analysis,
        config_path=args.config,
        output_path=args.output,
        no_gold=args.no_gold,
        max_samples=args.max_samples
    )


if __name__ == "__main__":
    main()
