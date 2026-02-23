"""
Query Plan 생성 스크립트
- SQL 생성 전에 LLM이 쿼리 계획을 먼저 수립
- Granularity Mismatch 감지 → Window Function vs GROUP BY 결정
"""

import json
import os
import sys
import csv
import concurrent.futures
from pathlib import Path
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

client = OpenAI()

QUERY_PLAN_PROMPT = """당신은 SQL 쿼리 설계 전문가입니다.
주어진 자연어 질문과 스키마를 분석하여 Query Plan을 작성해주세요.

## 분석할 질문
{question}

## 스키마
{schema}

## 힌트 (있는 경우)
{hints}

---

## Query Plan 작성 지침

### 1. OUTPUT COLUMNS (출력 컬럼 분석)
질문이 요구하는 각 출력 컬럼에 대해:
- column_name: 출력할 컬럼 (질문에서 요구하는 정보)
- source: 테이블.컬럼 (스키마에서 매핑)
- function: 집계함수 (SUM/COUNT/AVG/MIN/MAX) 또는 "none"
- grain: 이 정보의 단위 (어떤 entity 기준인지, 예: "material", "subject", "instructor")

### 2. GRANULARITY ANALYSIS (단위 분석)
- output_grain: 출력 행의 단위 (가장 세분화된 grain)
- aggregation_grain: 집계 기준 ("for each" 뒤의 단위)
- mismatch: output_grain이 aggregation_grain보다 세분화되어 있으면 true

### 3. AGGREGATION DECISION (집계 방식 결정)
- method: "WINDOW_FUNCTION" 또는 "GROUP_BY"
- reason: 선택 이유
- pattern: 구체적인 SQL 패턴 (예: "SUM(...) OVER (PARTITION BY ...)")

---

## 응답 형식 (JSON)
```json
{{
  "output_columns": [
    {{
      "column_name": "material_title",
      "source": "TIP_MATERIAL.TITLE",
      "function": "none",
      "grain": "material"
    }},
    {{
      "column_name": "total_cost",
      "source": "TIP_MATERIAL.NEW_SHELF_PRICE",
      "function": "SUM",
      "grain": "subject"
    }}
  ],
  "granularity_analysis": {{
    "output_grain": "material",
    "aggregation_grain": "subject",
    "mismatch": true,
    "explanation": "material 상세정보(title, isbn)를 출력하면서 subject별 합계를 계산해야 함"
  }},
  "aggregation_decision": {{
    "method": "WINDOW_FUNCTION",
    "reason": "output_grain(material)이 aggregation_grain(subject)보다 세분화됨",
    "pattern": "SUM(NEW_SHELF_PRICE) OVER (PARTITION BY subject_title)"
  }}
}}
```

집계가 필요 없는 단순 조회 질문의 경우:
```json
{{
  "output_columns": [...],
  "granularity_analysis": {{
    "output_grain": "room",
    "aggregation_grain": null,
    "mismatch": false,
    "explanation": "단순 조회, 집계 없음"
  }},
  "aggregation_decision": {{
    "method": "NONE",
    "reason": "집계 함수가 필요 없는 단순 조회",
    "pattern": null
  }}
}}
```

JSON만 출력하세요. 다른 설명은 불필요합니다.
"""


def generate_query_plan(question: str, schema: str, hints: str = "", model: str = "gpt-4o") -> dict:
    """단일 질문에 대한 Query Plan 생성"""

    prompt = QUERY_PLAN_PROMPT.format(
        question=question,
        schema=schema,
        hints=hints if hints else "(없음)"
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2000
        )

        content = response.choices[0].message.content.strip()

        # JSON 추출 (```json ... ``` 형태 처리)
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()

        plan = json.loads(content)
        return {"success": True, "plan": plan}

    except json.JSONDecodeError as e:
        return {"success": False, "error": f"JSON parse error: {e}", "raw": content}
    except Exception as e:
        return {"success": False, "error": str(e)}


def load_dataset(dataset_path: str) -> list:
    """데이터셋 로드"""
    with open(dataset_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_formatted_data(formatted_path: str) -> list:
    """formatted_data.json 로드 (스키마 포함)"""
    with open(formatted_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_hints(item: dict) -> str:
    """힌트 문자열 생성"""
    hints = []

    if item.get('evidence'):
        hints.append(f"Evidence: {item['evidence']}")

    if item.get('mapping'):
        mapping_texts = [f"- '{k}' → {', '.join(v)}" for k, v in item['mapping'].items()]
        hints.append("Mapping:\n" + "\n".join(mapping_texts))

    if item.get('join_keys'):
        join_str = ", ".join([f"({p[0]} = {p[1]})" for p in item['join_keys']])
        hints.append(f"Join Keys: {join_str}")

    return "\n\n".join(hints)


def export_to_csv(results: list, csv_path: str, run_id: str = None):
    """결과를 CSV로 내보내기"""
    rows = []
    for r in results:
        row = {
            "index": r["index"],
            "question": r["question"][:200],  # 너무 길면 자르기
            "gold_sql": r["gold_sql"][:300],
        }

        if r.get("success"):
            plan = r["plan"]
            ga = plan.get("granularity_analysis", {})
            ad = plan.get("aggregation_decision", {})

            # output_columns에서 집계함수 사용하는 컬럼 추출
            agg_cols = [c for c in plan.get("output_columns", []) if c.get("function") != "none"]
            agg_cols_str = ", ".join([f"{c['function']}({c['source']})" for c in agg_cols])

            row.update({
                "success": True,
                "output_grain": ga.get("output_grain", ""),
                "aggregation_grain": ga.get("aggregation_grain", ""),
                "mismatch": ga.get("mismatch", False),
                "explanation": ga.get("explanation", "")[:150],
                "method": ad.get("method", ""),
                "reason": ad.get("reason", "")[:100],
                "pattern": ad.get("pattern", "")[:150],
                "agg_columns": agg_cols_str,
            })
        else:
            row.update({
                "success": False,
                "output_grain": "",
                "aggregation_grain": "",
                "mismatch": "",
                "explanation": r.get("error", "")[:100],
                "method": "",
                "reason": "",
                "pattern": "",
                "agg_columns": "",
            })

        if run_id:
            row["run_id"] = run_id

        rows.append(row)

    # CSV 저장
    fieldnames = ["index", "question", "success", "output_grain", "aggregation_grain",
                  "mismatch", "method", "pattern", "agg_columns", "reason", "explanation", "gold_sql"]
    if run_id:
        fieldnames.insert(0, "run_id")

    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported CSV to: {csv_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate Query Plans for dataset")
    parser.add_argument("--dataset", default="data/beaver/dw/dw.json", help="Dataset path")
    parser.add_argument("--formatted", default="data/beaver/dw/formatted_data.json", help="Formatted data path (with schema)")
    parser.add_argument("--output", default=None, help="Output file path (without extension)")
    parser.add_argument("--model", default="gpt-4o", help="Model to use")
    parser.add_argument("--test_n", type=int, default=None, help="Test with first N items")
    parser.add_argument("--test_indices", type=str, default=None, help="Comma-separated indices to test")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs (for consistency check)")
    parser.add_argument("--csv", action="store_true", help="Export to CSV")
    parser.add_argument("--workers", type=int, default=8, help="Number of parallel workers")

    args = parser.parse_args()

    # 데이터 로드
    dataset = load_dataset(args.dataset)
    formatted_data = load_formatted_data(args.formatted)

    # 테스트 모드
    if args.test_indices:
        indices = [int(i.strip()) for i in args.test_indices.split(",")]
    elif args.test_n:
        indices = list(range(args.test_n))
    else:
        indices = list(range(len(dataset)))

    # 출력 경로 기본값
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_output = args.output if args.output else f"outputs/query_plans_{timestamp}"

    os.makedirs(os.path.dirname(base_output) if os.path.dirname(base_output) else "outputs", exist_ok=True)

    all_runs_results = []

    for run in range(1, args.runs + 1):
        run_id = f"run{run}" if args.runs > 1 else None
        print(f"\n{'='*50}")
        print(f"Run {run}/{args.runs}: Generating Query Plans for {len(indices)} items using {args.model}...")
        print(f"{'='*50}")

        results = []

        def process_item(idx):
            item = dataset[idx]
            formatted_item = formatted_data[idx] if idx < len(formatted_data) else {}
            question = item.get('question', '')
            schema = formatted_item.get('formatted_schema', '')
            hints = build_hints(item)
            gold_sql = item.get('SQL', item.get('sql', ''))
            result = generate_query_plan(question, schema, hints, args.model)
            result_entry = {
                "index": idx,
                "question": question,
                "gold_sql": gold_sql,
                "hints": hints,
                **result
            }
            if run_id:
                result_entry["run_id"] = run_id
            return result_entry

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_item, idx): idx for idx in indices}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(indices), desc=f"Run {run}"):
                results.append(future.result())

        # index 순으로 정렬
        results.sort(key=lambda x: x["index"])

        all_runs_results.extend(results)

        # 개별 run 저장 (JSON)
        if args.runs > 1:
            run_output = f"{base_output}_run{run}.json"
        else:
            run_output = f"{base_output}.json"

        with open(run_output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Saved to: {run_output}")

        # 요약 통계
        success_count = sum(1 for r in results if r.get('success'))
        mismatch_count = sum(1 for r in results if r.get('success') and
                             r.get('plan', {}).get('granularity_analysis', {}).get('mismatch'))
        print(f"  Success: {success_count}/{len(results)}, Mismatch: {mismatch_count}")

    # CSV 내보내기
    if args.csv:
        if args.runs > 1:
            # 모든 run 합친 CSV
            csv_path = f"{base_output}_all_runs.csv"
            export_to_csv(all_runs_results, csv_path)
        else:
            csv_path = f"{base_output}.csv"
            export_to_csv(all_runs_results, csv_path)

    # 샘플 출력 (마지막 run에서)
    print(f"\n=== Sample Plans (Last Run) ===")
    for r in results[:3]:
        print(f"\n[{r['index']}] {r['question'][:80]}...")
        if r.get('success'):
            plan = r['plan']
            ga = plan.get('granularity_analysis', {})
            ad = plan.get('aggregation_decision', {})
            print(f"  Output Grain: {ga.get('output_grain')}")
            print(f"  Aggregation Grain: {ga.get('aggregation_grain')}")
            print(f"  Mismatch: {ga.get('mismatch')}")
            print(f"  Decision: {ad.get('method')} - {ad.get('reason', '')[:50]}")
        else:
            print(f"  Error: {r.get('error', 'Unknown')}")


if __name__ == "__main__":
    main()
