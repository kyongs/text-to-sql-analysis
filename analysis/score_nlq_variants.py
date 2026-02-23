"""
NLQ variant 채점 스크립트
각 NLQ variant로 SQL 생성 후 gold SQL과 execution accuracy 비교

Usage:
  python analysis/score_nlq_variants.py --config configs/beaver_dw_openai.yaml --nlq_json analysis/nlq_analysis.json --test_n 3
  python analysis/score_nlq_variants.py --config configs/beaver_dw_openai.yaml
  python analysis/score_nlq_variants.py --config configs/beaver_dw_openai.yaml --variants gold_nlq llm_nlq
"""

import json, os, csv, argparse, yaml, sys, subprocess
from datetime import datetime
from dotenv import load_dotenv
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from src.utils.nlq_condition_hints import generate_nlq_condition_hints

# ── NLQ variant 정의 ──────────────────────────────────────────────
# col: NLQ 텍스트가 저장된 컬럼
# pass_col: PASS 여부 boolean 컬럼 (None이면 base variant)
# parent: PASS일 때 점수를 상속받을 부모 variant
VARIANTS = [
    {"col": "gold_nlq",                        "pass_col": None,                                    "parent": None},
    {"col": "llm_nlq",                         "pass_col": None,                                    "parent": None},
    {"col": "llm_nlq_rev",                     "pass_col": "llm_nlq_rev_pass",                      "parent": "llm_nlq"},
    {"col": "gold_nlq_rev",                    "pass_col": "gold_nlq_rev_pass",                     "parent": "gold_nlq"},
    {"col": "gold_nlq_rev_v2",                 "pass_col": "gold_nlq_rev_v2_pass",                  "parent": "gold_nlq"},
    {"col": "gold_nlq_verify_loop",            "pass_col": "gold_nlq_verify_loop_pass",              "parent": "gold_nlq"},
    {"col": "llm_nlq_rev_cross",               "pass_col": "llm_nlq_rev_cross_pass",                "parent": "llm_nlq"},
    {"col": "llm_nlq_rev_cross_gemini",        "pass_col": "llm_nlq_rev_cross_gemini_pass",         "parent": "llm_nlq"},
    {"col": "llm_nlq_humanistic",              "pass_col": "llm_nlq_humanistic_pass",               "parent": "llm_nlq"},
    {"col": "gold_rev_llm_nlq_mid_humanistic", "pass_col": "gold_rev_llm_nlq_mid_humanistic_pass",  "parent": "gold_nlq"},
]


def build_hints(item):
    """main.py process_item과 동일한 힌트 조립 로직"""
    all_hints = []

    evidence = item.get('evidence', '')
    if evidence:
        all_hints.append(evidence)

    mapping = item.get('mapping', {})
    if mapping:
        mapping_texts = [f"- '{phrase}' is related to {', '.join(columns)}"
                         for phrase, columns in mapping.items()]
        all_hints.append("Schema Mapping Hints:\n" + "\n".join(mapping_texts))

    join_keys = item.get('join_keys', [])
    if join_keys:
        join_str = ", ".join([f"({pair[0]} = {pair[1]})" for pair in join_keys])
        all_hints.append(f"Join Information: {join_str}")

    return "\n\n".join(all_hints)


def generate_sql(client, model_name, prompt):
    """OpenAI API로 SQL 생성 (no tools, baseline)"""
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = response.choices[0].message.content.strip()
    if content.startswith("```sql"):
        content = content[6:]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


def run_evaluation(variant_dir, config):
    """evaluation.py 실행 후 결과 반환"""
    eval_config = config.get('evaluation', {})
    db_conn = config.get('db_connection', {})
    db_password = db_conn.get('password', '')
    if db_password == 'from_env':
        db_password = os.getenv('MYSQL_PASSWORD', '')

    data_mode = config['dataset']['split']

    command = [
        'python', eval_config['script_path'],
        '--predicted_sql_path', variant_dir,
        '--ground_truth_path', eval_config['ground_truth_dir'],
        '--data_mode', data_mode,
        '--db_host', str(db_conn.get('host', '127.0.0.1')),
        '--db_port', str(db_conn.get('port', 3306)),
        '--db_user', str(db_conn.get('user', 'root')),
        '--db_password', db_password,
        '--num_cpus', str(eval_config.get('num_cpus', 8)),
        '--meta_time_out', str(eval_config.get('meta_time_out', 30.0))
    ]

    subprocess.run(command, check=True)

    exec_results_path = os.path.join(variant_dir, 'exec_results_detail.json')
    if os.path.exists(exec_results_path):
        with open(exec_results_path, 'r') as f:
            return json.load(f)
    return []


def main():
    parser = argparse.ArgumentParser(description="Score NLQ variants via SQL generation + execution accuracy")
    parser.add_argument("--config", required=True, help="Path to config YAML (e.g., configs/beaver_dw_openai.yaml)")
    parser.add_argument("--nlq_json", default="./analysis/nlq_analysis.json", help="Path to NLQ analysis JSON")
    parser.add_argument("--model", default=None, help="Override model name for SQL generation")
    parser.add_argument("--test_n", type=int, default=None, help="Run only first N items")
    parser.add_argument("--max_workers", type=int, default=4, help="Parallel workers for SQL generation")
    parser.add_argument("--output", default="./analysis/nlq_scores.csv", help="Output CSV path")
    parser.add_argument("--variants", nargs='+', default=None,
                        help="Specific variants to score (e.g., gold_nlq llm_nlq). Default: all")
    args = parser.parse_args()

    # ── Load config ──
    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    model_name = args.model or config['model']['name']
    db_type = config['dataset'].get('db_type', 'mysql')

    # ── Load data (schema, hints 등) ──
    data_loader = BeaverLoader(config)
    dataset = data_loader.load_data(load_views=False)

    # ── Load NLQ analysis ──
    with open(args.nlq_json, 'r', encoding='utf-8') as f:
        nlq_data = json.load(f)
    if args.test_n:
        nlq_data = nlq_data[:args.test_n]

    # ── Select variants ──
    variants = VARIANTS
    if args.variants:
        variants = [v for v in VARIANTS if v['col'] in args.variants]

    print(f"Model: {model_name}")
    print(f"Items: {len(nlq_data)}")
    print(f"Variants: {[v['col'] for v in variants]}")

    client = OpenAI()

    # ══ Phase 0: 기존 채점 결과 로드 ══════════════════════════════
    variant_scores = {}  # {(idx, variant_col): 0 or 1}

    existing_csv = args.output
    if os.path.exists(existing_csv):
        with open(existing_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                idx = int(row['idx'])
                for key, val in row.items():
                    if key.endswith('_score') and val != '':
                        v_col = key[:-6]  # remove '_score' suffix
                        variant_scores[(idx, v_col)] = int(val)
        loaded_variants = set(v_col for (_, v_col) in variant_scores.keys())
        print(f"Loaded existing scores from {existing_csv}: {loaded_variants}")

    # ══ Phase 1: 태스크 분류 ══════════════════════════════════════
    # generation_tasks: SQL 생성이 필요한 (idx, variant, nlq_text) 목록
    # pass_inherit: PASS여서 부모 점수를 상속받는 (idx, variant, parent) 목록
    generation_tasks = []
    pass_inherit = []

    for nlq_item in nlq_data:
        idx = nlq_item['idx']
        if idx >= len(dataset):
            print(f"Warning: idx {idx} exceeds dataset length {len(dataset)}, skipping")
            continue

        data_item = dataset[idx]

        for variant in variants:
            col = variant['col']
            pass_col = variant['pass_col']
            parent = variant['parent']

            # 기존 점수가 있으면 스킵
            if (idx, col) in variant_scores:
                continue

            if pass_col is None:
                # Base variant → 항상 생성
                nlq_text = nlq_item.get(col, '')
                if not nlq_text:
                    continue
                generation_tasks.append((idx, col, nlq_text, data_item))
            else:
                # Derived variant → PASS 여부 확인
                is_pass = nlq_item.get(pass_col, False)
                if is_pass:
                    pass_inherit.append((idx, col, parent))
                else:
                    nlq_text = nlq_item.get(col, '')
                    if not nlq_text:
                        continue
                    generation_tasks.append((idx, col, nlq_text, data_item))

    print(f"\nSQL generation needed: {len(generation_tasks)}")
    print(f"PASS (inherit parent score): {len(pass_inherit)}")

    # ══ Phase 2: SQL 생성 (병렬) ══════════════════════════════════
    sql_results = {}  # {(idx, variant_col): {"sql": ..., "db_id": ...}}

    def gen_task(task):
        idx, col, nlq_text, data_item = task
        schema = data_item.get('formatted_schema', '')
        db_id = data_item['db_id']

        hints = build_hints(data_item)

        # NLQ 키워드 기반 조건 힌트 (main.py와 동일)
        nlq_cond = generate_nlq_condition_hints(nlq_text)
        if nlq_cond:
            hints = f"{hints}\n\n{nlq_cond}" if hints else nlq_cond

        prompt = build_prompt(
            schema=schema,
            question=nlq_text,
            db_name=db_id,
            db_type=db_type,
            hints=hints
        )

        try:
            sql = generate_sql(client, model_name, prompt)
        except Exception as e:
            sql = f"Error: {str(e)[:200]}"

        return idx, col, sql, db_id

    print(f"\nGenerating SQL with {args.max_workers} workers...")
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {executor.submit(gen_task, task): task for task in generation_tasks}
        for future in tqdm(as_completed(futures), total=len(futures), desc="SQL Generation"):
            idx, col, sql, db_id = future.result()
            sql_results[(idx, col)] = {"sql": sql, "db_id": db_id}

    # ══ Phase 3: variant별 평가 ══════════════════════════════════
    # variant별로 prediction 파일 생성 → eval script 실행 → 결과 수집
    eval_base_dir = os.path.join("analysis", "nlq_score_eval")
    os.makedirs(eval_base_dir, exist_ok=True)

    # variant별로 그룹핑
    variant_predictions = {}
    for (idx, col), result in sql_results.items():
        if col not in variant_predictions:
            variant_predictions[col] = []
        variant_predictions[col].append((idx, result['sql'], result['db_id']))

    data_mode = config['dataset']['split']

    for variant_col, predictions in variant_predictions.items():
        print(f"\nEvaluating: {variant_col} ({len(predictions)} items)")
        predictions.sort(key=lambda x: x[0])

        variant_dir = os.path.join(eval_base_dir, variant_col)
        os.makedirs(variant_dir, exist_ok=True)

        # predict_{data_mode}.json (BIRD format)
        pred_dict = {}
        original_indices = []
        for i, (idx, sql, db_id) in enumerate(predictions):
            pred_dict[str(i)] = f"{sql}\t----- bird -----\t{db_id}"
            original_indices.append(idx)

        with open(os.path.join(variant_dir, f"predict_{data_mode}.json"), 'w', encoding='utf-8') as f:
            json.dump(pred_dict, f, indent=4)

        # predictions.json (test_set mode 지원)
        predictions_json = [
            {"db_id": db_id, "predicted_sql": sql, "original_index": idx}
            for idx, sql, db_id in predictions
        ]
        with open(os.path.join(variant_dir, "predictions.json"), 'w', encoding='utf-8') as f:
            json.dump(predictions_json, f, indent=4)

        # 평가 실행
        try:
            exec_results = run_evaluation(variant_dir, config)
            for result in exec_results:
                eval_idx = result['sql_idx']
                original_idx = original_indices[eval_idx]
                variant_scores[(original_idx, variant_col)] = result['res']
        except Exception as e:
            print(f"  Evaluation failed for {variant_col}: {e}")

    # ══ Phase 4: PASS 상속 ════════════════════════════════════════
    for idx, col, parent_col in pass_inherit:
        parent_score = variant_scores.get((idx, parent_col))
        if parent_score is not None:
            variant_scores[(idx, col)] = parent_score
        else:
            variant_scores[(idx, col)] = -1  # parent not scored

    # ══ Phase 5: 결과 정리 & 저장 ════════════════════════════════
    # 기존 + 신규 모든 variant 컬럼 출력
    all_scored_variants = sorted(set(v_col for (_, v_col) in variant_scores.keys()),
                                  key=lambda c: next((i for i, v in enumerate(VARIANTS) if v['col'] == c), 99))

    rows = []
    for nlq_item in nlq_data:
        idx = nlq_item['idx']
        row = {"idx": idx, "gold_sql": nlq_item.get('gold_sql', '')}
        for v_col in all_scored_variants:
            row[f"{v_col}_score"] = variant_scores.get((idx, v_col), '')
        rows.append(row)

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"SCORING SUMMARY ({len(nlq_data)} items)")
    print(f"{'='*60}")
    for v_col in all_scored_variants:
        scores = [variant_scores.get((item['idx'], v_col), -1) for item in nlq_data]
        valid = [s for s in scores if s >= 0]
        correct = sum(valid)
        total = len(valid)
        pct = 100 * correct / total if total > 0 else 0
        print(f"  {v_col:45s} {correct}/{total} ({pct:.1f}%)")

    # ── CSV ──
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # ── JSON ──
    json_path = args.output.replace('.csv', '.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {args.output} and {json_path}")


if __name__ == "__main__":
    main()
