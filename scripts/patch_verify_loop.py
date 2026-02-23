"""
Verify Loop: Gold NLQ → SQL 생성 → gold SQL과 실행 비교 → NLQ 보완 (최대 N회)

Flow (per item):
  gold_nlq → generate SQL → execute & compare with gold SQL
  → match: record success at iteration N
  → mismatch: show (gold SQL vs generated SQL) → revise NLQ → retry

Usage:
  python scripts/patch_verify_loop.py --config configs/beaver_dw_openai.yaml --test_n 3
  python scripts/patch_verify_loop.py --config configs/beaver_dw_openai.yaml --max_iter 5

  # 실패 문항만 재시도 (기존 결과에서 fail 항목만 추출, 이전 NLQ 이어서)
  python scripts/patch_verify_loop.py --config configs/beaver_dw_openai.yaml --fail_only --max_iter 10 --timeout 300
"""

import json, os, argparse, yaml, sys
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy import create_engine, text
from func_timeout import func_timeout, FunctionTimedOut

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.data_loader import BeaverLoader
from src.prompt_builder import build_prompt
from src.utils.nlq_condition_hints import generate_nlq_condition_hints

# ── NLQ 보완 프롬프트 ────────────────────────────────────────────
SYSTEM_REVISE = """You are given:
1. A Gold SQL query (the correct answer)
2. A Generated SQL query (produced by another AI from the NLQ below)
3. The current NLQ (natural language question)
4. Schema hints (Mappings, Join Keys, Evidence)

The Generated SQL is WRONG — it produces different results from the Gold SQL.

Your task: revise the NLQ so that an SQL generation model would produce the correct SQL.

Steps:
1. Compare Gold SQL vs Generated SQL clause by clause (SELECT, WHERE, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, etc.)
2. Identify what the NLQ failed to communicate — what caused the SQL generator to go wrong?
3. Add or clarify ONLY the missing/ambiguous information in the NLQ.

Rules:
- Preserve the original NLQ's natural language style as much as possible
- Do NOT use SQL keywords (SELECT, WHERE, JOIN, GROUP BY, etc.)
- Do NOT use raw column/table names — use natural language from the Mappings
- Be specific: if a filter value is wrong, make the correct value unmistakable in the NLQ
- The revised NLQ MUST be in English
- Output ONLY the revised question, nothing else"""

SYSTEM_REVISE_WITH_RESULTS = """You are given:
1. A Gold SQL query (the correct answer) and its execution results
2. A Generated SQL query (produced by another AI from the NLQ below) and its execution results
3. The current NLQ (natural language question)
4. Schema hints (Mappings, Join Keys, Evidence)

The Generated SQL is WRONG — it produces different results from the Gold SQL.
You can see BOTH result sets side by side to understand exactly what's different.

Your task: revise the NLQ so that an SQL generation model would produce the correct SQL.

Steps:
1. Compare the two execution result sets — what rows/columns are different?
2. Compare Gold SQL vs Generated SQL clause by clause to understand WHY the results differ
3. Identify what the NLQ failed to communicate — what caused the SQL generator to go wrong?
4. Add or clarify ONLY the missing/ambiguous information in the NLQ.

Rules:
- Preserve the original NLQ's natural language style as much as possible
- Do NOT use SQL keywords (SELECT, WHERE, JOIN, GROUP BY, etc.)
- Do NOT use raw column/table names — use natural language from the Mappings
- Be specific: if a filter value is wrong, make the correct value unmistakable in the NLQ
- If the result structure is fundamentally different (e.g. missing subtotals, wrong grouping level), describe the expected output structure clearly
- The revised NLQ MUST be in English
- Output ONLY the revised question, nothing else"""

SYSTEM_REVISE_WITH_TEMPLATE = """You are a SQL coaching assistant. You help another AI generate correct SQL by providing two things:
1. A revised NLQ (natural language question)
2. An optional SQL skeleton template (structural hint)

You are given:
- A Gold SQL query (the correct answer) and its execution results
- A Generated SQL query (wrong) and its execution results
- The current NLQ
- Schema hints
- The previous skeleton template (if any)

The Generated SQL is WRONG. Compare the two result sets and the SQL structures.

Your task: output a revised NLQ AND, if needed, a skeleton template.

## When to provide a TEMPLATE:
- The Gold SQL uses a non-obvious structure that NLQ alone cannot convey
  (e.g., window function computed in subquery before GROUP BY, post-aggregation join,
   ROLLUP+GROUPING+LAG report pattern, UNION ALL detail+total, specific function dialect)
- The SQL generator keeps choosing the wrong SQL architecture despite clear NLQ
- Do NOT provide a template if the issue is just wrong filter values or missing columns

## TEMPLATE format:
- Short structural skeleton showing the query architecture
- Use pseudocode style: table/column references OK, but keep it pattern-level
- Focus on: join order, aggregation strategy, window function placement, output structure
- Example:
  SELECT ... FROM (SELECT ..., SUM(...) OVER (PARTITION BY X) AS total FROM T) sub
  JOIN dim_table ON ...
  GROUP BY ..., sub.total

## Output format (MUST follow exactly):
[NLQ]
(revised natural language question here)

[TEMPLATE]
(skeleton template here, or write NONE if NLQ alone is sufficient)

Rules for NLQ:
- Preserve natural language style
- Do NOT use SQL keywords in the NLQ section
- Be specific about filter values, grouping, output structure
- The revised NLQ MUST be in English

Rules for TEMPLATE:
- Keep it concise (max 10 lines)
- Show the structural pattern, not the full query
- If previous template was close but had issues, refine it based on execution results
- If the issue is only value-level (wrong filter, wrong column), output NONE"""


def build_hints(item):
    parts = []
    evidence = item.get('evidence', '')
    if evidence:
        parts.append(f"Evidence: {evidence}")
    mapping = item.get('mapping', {})
    if mapping:
        lines = [f"- '{k}' → {', '.join(v)}" for k, v in mapping.items()]
        parts.append("Mappings:\n" + "\n".join(lines))
    join_keys = item.get('join_keys', [])
    if join_keys:
        jstr = ", ".join([f"({p[0]} = {p[1]})" for p in join_keys])
        parts.append(f"Join Keys: {jstr}")
    return "\n\n".join(parts)


def build_hints_for_prompt(item):
    """main.py process_item과 동일한 힌트 조립"""
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


# ── SQL 실행 비교 ────────────────────────────────────────────────
def get_mysql_url(conn_info, db_id):
    return (f"mysql+pymysql://{conn_info['user']}:{conn_info['password']}"
            f"@{conn_info['host']}:{conn_info['port']}/{db_id}")


def execute_with_results(sql, db_id, conn_info, timeout=90, max_rows=30):
    """SQL 실행하여 결과 행 + 컬럼명 반환.
    Returns dict: {success, columns, rows, row_count, error}
    """
    def _run():
        engine = create_engine(get_mysql_url(conn_info, db_id))
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            columns = list(result.keys())
            rows = result.fetchall()
            return {
                "success": True,
                "columns": columns,
                "rows": [tuple(r) for r in rows[:max_rows]],
                "row_count": len(rows),
                "error": None
            }

    try:
        return func_timeout(timeout, _run)
    except FunctionTimedOut:
        return {"success": False, "columns": [], "rows": [], "row_count": 0, "error": "Timeout"}
    except Exception as e:
        return {"success": False, "columns": [], "rows": [], "row_count": 0, "error": str(e)[:300]}


def format_result_table(exec_result, label="SQL", max_display=30):
    """실행 결과를 읽기 쉬운 테이블 텍스트로 포맷"""
    if not exec_result["success"]:
        return f"[{label}] Error: {exec_result['error']}"

    cols = exec_result["columns"]
    rows = exec_result["rows"][:max_display]
    total = exec_result["row_count"]

    if not rows:
        return f"[{label}] 0 rows returned"

    # 컬럼 너비 계산
    widths = [len(str(c)) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], min(len(str(val)), 40))

    # 헤더
    header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
    sep = "-+-".join("-" * widths[i] for i in range(len(cols)))

    lines = [f"[{label}] {total} rows (showing {len(rows)})", header, sep]
    for row in rows:
        line = " | ".join(str(v)[:40].ljust(widths[i]) for i, v in enumerate(row))
        lines.append(line)

    return "\n".join(lines)


def execute_and_compare(predicted_sql, gold_sql, db_id, conn_info, timeout=90):
    """실행 결과 비교. Returns (match: bool, pred_error: str|None)"""
    def _run():
        engine = create_engine(get_mysql_url(conn_info, db_id))
        with engine.connect() as conn:
            try:
                pred_res = conn.execute(text(predicted_sql)).fetchall()
            except Exception as e:
                return False, f"Execution error: {str(e)[:200]}"
            gold_res = conn.execute(text(gold_sql)).fetchall()
        return set(pred_res) == set(gold_res), None

    try:
        return func_timeout(timeout, _run)
    except FunctionTimedOut:
        return False, "Timeout"
    except Exception as e:
        return False, f"Error: {str(e)[:200]}"


# ── SQL 생성 ─────────────────────────────────────────────────────
def generate_sql(client, model_name, prompt):
    response = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = response.choices[0].message.content.strip()
    if content.startswith("```sql"):
        content = content[6:]
    if content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    return content.strip()


# ── NLQ 보완 ─────────────────────────────────────────────────────
def _format_past_failures(past_iterations):
    """이전 실패 이력을 간결한 요약으로 포맷"""
    if not past_iterations:
        return ""
    lines = [f"\n=== Previous Failed Attempts ({len(past_iterations)} tries, ALL FAILED) ==="]
    for it in past_iterations:
        err = f" ({it.get('error', '')})" if it.get('error') else ""
        lines.append(f"- Attempt {it['attempt']}: NLQ tried: \"{it['nlq'][:150]}...\"")
        lines.append(f"  Generated SQL (wrong): {it['generated_sql'][:200]}...{err}")
    lines.append("Do NOT repeat these same approaches. Try a fundamentally different angle.")
    return "\n".join(lines)


def _parse_nlq_and_template(response_text):
    """[NLQ]...[TEMPLATE]... 형식 파싱. Returns (nlq, template_or_None)"""
    text = response_text.strip()

    if '[NLQ]' in text:
        parts = text.split('[TEMPLATE]')
        nlq_part = parts[0].replace('[NLQ]', '').strip()
        template_part = parts[1].strip() if len(parts) > 1 else None
        if template_part and template_part.upper() == 'NONE':
            template_part = None
        return nlq_part, template_part
    # fallback: 포맷 안 지켰으면 전체를 NLQ로
    return text, None


def revise_nlq(client, model_name, item, current_nlq, gold_sql, generated_sql, error_info,
               past_iterations=None, gold_results=None, pred_results=None,
               with_template=False, current_template=None):
    past_summary = _format_past_failures(past_iterations or [])

    # 실행 결과 비교 섹션 (있을 때만)
    results_section = ""
    if gold_results and pred_results:
        gold_table = format_result_table(gold_results, label="Gold SQL Results")
        pred_table = format_result_table(pred_results, label="Generated SQL Results")
        results_section = f"""

=== Execution Result Comparison ===
{gold_table}

{pred_table}
==================================="""

    # 이전 템플릿 섹션
    template_section = ""
    if with_template and current_template:
        template_section = f"""

=== Previous Skeleton Template (provided to SQL generator) ===
{current_template}
==============================================================="""

    user_prompt = f"""{build_hints(item)}

Gold SQL (correct):
```sql
{gold_sql}
```

Generated SQL (wrong):
```sql
{generated_sql}
```
{f"Error: {error_info}" if error_info else ""}
{results_section}
{template_section}
{past_summary}

Current NLQ: {current_nlq}"""

    if with_template:
        user_prompt += """

Revise the NLQ and optionally provide/update a skeleton template.
Output in the format:
[NLQ]
(revised question)

[TEMPLATE]
(skeleton or NONE)"""
        system = SYSTEM_REVISE_WITH_TEMPLATE
    else:
        user_prompt += """

Revise the NLQ to fix what the SQL generator got wrong.
Output ONLY the revised question:"""
        system = SYSTEM_REVISE_WITH_RESULTS if (gold_results and pred_results) else SYSTEM_REVISE

    resp = client.chat.completions.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )
    raw = resp.choices[0].message.content.strip()

    if with_template:
        return _parse_nlq_and_template(raw)
    return raw, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--nlq_json", default="./analysis/nlq_analysis.json")
    parser.add_argument("--model", default=None)
    parser.add_argument("--max_iter", type=int, default=5)
    parser.add_argument("--test_n", type=int, default=None)
    parser.add_argument("--timeout", type=int, default=90, help="SQL execution timeout in seconds")
    parser.add_argument("--fail_only", action='store_true',
                        help="Re-run only failed items (success_iter==-1) from existing output, continuing from last NLQ")
    parser.add_argument("--with_results", action='store_true',
                        help="Include execution results (30 rows) comparison in NLQ revision prompt")
    parser.add_argument("--with_template", action='store_true',
                        help="Let NLQ reviser also suggest SQL skeleton templates for SQL generator")
    parser.add_argument("--output", default="./analysis/verify_loop_results.json")
    args = parser.parse_args()

    with open(args.config, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    model_name = args.model or config['model']['name']
    db_type = config['dataset'].get('db_type', 'mysql')

    # DB 연결 정보
    conn_info = config.get('db_connection', {}).copy()
    if conn_info.get('password') == 'from_env':
        conn_info['password'] = os.getenv('MYSQL_PASSWORD', '')

    # 데이터 로드
    data_loader = BeaverLoader(config)
    dataset = data_loader.load_data(load_views=False)

    with open(args.nlq_json, 'r', encoding='utf-8') as f:
        nlq_data = json.load(f)
    if args.test_n:
        nlq_data = nlq_data[:args.test_n]

    client = OpenAI()
    print(f"Model: {model_name}")
    print(f"Items: {len(nlq_data)}, Max iterations: {args.max_iter}, Timeout: {args.timeout}s")
    if args.with_results:
        print(f"  ** with_results ON: execution results (30 rows) will be included in NLQ revision **")
    if args.with_template:
        print(f"  ** with_template ON: NLQ reviser will also suggest SQL skeleton templates **")

    # ── 기존 결과 로드 (이어하기) ──
    results = []
    done_indices = set()
    fail_prev = {}  # {idx: {"nlq": last_nlq, "iterations": [...]}} — fail_only용
    result_idx_map = {}  # {idx: results 리스트 내 위치} — in-place 교체용
    if os.path.exists(args.output):
        with open(args.output, 'r', encoding='utf-8') as f:
            prev = json.load(f)
        results = prev.get("items", [])

        if args.fail_only:
            # 실패 항목을 파일에서 제거하지 않고 유지, 처리 후 in-place 교체
            for i, r in enumerate(results):
                result_idx_map[r['idx']] = i
                if r.get('success_iter', -1) != -1:
                    done_indices.add(r['idx'])
                else:
                    fail_prev[r['idx']] = {
                        "nlq": r.get('final_nlq', r.get('gold_nlq', '')),
                        "iterations": r.get('iterations', [])
                    }
            print(f"--fail_only: {len(fail_prev)} failed items to retry, {len(done_indices)} succeeded items kept")
        else:
            done_indices = {r['idx'] for r in results}
            print(f"Resuming: {len(done_indices)} items already done, skipping")

    # 누적 성공 카운터: iteration_successes[i] = i번째 시도에서 처음 성공한 수
    iteration_successes = [0] * args.max_iter
    for r in results:
        si = r.get('success_iter', -1)
        if 1 <= si <= args.max_iter:
            iteration_successes[si - 1] += 1

    for item_i, nlq_item in enumerate(nlq_data):
        idx = nlq_item['idx']

        if idx in done_indices:
            continue

        if args.fail_only and idx not in fail_prev:
            continue

        gold_nlq = nlq_item['gold_nlq']
        gold_sql = nlq_item['gold_sql']
        data_item = dataset[idx]
        db_id = data_item['db_id']

        print(f"\n[{item_i+1}/{len(nlq_data)}] idx={idx}")

        # fail_only: 이전 마지막 NLQ에서 이어서 시작, 이전 iterations 보존
        prev_data = fail_prev.get(idx, {})
        current_nlq = prev_data.get('nlq', gold_nlq)
        prev_iterations = prev_data.get('iterations', [])

        if prev_iterations:
            print(f"  (resuming from {len(prev_iterations)} previous iterations)")

        item_result = {
            "idx": idx,
            "gold_nlq": gold_nlq,
            "gold_sql": gold_sql,
            "success_iter": -1,
            "iterations": list(prev_iterations)  # 이전 이력 이어붙이기
        }

        current_template = None  # skeleton template (iteration 간 유지)

        for attempt in range(args.max_iter):
            # SQL 생성
            hints = build_hints_for_prompt(data_item)
            nlq_cond = generate_nlq_condition_hints(current_nlq)
            if nlq_cond:
                hints = f"{hints}\n\n{nlq_cond}" if hints else nlq_cond

            # skeleton template 주입
            if current_template:
                template_hint = f"SQL Structure Guide (follow this pattern):\n{current_template}"
                hints = f"{hints}\n\n{template_hint}" if hints else template_hint

            prompt = build_prompt(
                schema=data_item.get('formatted_schema', ''),
                question=current_nlq,
                db_name=db_id,
                db_type=db_type,
                hints=hints
            )

            try:
                gen_sql = generate_sql(client, model_name, prompt)
            except Exception as e:
                gen_sql = f"Error: {str(e)[:200]}"

            # 실행 비교
            match, error_info = execute_and_compare(gen_sql, gold_sql, db_id, conn_info, timeout=args.timeout)

            iter_record = {
                "attempt": attempt + 1,
                "nlq": current_nlq,
                "generated_sql": gen_sql,
                "match": match,
                "error": error_info
            }
            if current_template:
                iter_record["template"] = current_template
            item_result["iterations"].append(iter_record)

            status = "OK" if match else f"FAIL{f' ({error_info})' if error_info else ''}"
            print(f"  iter {attempt+1}: {status}")
            if not match:
                print(f"    NLQ: {current_nlq[:120]}")
                if current_template:
                    print(f"    TPL: {current_template[:100]}...")

            if match:
                item_result["success_iter"] = attempt + 1
                iteration_successes[attempt] += 1
                break

            # NLQ 보완
            if attempt < args.max_iter - 1:
                # 현재 item의 모든 이전 실패 이력 (이전 run + 현재 run)
                all_past = item_result["iterations"][:-1]  # 마지막(방금 실패)은 제외, 그건 이미 프롬프트에 있음

                # 실행 결과 비교 (--with_results 모드)
                gold_results = None
                pred_results = None
                if args.with_results or args.with_template:
                    gold_results = execute_with_results(gold_sql, db_id, conn_info, timeout=args.timeout)
                    pred_results = execute_with_results(gen_sql, db_id, conn_info, timeout=args.timeout)
                    g_count = gold_results["row_count"] if gold_results["success"] else "ERR"
                    p_count = pred_results["row_count"] if pred_results["success"] else "ERR"
                    print(f"    results: gold={g_count} rows, pred={p_count} rows")

                current_nlq, new_template = revise_nlq(
                    client, model_name, data_item,
                    current_nlq, gold_sql, gen_sql, error_info,
                    past_iterations=all_past if len(all_past) >= 3 else None,
                    gold_results=gold_results,
                    pred_results=pred_results,
                    with_template=args.with_template,
                    current_template=current_template
                )
                if args.with_template:
                    if new_template:
                        current_template = new_template
                    # template이 None이면 이전 것 유지 (NONE 출력 시만 제거됨)

        # 최종 NLQ/template 저장
        item_result["final_nlq"] = current_nlq
        if current_template:
            item_result["final_template"] = current_template
        if idx in result_idx_map:
            # in-place 교체 (--fail_only: 기존 실패 항목 덮어쓰기)
            results[result_idx_map[idx]] = item_result
        else:
            results.append(item_result)
            result_idx_map[idx] = len(results) - 1

        # ── 문항마다 중간 저장 ──
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump({
                "config": {"model": model_name, "max_iter": args.max_iter, "n_items": len(nlq_data)},
                "summary": {
                    "cumulative": [sum(iteration_successes[:i+1]) for i in range(args.max_iter)],
                    "per_iter": iteration_successes,
                    "total": len(nlq_data),
                    "done": len(results)
                },
                "items": results
            }, f, ensure_ascii=False, indent=2)

    # ── Summary ──
    n = len(nlq_data)
    print(f"\n{'='*60}")
    print(f"VERIFY LOOP RESULTS ({n} items, max {args.max_iter} iterations)")
    print(f"{'='*60}")

    cumulative = 0
    for i in range(args.max_iter):
        cumulative += iteration_successes[i]
        print(f"  After iter {i+1}: {cumulative}/{n} ({100*cumulative/n:.1f}%)  [+{iteration_successes[i]} new]")

    failed = n - cumulative
    print(f"  Failed all:    {failed}/{n} ({100*failed/n:.1f}%)")

    # ── 저장 ──
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump({
            "config": {"model": model_name, "max_iter": args.max_iter, "n_items": n},
            "summary": {
                "cumulative": [sum(iteration_successes[:i+1]) for i in range(args.max_iter)],
                "per_iter": iteration_successes,
                "total": n
            },
            "items": results
        }, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {args.output}")

    # ── nlq_analysis.json에 최종 NLQ 컬럼 추가 ──
    with open(args.nlq_json, 'r', encoding='utf-8') as f:
        full_nlq_data = json.load(f)

    result_map = {r['idx']: r for r in results}
    for nlq_item in full_nlq_data:
        idx = nlq_item['idx']
        if idx in result_map:
            r = result_map[idx]
            nlq_item['gold_nlq_verify_loop'] = r['final_nlq']
            nlq_item['gold_nlq_verify_loop_pass'] = (r['success_iter'] == 1)
            nlq_item['gold_nlq_verify_loop_iter'] = r['success_iter']

    with open(args.nlq_json, 'w', encoding='utf-8') as f:
        json.dump(full_nlq_data, f, ensure_ascii=False, indent=2)
    print(f"Appended verify_loop columns to {args.nlq_json}")


if __name__ == "__main__":
    main()
