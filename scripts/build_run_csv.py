"""
build_run_csv.py
----------------
Reads three data sources from a GPT-5.2 evaluation run and produces a
consolidated analysis CSV.

Data sources
  1. exec_results_detail.json  -- per-question correctness (res: 1/0)
  2. predictions.json           -- predicted SQL & question text
  3. run_log.txt                -- tool-call trace per question

Output
  analysis.csv  with columns:
    idx, question, correct, predicted_sql, gold_sql,
    n_tool_calls, tool_calls_detail
"""

import json
import re
import csv
import os


# ── paths ────────────────────────────────────────────────────────────────
BASE      = r"c:\Users\domir\Desktop\code\text-to-sql-analysis"
RUN_DIR   = os.path.join(BASE, "outputs", "20260210_gpt-5.2")
LOG_DIR   = os.path.join(BASE, "logs", "20260210_gpt-5.2")

EXEC_PATH = os.path.join(RUN_DIR, "exec_results_detail.json")
PRED_PATH = os.path.join(RUN_DIR, "predictions.json")
LOG_PATH  = os.path.join(LOG_DIR, "run_log.txt")
OUT_PATH  = os.path.join(RUN_DIR, "analysis.csv")


# ── 1.  Load exec results  (idx -> {ground_truth, res}) ─────────────────
def load_exec_results(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {item["sql_idx"]: item for item in data}


# ── 2.  Load predictions  (idx -> {question, predicted_sql, db_id}) ─────
def load_predictions(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {item["original_index"]: item for item in data}


# ── 3.  Parse log file  (question_number -> list of tool-call dicts) ────
def parse_log(path):
    """
    Return dict:  question_number (int) ->  list of {sql, response_summary}
    Only execute_sql calls are captured.

    Log structure per question:
        ================================================================
        [Question #N] <question text>
        DB: <db_id>
        ================================================================
        ... prompt, schema, tool call log, token info ...
        *** (star separators) ***

    The '=' lines bracket the header; content follows the second '=' line
    until the next '=' header block.
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    # Locate every question header via its distinctive pattern.
    # Each header is:  ===\n[Question #N] ...\nDB: ...\n===
    header_re = re.compile(
        r"={10,}\n\[Question\s+#(\d+)\].*?\n.*?\n={10,}\n",
        re.DOTALL,
    )
    headers = list(header_re.finditer(text))

    result = {}  # question_number -> [tool_call_info, ...]

    for i, hdr in enumerate(headers):
        q_num = int(hdr.group(1))
        block_start = hdr.end()
        block_end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block = text[block_start:block_end]

        tool_calls = []

        # Find the TOOL CALL LOG section inside this block
        tcl_pos = block.find("***** TOOL CALL LOG *****")
        if tcl_pos == -1:
            result[q_num] = tool_calls
            continue

        log_section = block[tcl_pos:]

        # Each iteration line looks like:
        #   [Iteration N] <emoji> LLM Tool Call:  /  Tool Response:  /  Final SQL
        # We match each [Iteration ...] segment up to the next one (or end).
        iter_matches = list(re.finditer(
            r"\[Iteration\s+(\d+)\]\s+(.*?)(?=\[Iteration\s+\d+\]|\*{5}|\Z)",
            log_section,
            re.DOTALL,
        ))

        idx = 0
        while idx < len(iter_matches):
            content = iter_matches[idx].group(2).strip()

            # Check for an LLM Tool Call that uses execute_sql
            if "LLM Tool Call:" in content and "Function: execute_sql" in content:
                sql_str = _extract_sql_from_arguments(content)

                # The very next [Iteration] block should be the Tool Response
                response_summary = ""
                if idx + 1 < len(iter_matches):
                    next_content = iter_matches[idx + 1].group(2).strip()
                    if "Tool Response:" in next_content:
                        response_summary = _extract_response_summary(next_content)
                        idx += 1  # skip the response match

                tool_calls.append({
                    "sql": sql_str,
                    "response": response_summary,
                })

            idx += 1

        result[q_num] = tool_calls

    return result


def _extract_sql_from_arguments(content):
    """Extract SQL string from the Arguments JSON in a tool call block."""
    # Find Arguments: { ... }
    args_match = re.search(r"Arguments:\s*(\{.*)", content, re.DOTALL)
    if not args_match:
        return ""
    args_text = args_match.group(1).strip()
    # Try to parse as JSON
    try:
        args = json.loads(args_text)
        return args.get("sql", "")
    except json.JSONDecodeError:
        # Fallback: extract sql value with regex
        sql_match = re.search(r'"sql"\s*:\s*"(.*?)"', args_text, re.DOTALL)
        if sql_match:
            return sql_match.group(1).replace("\\n", "\n")
        return ""


def _extract_response_summary(content):
    """Extract a one-line summary from a Tool Response block."""
    # Look for lines after "Tool Response:"
    resp_match = re.search(r"Tool Response:\s*\n?\s*(.*)", content)
    if not resp_match:
        return ""
    first_line = resp_match.group(1).strip()
    return first_line  # e.g. "SUCCESS: 52 rows returned" or "ERROR: ..."


def truncate(s, maxlen):
    """Truncate string to maxlen, appending '...' if truncated."""
    if len(s) <= maxlen:
        return s
    return s[:maxlen] + "..."


def build_tool_call_detail(tool_calls):
    """Build semicolon-separated summary of tool calls."""
    parts = []
    for tc in tool_calls:
        sql_short = truncate(tc["sql"].replace("\n", " "), 80)
        resp = tc["response"]
        parts.append(f"SQL: {sql_short} -> {resp}")
    return "; ".join(parts)


# ── main ─────────────────────────────────────────────────────────────────
def main():
    exec_results = load_exec_results(EXEC_PATH)
    predictions  = load_predictions(PRED_PATH)
    tool_calls   = parse_log(LOG_PATH)

    # Sanity check
    all_indices = sorted(set(exec_results.keys()) | set(predictions.keys()))
    print(f"Loaded {len(exec_results)} exec results, "
          f"{len(predictions)} predictions, "
          f"{len(tool_calls)} question blocks from log")

    rows = []
    for idx in all_indices:
        pred = predictions.get(idx, {})
        er   = exec_results.get(idx, {})
        tc   = tool_calls.get(idx, [])

        row = {
            "idx":               idx,
            "question":          truncate(pred.get("question", ""), 100),
            "correct":           er.get("res", ""),
            "predicted_sql":     pred.get("predicted_sql", ""),
            "gold_sql":          er.get("ground_truth", ""),
            "n_tool_calls":      len(tc),
            "tool_calls_detail": build_tool_call_detail(tc),
        }
        rows.append(row)

    # Write CSV
    fieldnames = ["idx", "question", "correct", "predicted_sql",
                  "gold_sql", "n_tool_calls", "tool_calls_detail"]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nCSV written to: {OUT_PATH}")
    print(f"Total rows: {len(rows)}")

    # ── summary stats ──
    correct_count = sum(1 for r in rows if r["correct"] == 1)
    total = len(rows)
    accuracy = correct_count / total * 100 if total else 0
    avg_tc = sum(r["n_tool_calls"] for r in rows) / total if total else 0

    print(f"\n{'='*50}")
    print(f"  Total questions:    {total}")
    print(f"  Correct:            {correct_count}")
    print(f"  Accuracy:           {accuracy:.1f}%")
    print(f"  Avg tool calls:     {avg_tc:.2f}")
    print(f"{'='*50}")

    # Print first 5 rows preview
    print(f"\n--- First 5 rows preview ---")
    for r in rows[:5]:
        print(f"  idx={r['idx']:>2}  correct={r['correct']}  "
              f"n_tool_calls={r['n_tool_calls']}  "
              f"question={r['question'][:60]}...")


if __name__ == "__main__":
    main()
