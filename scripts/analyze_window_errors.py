"""
Analyze window function error patterns in text-to-SQL experiment results.

Reads exec_results_detail.json and predictions.json, cross-tabulates
window function usage in gold vs predicted SQL against correctness,
and prints detailed diagnostics.
"""

import json
import re
import os
import sys
from collections import defaultdict

# ── paths ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.join(
    r"c:\Users\domir\Desktop\code\text-to-sql-analysis",
    "outputs", "20260209_gpt-5.2",
)
EXEC_PATH = os.path.join(BASE_DIR, "exec_results_detail.json")
PRED_PATH = os.path.join(BASE_DIR, "predictions.json")

# ── window-function detection ────────────────────────────────────────────
WINDOW_KEYWORDS = [
    r"\bOVER\s*\(",
    r"\bPARTITION\s+BY\b",
    r"\bROW_NUMBER\s*\(",
    r"\bRANK\s*\(",
    r"\bDENSE_RANK\s*\(",
    r"\bNTILE\s*\(",
    r"\bLAG\s*\(",
    r"\bLEAD\s*\(",
    r"\bFIRST_VALUE\s*\(",
    r"\bLAST_VALUE\s*\(",
    r"\bNTH_VALUE\s*\(",
    r"\bCUME_DIST\s*\(",
    r"\bPERCENT_RANK\s*\(",
]
_WINDOW_RE = re.compile("|".join(WINDOW_KEYWORDS), re.IGNORECASE)


def uses_window(sql: str) -> bool:
    """Return True if *sql* contains any window-function construct."""
    return bool(_WINDOW_RE.search(sql))


def find_window_funcs(sql: str) -> list[str]:
    """Return a deduplicated list of matched window-function tokens."""
    return list({m.group().strip().upper() for m in _WINDOW_RE.finditer(sql)})


# ── load data ────────────────────────────────────────────────────────────
with open(EXEC_PATH, encoding="utf-8") as f:
    exec_results = json.load(f)

with open(PRED_PATH, encoding="utf-8") as f:
    predictions = json.load(f)

# Build a lookup from original_index -> prediction record
pred_by_idx = {p["original_index"]: p for p in predictions}

# ── classify every question ─────────────────────────────────────────────
records = []
for er in exec_results:
    idx = er["sql_idx"]
    gold_sql = er["ground_truth"]
    correct = er["res"] == 1

    pred_rec = pred_by_idx.get(idx)
    if pred_rec is None:
        print(f"WARNING: no prediction found for sql_idx={idx}", file=sys.stderr)
        continue

    pred_sql = pred_rec["predicted_sql"]
    question = pred_rec["question"]
    db_id = pred_rec["db_id"]

    gold_win = uses_window(gold_sql)
    pred_win = uses_window(pred_sql)

    records.append(
        dict(
            idx=idx,
            db_id=db_id,
            question=question,
            gold_sql=gold_sql,
            pred_sql=pred_sql,
            correct=correct,
            gold_win=gold_win,
            pred_win=pred_win,
            gold_win_funcs=find_window_funcs(gold_sql),
            pred_win_funcs=find_window_funcs(pred_sql),
        )
    )

total = len(records)

# ── helper ───────────────────────────────────────────────────────────────
def pct(n, d):
    return f"{100 * n / d:.1f}%" if d else "N/A"


# ── 1. Cross-tabulation: Gold window usage x correctness ────────────────
gold_win_correct   = [r for r in records if r["gold_win"] and r["correct"]]
gold_win_wrong     = [r for r in records if r["gold_win"] and not r["correct"]]
gold_nowin_correct = [r for r in records if not r["gold_win"] and r["correct"]]
gold_nowin_wrong   = [r for r in records if not r["gold_win"] and not r["correct"]]

# ── 2. Cross-tabulation: Predicted window usage x correctness ───────────
pred_win_correct   = [r for r in records if r["pred_win"] and r["correct"]]
pred_win_wrong     = [r for r in records if r["pred_win"] and not r["correct"]]
pred_nowin_correct = [r for r in records if not r["pred_win"] and r["correct"]]
pred_nowin_wrong   = [r for r in records if not r["pred_win"] and not r["correct"]]

# ── 3. Mismatch categories ──────────────────────────────────────────────
gold_win_pred_nowin = [r for r in records if r["gold_win"] and not r["pred_win"]]
gold_nowin_pred_win = [r for r in records if not r["gold_win"] and r["pred_win"]]

# ═══════════════════════════════════════════════════════════════════════
# PRINT REPORT
# ═══════════════════════════════════════════════════════════════════════
SEP = "=" * 80

print(SEP)
print("  WINDOW FUNCTION ERROR ANALYSIS  --  GPT-5.2 Experiment")
print(SEP)
print(f"\nTotal questions: {total}")
print(f"  Overall accuracy: {pct(sum(r['correct'] for r in records), total)}"
      f"  ({sum(r['correct'] for r in records)}/{total})")

# ── Gold cross-tab ──────────────────────────────────────────────────────
gold_win_total = len(gold_win_correct) + len(gold_win_wrong)
gold_nowin_total = len(gold_nowin_correct) + len(gold_nowin_wrong)

print(f"\n{'─' * 80}")
print("A. GOLD SQL  --  Window Function Cross-Tabulation")
print(f"{'─' * 80}")
print(f"{'':30s} {'Correct':>10s} {'Wrong':>10s} {'Total':>10s} {'Accuracy':>10s}")
print(f"{'─' * 80}")
print(f"{'Gold uses WINDOW':30s} {len(gold_win_correct):>10d} {len(gold_win_wrong):>10d}"
      f" {gold_win_total:>10d} {pct(len(gold_win_correct), gold_win_total):>10s}")
print(f"{'Gold uses NO window':30s} {len(gold_nowin_correct):>10d} {len(gold_nowin_wrong):>10d}"
      f" {gold_nowin_total:>10d} {pct(len(gold_nowin_correct), gold_nowin_total):>10s}")
print(f"{'─' * 80}")
print(f"{'TOTAL':30s} {sum(r['correct'] for r in records):>10d}"
      f" {sum(not r['correct'] for r in records):>10d}"
      f" {total:>10d}")

# ── Pred cross-tab ──────────────────────────────────────────────────────
pred_win_total = len(pred_win_correct) + len(pred_win_wrong)
pred_nowin_total = len(pred_nowin_correct) + len(pred_nowin_wrong)

print(f"\n{'─' * 80}")
print("B. PREDICTED SQL  --  Window Function Cross-Tabulation")
print(f"{'─' * 80}")
print(f"{'':30s} {'Correct':>10s} {'Wrong':>10s} {'Total':>10s} {'Accuracy':>10s}")
print(f"{'─' * 80}")
print(f"{'Pred uses WINDOW':30s} {len(pred_win_correct):>10d} {len(pred_win_wrong):>10d}"
      f" {pred_win_total:>10d} {pct(len(pred_win_correct), pred_win_total):>10s}")
print(f"{'Pred uses NO window':30s} {len(pred_nowin_correct):>10d} {len(pred_nowin_wrong):>10d}"
      f" {pred_nowin_total:>10d} {pct(len(pred_nowin_correct), pred_nowin_total):>10s}")
print(f"{'─' * 80}")
print(f"{'TOTAL':30s} {sum(r['correct'] for r in records):>10d}"
      f" {sum(not r['correct'] for r in records):>10d}"
      f" {total:>10d}")

# ── Gold vs Pred window agreement ──────────────────────────────────────
both_win     = [r for r in records if r["gold_win"] and r["pred_win"]]
neither_win  = [r for r in records if not r["gold_win"] and not r["pred_win"]]
print(f"\n{'─' * 80}")
print("C. GOLD vs PREDICTED  --  Window Function Agreement")
print(f"{'─' * 80}")
print(f"{'':40s} {'Count':>8s} {'Correct':>8s} {'Accuracy':>10s}")
print(f"{'─' * 80}")
print(f"{'Both use window':40s} {len(both_win):>8d}"
      f" {sum(r['correct'] for r in both_win):>8d}"
      f" {pct(sum(r['correct'] for r in both_win), len(both_win)):>10s}")
print(f"{'Gold=window, Pred=NO window':40s} {len(gold_win_pred_nowin):>8d}"
      f" {sum(r['correct'] for r in gold_win_pred_nowin):>8d}"
      f" {pct(sum(r['correct'] for r in gold_win_pred_nowin), len(gold_win_pred_nowin)):>10s}")
print(f"{'Gold=NO window, Pred=window':40s} {len(gold_nowin_pred_win):>8d}"
      f" {sum(r['correct'] for r in gold_nowin_pred_win):>8d}"
      f" {pct(sum(r['correct'] for r in gold_nowin_pred_win), len(gold_nowin_pred_win)):>10s}")
print(f"{'Neither uses window':40s} {len(neither_win):>8d}"
      f" {sum(r['correct'] for r in neither_win):>8d}"
      f" {pct(sum(r['correct'] for r in neither_win), len(neither_win)):>10s}")

# ── Section D: Pred used window but got it WRONG ────────────────────────
print(f"\n{'=' * 80}")
print("D. CASES: Predicted SQL USED window functions but got WRONG")
print(f"{'=' * 80}")
if pred_win_wrong:
    for r in pred_win_wrong:
        print(f"\n  [idx={r['idx']}]  db={r['db_id']}")
        print(f"  Question : {r['question'][:120]}")
        print(f"  Pred window funcs: {', '.join(r['pred_win_funcs'])}")
        print(f"  Gold window funcs: {', '.join(r['gold_win_funcs']) if r['gold_win_funcs'] else '(none)'}")
        print(f"  Gold SQL : {r['gold_sql'][:200]}...")
        print(f"  Pred SQL : {r['pred_sql'][:200]}...")
else:
    print("  (none)")

# ── Section E: Gold used window but Pred did NOT ────────────────────────
print(f"\n{'=' * 80}")
print("E. CASES: Gold SQL USED window functions but Predicted SQL did NOT")
print(f"{'=' * 80}")
if gold_win_pred_nowin:
    for r in gold_win_pred_nowin:
        status = "CORRECT" if r["correct"] else "WRONG"
        print(f"\n  [idx={r['idx']}]  db={r['db_id']}  result={status}")
        print(f"  Question : {r['question'][:120]}")
        print(f"  Gold window funcs: {', '.join(r['gold_win_funcs'])}")
        print(f"  Gold SQL : {r['gold_sql'][:200]}...")
        print(f"  Pred SQL : {r['pred_sql'][:200]}...")
else:
    print("  (none)")

# ── Section F: Pred used window but Gold did NOT ────────────────────────
print(f"\n{'=' * 80}")
print("F. CASES: Predicted SQL USED window functions but Gold SQL did NOT")
print(f"{'=' * 80}")
if gold_nowin_pred_win:
    for r in gold_nowin_pred_win:
        status = "CORRECT" if r["correct"] else "WRONG"
        print(f"\n  [idx={r['idx']}]  db={r['db_id']}  result={status}")
        print(f"  Question : {r['question'][:120]}")
        print(f"  Pred window funcs: {', '.join(r['pred_win_funcs'])}")
        print(f"  Gold SQL : {r['gold_sql'][:200]}...")
        print(f"  Pred SQL : {r['pred_sql'][:200]}...")
else:
    print("  (none)")

# ── Summary ─────────────────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print("G. SUMMARY")
print(f"{'=' * 80}")
print(f"  Total questions             : {total}")
print(f"  Gold uses window            : {gold_win_total} ({pct(gold_win_total, total)})")
print(f"  Predicted uses window       : {pred_win_total} ({pct(pred_win_total, total)})")
print()
print(f"  Accuracy when Gold has window       : {pct(len(gold_win_correct), gold_win_total)}"
      f"  ({len(gold_win_correct)}/{gold_win_total})")
print(f"  Accuracy when Gold has NO window    : {pct(len(gold_nowin_correct), gold_nowin_total)}"
      f"  ({len(gold_nowin_correct)}/{gold_nowin_total})")
print(f"  Accuracy gap (no-window minus window): "
      f"{(len(gold_nowin_correct)/gold_nowin_total - len(gold_win_correct)/gold_win_total)*100:+.1f} pp"
      if gold_win_total and gold_nowin_total else "")
print()
print(f"  Accuracy when Pred has window        : {pct(len(pred_win_correct), pred_win_total)}"
      f"  ({len(pred_win_correct)}/{pred_win_total})")
print(f"  Accuracy when Pred has NO window     : {pct(len(pred_nowin_correct), pred_nowin_total)}"
      f"  ({len(pred_nowin_correct)}/{pred_nowin_total})")
print()
print(f"  Gold=window but Pred=no-window       : {len(gold_win_pred_nowin)} cases"
      f"  (accuracy {pct(sum(r['correct'] for r in gold_win_pred_nowin), len(gold_win_pred_nowin))})")
print(f"  Gold=no-window but Pred=window       : {len(gold_nowin_pred_win)} cases"
      f"  (accuracy {pct(sum(r['correct'] for r in gold_nowin_pred_win), len(gold_nowin_pred_win))})")
print(f"  Window usage agreement rate          : {pct(len(both_win)+len(neither_win), total)}"
      f"  ({len(both_win)+len(neither_win)}/{total})")
print(SEP)
