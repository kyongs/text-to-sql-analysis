"""
분석용 CSV 내보내기: idx, question, Hints, Gold SQL, final_note, sql_4o
"""

import json
import csv
import re
import sys
from pathlib import Path


def extract_final_notes_from_log(log_path: str) -> dict:
    """로그 파일에서 Question ID별 Final Note 추출"""
    with open(log_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Question 블록 패턴
    question_pattern = r'\[Question #(\d+)\]'
    note_final_pattern = r'\[Note Final\] 📋 Final Note:\s*\n(.*?)(?=\n\*{5}|\n={20,}|$)'

    questions = list(re.finditer(question_pattern, content))
    final_notes = {}

    for i, q_match in enumerate(questions):
        question_id = int(q_match.group(1))
        start_pos = q_match.start()
        end_pos = questions[i + 1].start() if i + 1 < len(questions) else len(content)
        question_block = content[start_pos:end_pos]

        note_match = re.search(note_final_pattern, question_block, re.DOTALL)
        if note_match:
            final_note = note_match.group(1).strip()
            # 들여쓰기 제거
            lines = []
            for line in final_note.split('\n'):
                if line.startswith('  '):
                    lines.append(line[2:])
                else:
                    lines.append(line)
            final_note = '\n'.join(lines)
            final_note = final_note.replace('=== FINAL NOTE ===', 'FINAL NOTE')

            # 마지막 iter의 SQL 블록 제거 (sql_4o와 중복)
            # 패턴: [Iter N] 생성된 SQL:\n```sql\n...\n```
            final_note = re.sub(
                r'\n*\[Iter \d+\] 생성된 SQL:\s*\n```sql\n.*?```\s*$',
                '',
                final_note,
                flags=re.DOTALL
            )
        else:
            final_note = "(No Final Note)"

        final_notes[question_id] = final_note

    return final_notes


def export_analysis_csv(output_dir: str, dataset_path: str = None):
    """분석용 CSV 내보내기"""
    output_path = Path(output_dir)
    exp_name = output_path.name  # e.g., "20260126_4o_rule_note"

    # 로그 파일 경로
    log_path = Path("logs") / exp_name / "run_log.txt"
    if not log_path.exists():
        print(f"Log file not found: {log_path}")
        return

    # predictions.json 로드
    with open(output_path / 'predictions.json', 'r', encoding='utf-8') as f:
        predictions = json.load(f)

    # exec_results_detail.json 로드 (Gold SQL 포함)
    with open(output_path / 'exec_results_detail.json', 'r', encoding='utf-8') as f:
        exec_results = json.load(f)

    # 데이터셋 로드 (Hints 포함)
    if dataset_path is None:
        # 기본 경로 시도
        dataset_path = "data/beaver/dw/train.json"

    hints_map = {}
    if Path(dataset_path).exists():
        with open(dataset_path, 'r', encoding='utf-8') as f:
            dataset = json.load(f)
        for i, item in enumerate(dataset):
            hints_parts = []
            if item.get('evidence'):
                hints_parts.append(f"[Evidence] {item['evidence']}")
            if item.get('mapping'):
                mapping_str = ", ".join([f"'{k}'→{','.join(v)}" for k, v in item['mapping'].items()])
                hints_parts.append(f"[Mapping] {mapping_str}")
            if item.get('join_keys'):
                join_str = ", ".join([f"({p[0]}={p[1]})" for p in item['join_keys']])
                hints_parts.append(f"[Joins] {join_str}")
            hints_map[i] = " | ".join(hints_parts) if hints_parts else ""

    # Final Notes 추출
    print(f"Extracting final notes from {log_path}...")
    final_notes = extract_final_notes_from_log(str(log_path))

    # CSV 생성
    csv_path = output_path / f"{exp_name}_analysis.csv"
    rows = []

    for i, pred in enumerate(predictions):
        idx = pred.get('original_index', i)
        question = pred['question']
        predicted_sql = pred['predicted_sql']

        # Gold SQL
        gold_sql = exec_results[i]['ground_truth'] if i < len(exec_results) else ""

        # Hints
        hints = hints_map.get(idx, "")

        # Final Note
        final_note = final_notes.get(idx, "(Not found)")

        # res (정답 여부)
        res = exec_results[i].get('res', '') if i < len(exec_results) else ''

        rows.append({
            'idx': idx,
            'res': res,
            'question': question,
            'hints': hints,
            'gold_sql': gold_sql,
            'final_note': final_note,
            'sql_4o': predicted_sql
        })

    # CSV 저장
    with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['idx', 'res', 'question', 'hints', 'gold_sql', 'final_note', 'sql_4o'])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Exported {len(rows)} rows to: {csv_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python export_analysis_csv.py <output_dir> [dataset_path]")
        print("Example: python export_analysis_csv.py outputs/20260126_4o_rule_note data/beaver/dw/train.json")
        sys.exit(1)

    output_dir = sys.argv[1]
    dataset_path = sys.argv[2] if len(sys.argv) > 2 else None
    export_analysis_csv(output_dir, dataset_path)
