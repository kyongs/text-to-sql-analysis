"""
로그 파일에서 [Note Final] 부분만 추출하여 CSV로 저장하는 스크립트
"""

import re
import csv
import sys
from pathlib import Path


def extract_final_notes(log_path: str, output_path: str = None):
    """
    로그 파일에서 Question ID와 Final Note를 추출하여 CSV로 저장

    Args:
        log_path: 로그 파일 경로
        output_path: 출력 CSV 경로 (None이면 자동 생성)
    """
    log_path = Path(log_path)
    if output_path is None:
        output_path = log_path.parent / f"{log_path.stem}_final_notes.csv"

    with open(log_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Question 블록 패턴: [Question #N] question_text
    question_pattern = r'\[Question #(\d+)\]\s*(.+?)\nDB:'

    # [Note Final] 블록 추출
    # [Note Final]부터 다음 ***** 또는 파일 끝까지
    note_final_pattern = r'\[Note Final\] 📋 Final Note:\s*\n(.*?)(?=\n\*{5}|\n={20,}|$)'

    # 각 Question 블록 찾기
    questions = list(re.finditer(question_pattern, content))

    results = []

    for i, q_match in enumerate(questions):
        question_id = int(q_match.group(1))
        question_text = q_match.group(2).strip()

        # 이 Question의 범위 결정 (현재 Question부터 다음 Question까지)
        start_pos = q_match.start()
        if i + 1 < len(questions):
            end_pos = questions[i + 1].start()
        else:
            end_pos = len(content)

        question_block = content[start_pos:end_pos]

        # 이 블록에서 [Note Final] 찾기
        note_match = re.search(note_final_pattern, question_block, re.DOTALL)

        if note_match:
            final_note = note_match.group(1).strip()
            # 들여쓰기 제거 (각 줄 앞의 공백 2개 제거)
            final_note_lines = []
            for line in final_note.split('\n'):
                if line.startswith('  '):
                    final_note_lines.append(line[2:])
                else:
                    final_note_lines.append(line)
            final_note = '\n'.join(final_note_lines)
            # 엑셀 호환성: === 제거
            final_note = final_note.replace('=== FINAL NOTE ===', 'FINAL NOTE')
        else:
            final_note = "(No Final Note)"

        results.append({
            'question_id': question_id,
            'question': question_text,
            'final_note': final_note
        })

    # CSV 저장
    with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['question_id', 'question', 'final_note'])
        writer.writeheader()
        writer.writerows(results)

    print(f"Extracted {len(results)} questions to: {output_path}")
    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_final_notes.py <log_file_path> [output_csv_path]")
        sys.exit(1)

    log_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    extract_final_notes(log_path, output_path)
