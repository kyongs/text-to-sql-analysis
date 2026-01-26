"""
데이터셋의 Gold SQL에서 skeleton hints를 추출하여 저장하는 스크립트
"""

import json
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.skeleton_hint import (
    extract_skeleton_hints,
    format_skeleton_hint,
    get_skeleton_hint_stats
)


def generate_and_save_skeleton_hints(dataset_path: str, output_path: str = None):
    """
    데이터셋에서 skeleton hints 추출 및 저장

    Args:
        dataset_path: 원본 데이터셋 경로
        output_path: 출력 파일 경로 (None이면 자동 생성)
    """
    dataset_path = Path(dataset_path)

    if output_path is None:
        output_path = dataset_path.parent / f"{dataset_path.stem}_skeleton_hints.json"
    else:
        output_path = Path(output_path)

    # 데이터셋 로드
    with open(dataset_path, 'r', encoding='utf-8') as f:
        dataset = json.load(f)

    print(f"Loaded {len(dataset)} items from {dataset_path}")

    # skeleton hints 추출
    hints_data = []
    all_hints = []

    for i, item in enumerate(dataset):
        gold_sql = item.get('sql') or item.get('gold_sql') or item.get('SQL', '')

        hints = extract_skeleton_hints(gold_sql)
        formatted = format_skeleton_hint(hints)

        hints_data.append({
            'index': i,
            'hints': hints,
            'formatted': formatted
        })
        all_hints.append(hints)

        # 샘플 출력 (처음 3개)
        if i < 3:
            print(f"\n[Item {i}]")
            print(f"  Gold SQL: {gold_sql[:100]}...")
            print(f"  Hints: {hints}")
            if formatted:
                print(f"  Formatted:\n    {formatted.replace(chr(10), chr(10) + '    ')}")

    # 통계 출력
    stats = get_skeleton_hint_stats(all_hints)
    print(f"\n=== Skeleton Hint Statistics ===")
    print(f"Total items: {len(dataset)}")
    for key, count in sorted(stats.items(), key=lambda x: -x[1]):
        pct = count / len(dataset) * 100
        print(f"  {key}: {count} ({pct:.1f}%)")

    # 저장
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(hints_data, f, ensure_ascii=False, indent=2)

    print(f"\nSaved skeleton hints to: {output_path}")

    return hints_data


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_skeleton_hints.py <dataset_path> [output_path]")
        print("Example: python generate_skeleton_hints.py data/beaver/dw/train.json")
        sys.exit(1)

    dataset_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    generate_and_save_skeleton_hints(dataset_path, output_path)
