"""
Aggregation Hint Generator
- PK level 정보 기반으로 집계 필요 여부 힌트 생성
"""
from typing import List, Dict, Optional
from src.utils.pk_lookup import get_pk_lookup


def generate_agg_hint(
    output_columns: List[str],
    source_columns: List[str],
    verbose: bool = False
) -> str:
    """
    출력/소스 컬럼 기반으로 집계 힌트 생성

    Args:
        output_columns: SELECT할 컬럼들 (TABLE.COLUMN 형식)
        source_columns: 계산에 사용될 컬럼들
        verbose: 상세 정보 포함 여부

    Returns:
        힌트 문자열
    """
    lookup = get_pk_lookup()
    result = lookup.needs_aggregation(output_columns, source_columns)

    if not result.get("needs_agg"):
        if result.get("reason") == "No PK info":
            return ""
        return ""

    # 힌트 생성
    lines = ["## Aggregation Analysis"]

    # 테이블 레벨 정보
    all_levels = result.get("all_levels", {})
    if all_levels:
        # 레벨별 그룹핑
        level_groups = {}
        for pk, level in all_levels.items():
            if level not in level_groups:
                level_groups[level] = []
            table = pk.split('.')[0]
            level_groups[level].append(table)

        lines.append("Table granularity (higher = finer grain):")
        for level in sorted(level_groups.keys()):
            tables = level_groups[level]
            lines.append(f"  Level {level}: {', '.join(tables)}")

    # 집계 경고
    out_level = result.get("output_level", 0)
    src_level = result.get("source_level", 0)

    lines.append("")
    lines.append(f"WARNING: Source data (level {src_level}) is more granular than output (level {out_level})")
    lines.append("-> This query likely needs GROUP BY and aggregation functions (SUM, COUNT, AVG, etc.)")

    return "\n".join(lines)


def extract_columns_from_mapping(mapping: Dict[str, List[str]]) -> tuple:
    """
    mapping에서 output/source 컬럼 추출 (휴리스틱)

    일반적으로:
    - "total", "sum", "count", "average" 등의 키워드가 있으면 → 집계 대상
    - 나머지는 grouping 대상 (output)
    """
    output_cols = []
    source_cols = []

    agg_keywords = ['total', 'sum', 'count', 'average', 'avg', 'number of', 'how many']

    for phrase, columns in mapping.items():
        phrase_lower = phrase.lower()

        # 집계 키워드 포함 여부
        is_agg = any(kw in phrase_lower for kw in agg_keywords)

        for col in columns:
            if is_agg:
                source_cols.append(col)
            else:
                output_cols.append(col)

    return output_cols, source_cols


def generate_agg_hint_from_item(item: dict) -> str:
    """
    데이터 item에서 aggregation 힌트 생성

    Args:
        item: 질문 데이터 (mapping 포함)

    Returns:
        힌트 문자열
    """
    mapping = item.get('mapping', {})
    if not mapping:
        return ""

    output_cols, source_cols = extract_columns_from_mapping(mapping)

    if not output_cols or not source_cols:
        # 모든 컬럼이 한 쪽에만 있으면, 테이블 기반으로 분석
        all_cols = []
        for cols in mapping.values():
            all_cols.extend(cols)

        if len(all_cols) < 2:
            return ""

        # 테이블 추출
        tables = set()
        for col in all_cols:
            if '.' in col:
                tables.add(col.split('.')[0])

        if len(tables) < 2:
            return ""

        # 테이블 레벨만 분석
        lookup = get_pk_lookup()
        levels = lookup.get_relative_level(all_cols)

        if not levels or len(set(levels.values())) <= 1:
            return ""

        # 레벨 차이 있으면 힌트
        lines = ["## Table Granularity Info"]
        level_groups = {}
        for pk, level in levels.items():
            if level not in level_groups:
                level_groups[level] = []
            table = pk.split('.')[0]
            level_groups[level].append(table)

        for level in sorted(level_groups.keys()):
            tables = level_groups[level]
            lines.append(f"  Level {level} (coarser→finer): {', '.join(tables)}")

        lines.append("")
        lines.append("Note: When selecting from coarser table but using data from finer table,")
        lines.append("      aggregation (GROUP BY + SUM/COUNT/etc.) may be needed.")

        return "\n".join(lines)

    return generate_agg_hint(output_cols, source_cols)


def main():
    """테스트"""
    print("=== Aggregation Hint Generator Test ===\n")

    # Test 1: 직접 컬럼 지정
    print("[Test 1] Direct columns")
    hint = generate_agg_hint(
        output_columns=["FCLT_BUILDING.BUILDING_NAME"],
        source_columns=["FCLT_ROOMS.AREA_SQ_FT"]
    )
    print(hint)
    print()

    # Test 2: mapping에서 추출
    print("[Test 2] From mapping")
    item = {
        "mapping": {
            "building name": ["FCLT_BUILDING.BUILDING_NAME"],
            "total area": ["FCLT_ROOMS.AREA_SQ_FT"],
            "department": ["MASTER_DEPT_HIERARCHY.DLC_NAME"]
        }
    }
    hint = generate_agg_hint_from_item(item)
    print(hint)
    print()

    # Test 3: 집계 불필요 케이스
    print("[Test 3] No aggregation needed")
    hint = generate_agg_hint(
        output_columns=["FCLT_ROOMS.ROOM_NAME", "FCLT_ROOMS.AREA_SQ_FT"],
        source_columns=["FCLT_BUILDING.BUILDING_NAME"]
    )
    print(f"Hint: '{hint}'" if hint else "No hint needed")


if __name__ == "__main__":
    main()
