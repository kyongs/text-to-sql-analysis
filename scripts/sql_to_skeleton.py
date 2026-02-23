"""
Gold SQL을 Skeleton으로 변환하는 스크립트
테이블명 → T1, T2, ...
컬럼명 → C1, C2, ...
리터럴 값 → VAL
"""

import json
import re
from collections import OrderedDict
from typing import Dict, Tuple
import argparse


def extract_skeleton(sql: str) -> Tuple[str, Dict[str, str], Dict[str, str]]:
    """
    SQL을 skeleton으로 변환
    Returns: (skeleton_sql, table_mapping, column_mapping)
    """
    # 테이블/컬럼 매핑
    table_map = OrderedDict()  # original -> T1, T2, ...
    column_map = OrderedDict()  # original -> C1, C2, ...
    table_counter = [1]
    column_counter = [1]

    def get_table_alias(name: str) -> str:
        name_upper = name.upper()
        if name_upper not in table_map:
            table_map[name_upper] = f"T{table_counter[0]}"
            table_counter[0] += 1
        return table_map[name_upper]

    def get_column_alias(name: str) -> str:
        name_upper = name.upper()
        if name_upper not in column_map:
            column_map[name_upper] = f"C{column_counter[0]}"
            column_counter[0] += 1
        return column_map[name_upper]

    # SQL 정규화
    sql = sql.strip().rstrip(';')

    # 1단계: 테이블명 추출 및 alias 매핑
    # FROM/JOIN 절에서 테이블 추출
    table_aliases = {}  # alias -> table_name

    # 패턴: FROM table alias, JOIN table alias ON
    from_pattern = r'\bFROM\s+(\w+)(?:\s+(\w+))?'
    join_pattern = r'\bJOIN\s+(\w+)(?:\s+(\w+))?\s+ON'

    for match in re.finditer(from_pattern, sql, re.IGNORECASE):
        table_name = match.group(1)
        alias = match.group(2) if match.group(2) else table_name
        table_aliases[alias.upper()] = table_name.upper()
        get_table_alias(table_name)

    for match in re.finditer(join_pattern, sql, re.IGNORECASE):
        table_name = match.group(1)
        alias = match.group(2) if match.group(2) else table_name
        table_aliases[alias.upper()] = table_name.upper()
        get_table_alias(table_name)

    # 2단계: 컬럼 추출 (table.column 또는 alias.column 형태)
    col_pattern = r'\b(\w+)\.(\w+)\b'
    for match in re.finditer(col_pattern, sql, re.IGNORECASE):
        prefix = match.group(1).upper()
        col_name = match.group(2).upper()
        # prefix가 테이블 alias면 컬럼으로 처리
        if prefix in table_aliases or prefix in [t.upper() for t in table_map.keys()]:
            get_column_alias(col_name)

    # 단독 컬럼명 (SELECT 절 등에서)
    # SELECT 뒤, GROUP BY 뒤, ORDER BY 뒤의 단어들
    select_match = re.search(r'\bSELECT\s+(DISTINCT\s+)?(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_part = select_match.group(2)
        # 함수 내부 컬럼 등 추출
        for col in re.findall(r'\b([A-Za-z_]\w*)\b', select_part):
            col_upper = col.upper()
            if col_upper not in ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NULL']:
                if col_upper not in table_aliases and col_upper not in table_map:
                    get_column_alias(col)

    # 3단계: SQL 변환
    result = sql

    # 테이블명 치환 (FROM/JOIN 절)
    def replace_table_in_from_join(match):
        keyword = match.group(1)  # FROM or JOIN
        table = match.group(2)
        alias = match.group(3)
        new_table = get_table_alias(table)
        if alias and alias.upper() != table.upper():
            return f"{keyword} {new_table} {alias}"
        return f"{keyword} {new_table}"

    result = re.sub(r'\b(FROM|JOIN)\s+(\w+)(?:\s+(\w+))?(?=\s+(?:ON|JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|$|\)))',
                    replace_table_in_from_join, result, flags=re.IGNORECASE)

    # table.column → Tx.Cy 변환
    def replace_table_col(match):
        prefix = match.group(1).upper()
        col = match.group(2).upper()
        # prefix가 alias인 경우 해당 테이블로 변환
        if prefix in table_aliases:
            table_name = table_aliases[prefix]
            new_prefix = table_map.get(table_name, prefix)
        elif prefix in table_map:
            new_prefix = table_map[prefix]
        else:
            new_prefix = prefix
        new_col = column_map.get(col, col)
        return f"{new_prefix}.{new_col}"

    result = re.sub(r'\b(\w+)\.(\w+)\b', replace_table_col, result)

    # 문자열 리터럴 → VAL
    result = re.sub(r"'[^']*'", "VAL", result)

    # 숫자 리터럴 → NUM
    result = re.sub(r'\b\d+\.?\d*\b', "NUM", result)

    # AS alias → AS A1, A2, ... (column alias 익명화)
    alias_counter = [1]
    alias_map = {}

    def replace_as_alias(match):
        alias_name = match.group(1).upper()
        if alias_name not in alias_map:
            alias_map[alias_name] = f"A{alias_counter[0]}"
            alias_counter[0] += 1
        return f" AS {alias_map[alias_name]}"

    result = re.sub(r'\s+AS\s+(\w+)', replace_as_alias, result, flags=re.IGNORECASE)

    return result, dict(table_map), dict(column_map)


def analyze_complexity(skeleton: str) -> Dict:
    """Skeleton SQL의 복잡도 분석"""
    skeleton_upper = skeleton.upper()

    return {
        'join_count': skeleton_upper.count(' JOIN '),
        'has_subquery': '(SELECT' in skeleton_upper,
        'has_cte': skeleton_upper.strip().startswith('WITH'),
        'has_distinct': 'DISTINCT' in skeleton_upper,
        'has_group_by': 'GROUP BY' in skeleton_upper,
        'has_having': 'HAVING' in skeleton_upper,
        'has_order_by': 'ORDER BY' in skeleton_upper,
        'has_union': 'UNION' in skeleton_upper,
        'has_case': 'CASE' in skeleton_upper,
        'has_window': ' OVER(' in skeleton_upper or ' OVER (' in skeleton_upper,
        'aggregate_funcs': len(re.findall(r'\b(SUM|COUNT|AVG|MAX|MIN)\s*\(', skeleton_upper)),
        'window_funcs': len(re.findall(r'\b(ROW_NUMBER|RANK|DENSE_RANK|LEAD|LAG|NTILE|FIRST_VALUE|LAST_VALUE)\s*\(', skeleton_upper)),
        'table_count': len(set(re.findall(r'\bT(\d+)\b', skeleton))),
        'column_count': len(set(re.findall(r'\bC(\d+)\b', skeleton))),
    }


def get_pattern_signature(complexity: Dict) -> str:
    """복잡도를 기반으로 패턴 시그니처 생성 (full - D 포함)"""
    parts = []
    parts.append(f"T{complexity['table_count']}")
    if complexity['join_count'] > 0:
        parts.append(f"J{complexity['join_count']}")
    if complexity['aggregate_funcs'] > 0:
        parts.append(f"A{complexity['aggregate_funcs']}")
    if complexity['has_group_by']:
        parts.append("G")
    if complexity['has_subquery']:
        parts.append("SQ")
    if complexity['has_cte']:
        parts.append("CTE")
    if complexity['has_distinct']:
        parts.append("D")
    if complexity['has_having']:
        parts.append("H")
    if complexity['has_union']:
        parts.append("U")
    if complexity['has_case']:
        parts.append("CASE")
    if complexity['has_window']:
        parts.append("W")
    return "_".join(parts)


def get_structure_signature(complexity: Dict) -> str:
    """구조적 패턴만 (T/J/A/D 제외)"""
    parts = []
    if complexity['aggregate_funcs'] > 0:
        parts.append("AGG")  # 집계 있음/없음만
    if complexity['has_group_by']:
        parts.append("G")
    if complexity['has_subquery']:
        parts.append("SQ")
    if complexity['has_cte']:
        parts.append("CTE")
    # DISTINCT 제외 - 구조에 영향 X
    if complexity['has_having']:
        parts.append("H")
    if complexity['has_union']:
        parts.append("U")
    if complexity['has_case']:
        parts.append("CASE")
    if complexity['has_window']:
        parts.append("W")
    return "_".join(parts) if parts else "SIMPLE"


def get_category(complexity: Dict) -> str:
    """큰 카테고리로 분류 (5-6개)"""
    has_agg = complexity['aggregate_funcs'] > 0 or complexity['has_group_by']
    has_nested = complexity['has_subquery'] or complexity['has_cte']
    has_union = complexity['has_union']
    has_window = complexity['has_window']

    # 우선순위: UNION > WINDOW > NESTED > AGG > SIMPLE
    if has_union:
        return "UNION"
    if has_window:
        return "WINDOW"
    if has_nested:
        return "NESTED"  # SQ or CTE
    if has_agg:
        return "AGG"
    return "SIMPLE"


def main():
    parser = argparse.ArgumentParser(description='Convert Gold SQL to Skeleton')
    parser.add_argument('--input', default='data/beaver/dw/formatted_data.json', help='Input JSON file')
    parser.add_argument('--output', default='outputs/sql_skeletons.json', help='Output JSON file')
    parser.add_argument('--csv', default='outputs/sql_skeletons.csv', help='Output CSV file')
    args = parser.parse_args()

    # 데이터 로드
    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    results = []
    pattern_counts = {}
    structure_counts = {}
    category_counts = {}

    for idx, item in enumerate(data):
        sql = item.get('sql', '')
        question = item.get('question', '')

        try:
            skeleton, table_map, column_map = extract_skeleton(sql)
            complexity = analyze_complexity(skeleton)
            pattern = get_pattern_signature(complexity)
            structure = get_structure_signature(complexity)
            category = get_category(complexity)

            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
            structure_counts[structure] = structure_counts.get(structure, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1

            results.append({
                'idx': idx,
                'question': question,
                'original_sql': sql,
                'skeleton': skeleton,
                'table_mapping': table_map,
                'column_mapping': column_map,
                'complexity': complexity,
                'pattern': pattern,
                'structure': structure,
                'category': category
            })
        except Exception as e:
            print(f"Error processing idx {idx}: {e}")
            results.append({
                'idx': idx,
                'question': question,
                'original_sql': sql,
                'skeleton': 'ERROR',
                'error': str(e)
            })

    # 결과 저장
    import os
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump({
            'results': results,
            'pattern_distribution': dict(sorted(pattern_counts.items(), key=lambda x: -x[1])),
            'structure_distribution': dict(sorted(structure_counts.items(), key=lambda x: -x[1]))
        }, f, indent=2, ensure_ascii=False)

    # CSV 저장
    import csv
    with open(args.csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['idx', 'category', 'structure', 'pattern', 'join_count', 'table_count',
                         'has_subquery', 'has_cte', 'has_window', 'has_union', 'skeleton'])
        for r in results:
            if 'complexity' in r:
                c = r['complexity']
                writer.writerow([
                    r['idx'], r.get('category', ''), r.get('structure', ''), r['pattern'],
                    c['join_count'], c['table_count'],
                    c['has_subquery'], c['has_cte'], c['has_window'], c['has_union'],
                    r['skeleton'][:500]
                ])

    # 패턴 분포 출력
    print("\n=== Full Pattern Distribution (top 15) ===")
    for pattern, count in sorted(pattern_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"{pattern}: {count}")

    print(f"\n=== Category Distribution (5 categories) ===")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        pct = count / len(results) * 100
        print(f"{cat}: {count} ({pct:.1f}%)")

    print(f"\n=== Structure Pattern (T/J/A/D 제외, {len(structure_counts)}개) ===")
    for structure, count in sorted(structure_counts.items(), key=lambda x: -x[1]):
        print(f"{structure}: {count}")

    print(f"\nTotal patterns: {len(pattern_counts)}")
    print(f"Total queries: {len(results)}")
    print(f"\nOutput saved to: {args.output}")
    print(f"CSV saved to: {args.csv}")


if __name__ == '__main__':
    main()
