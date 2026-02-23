"""
PK 관계 맵 생성
- table_pks: 테이블 -> PK 컬럼
- pk_parents / pk_children: parent-child 관계
- equivalent_pks: 1:1 동치 그룹

방향 판단 규칙 (join A.col_x = B.col_y):
  - col_y == B의 PK  →  B가 parent, A가 child  (A가 FK를 가짐)
  - col_x == A의 PK  →  A가 parent, B가 child
  - 둘 다 PK         →  1:1 equivalent
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analysis.pk_level_analyzer import PKLevelAnalyzer


def build_pk_maps():
    """PK 맵 생성 및 저장"""
    analyzer = PKLevelAnalyzer()

    # 1. 테이블별 PK (table -> pk_column)
    table_pks = {}
    for table, pks in analyzer.true_pks.items():
        if pks:
            table_pks[table] = pks[0]

    # 2. PK 간 관계 - join 컬럼이 PK인지 직접 판단
    pk_children = {}  # parent_pk -> set(child_pks)
    pk_parents = {}   # child_pk -> parent_pk
    equiv_pairs = []  # 1:1 쌍

    seen_pairs = set()  # 중복 제거용

    for pair in analyzer.join_keys:
        t1, c1 = pair[0].split('.', 1)
        t2, c2 = pair[1].split('.', 1)

        pk1 = table_pks.get(t1)
        pk2 = table_pks.get(t2)
        if not pk1 or not pk2:
            continue

        full_pk1 = f"{t1}.{pk1}"
        full_pk2 = f"{t2}.{pk2}"

        if full_pk1 == full_pk2:
            continue

        # 중복 제거 (A-B와 B-A 동일)
        pair_key = tuple(sorted([full_pk1, full_pk2]))
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        # 방향 판단: join 컬럼이 해당 테이블의 PK인지 확인
        c1_is_pk = (c1 == pk1)
        c2_is_pk = (c2 == pk2)

        if c1_is_pk and c2_is_pk:
            # 둘 다 PK로 조인 → 1:1
            equiv_pairs.append((full_pk1, full_pk2))

        elif c2_is_pk and not c1_is_pk:
            # B의 PK로 조인 → B가 parent, A가 child
            parent, child = full_pk2, full_pk1
            if child not in pk_parents:  # 첫 번째 parent만 유지
                pk_children.setdefault(parent, set()).add(child)
                pk_parents[child] = parent

        elif c1_is_pk and not c2_is_pk:
            # A의 PK로 조인 → A가 parent, B가 child
            parent, child = full_pk1, full_pk2
            if child not in pk_parents:
                pk_children.setdefault(parent, set()).add(child)
                pk_parents[child] = parent

        else:
            # 둘 다 PK가 아닌 컬럼으로 조인 → FK끼리 조인
            # 방향 불확실, skip
            pass

    # 3. Circular 검증 및 제거
    to_remove = []
    for child, parent in pk_parents.items():
        if parent in pk_parents and pk_parents[parent] == child:
            to_remove.append((child, parent))

    for child, parent in to_remove:
        if child in pk_parents:
            del pk_parents[child]
            pk_children.get(parent, set()).discard(child)
        if parent in pk_parents and pk_parents.get(parent) == child:
            del pk_parents[parent]
            pk_children.get(child, set()).discard(parent)

    # 4. 1:1 동치 그룹 (Union-Find)
    equivalents = {}

    def find(x):
        if x not in equivalents:
            equivalents[x] = x
        if equivalents[x] != x:
            equivalents[x] = find(equivalents[x])
        return equivalents[x]

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            equivalents[ry] = rx

    for a, b in equiv_pairs:
        union(a, b)

    equiv_groups = {}
    for pk in equivalents:
        rep = find(pk)
        equiv_groups.setdefault(rep, set()).add(pk)

    # set -> list
    pk_children_list = {k: sorted(v) for k, v in pk_children.items()}
    equiv_groups_list = [sorted(v) for v in equiv_groups.values() if len(v) > 1]

    result = {
        "table_pks": table_pks,
        "pk_children": pk_children_list,
        "pk_parents": pk_parents,
        "equivalent_pks": equiv_groups_list,
        "metadata": {
            "total_tables": len(table_pks),
            "total_relationships": sum(len(v) for v in pk_children_list.values()),
            "equivalent_groups": len(equiv_groups_list)
        }
    }

    return result


def main():
    print("Building PK maps...")
    result = build_pk_maps()

    output_path = "data/beaver/dw/pk_maps.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Saved to {output_path}")
    print(f"\n=== Summary ===")
    print(f"Tables with PK: {result['metadata']['total_tables']}")
    print(f"Parent-Child relationships: {result['metadata']['total_relationships']}")
    print(f"1:1 Equivalent groups: {result['metadata']['equivalent_groups']}")

    # Circular 체크
    parents = result['pk_parents']
    circulars = [(c, p) for c, p in parents.items() if p in parents and parents[p] == c]
    if circulars:
        print(f"\n!!! {len(circulars)} CIRCULAR relationships found !!!")
        for c, p in circulars:
            print(f"  {c} <-> {p}")
    else:
        print("\nNo circular relationships. OK")

    print(f"\n=== Sample Parent-Child ===")
    for parent, children in list(result['pk_children'].items())[:10]:
        print(f"  {parent} -> {children}")

    print(f"\n=== 1:1 Equivalent Groups ===")
    for group in result['equivalent_pks']:
        print(f"  {group}")


if __name__ == "__main__":
    main()
