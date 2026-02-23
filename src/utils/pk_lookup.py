"""
PK Lookup - 미리 계산된 PK 맵을 사용한 단순 조회
"""
import os
import json


class PKLookup:
    """PK 관계 조회 (사전 계산된 맵 사용)"""

    def __init__(self, map_path: str = None):
        if map_path is None:
            base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            map_path = os.path.join(base, "data/beaver/dw/pk_maps.json")

        with open(map_path, encoding='utf-8') as f:
            data = json.load(f)

        self.table_pks = data["table_pks"]       # table -> pk_column
        self.pk_children = data["pk_children"]   # parent_pk -> [child_pks]
        self.pk_parents = data["pk_parents"]     # child_pk -> parent_pk
        self.equivalent_pks = data["equivalent_pks"]  # [[pk1, pk2, ...], ...]

        # 역방향 조회용
        self._equiv_map = {}
        for group in self.equivalent_pks:
            rep = group[0]
            for pk in group:
                self._equiv_map[pk] = rep

    def get_table_pk(self, table: str) -> str:
        """테이블의 PK 반환 (TABLE.PK_COLUMN 형식)"""
        pk_col = self.table_pks.get(table)
        if pk_col:
            return f"{table}.{pk_col}"
        return None

    def get_column_pk(self, column: str) -> str:
        """컬럼이 속한 테이블의 PK 반환"""
        if '.' in column:
            table = column.split('.')[0]
            return self.get_table_pk(table)
        return None

    def get_canonical_pk(self, pk: str) -> str:
        """1:1 동치 그룹의 대표 PK 반환"""
        return self._equiv_map.get(pk, pk)

    def is_parent_of(self, pk1: str, pk2: str) -> bool:
        """pk1이 pk2의 parent인지 (pk1 -> pk2가 1:N)"""
        children = self.pk_children.get(pk1, [])
        return pk2 in children

    def is_child_of(self, pk1: str, pk2: str) -> bool:
        """pk1이 pk2의 child인지"""
        return self.pk_parents.get(pk1) == pk2

    def get_relative_level(self, columns: list) -> dict:
        """
        컬럼들의 상대적 granularity level 계산
        높을수록 더 세분화됨 (finest)

        Args:
            columns: ["TABLE1.COL1", "TABLE2.COL2", ...]

        Returns:
            {"TABLE1.PK1": 0, "TABLE2.PK2": 1, ...}
        """
        # 컬럼 -> PK 변환
        pks = set()
        for col in columns:
            pk = self.get_column_pk(col)
            if pk:
                # 1:1 동치 정규화
                pks.add(self.get_canonical_pk(pk))

        if not pks:
            return {}

        # 각 PK의 level 계산
        levels = {pk: 0 for pk in pks}

        # Parent-child 관계로 level 업데이트
        for pk in pks:
            # pk의 children 중 pks에 있는 것 찾기
            for child in self.pk_children.get(pk, []):
                canon_child = self.get_canonical_pk(child)
                if canon_child in pks:
                    levels[canon_child] = max(levels[canon_child], levels[pk] + 1)

        # 더 정확한 level 계산 (반복, 최대 N회)
        for _ in range(len(pks)):
            changed = False
            for pk in pks:
                parent = self.pk_parents.get(pk)
                if parent:
                    canon_parent = self.get_canonical_pk(parent)
                    if canon_parent in pks:
                        new_level = levels[canon_parent] + 1
                        if levels[pk] < new_level:
                            levels[pk] = new_level
                            changed = True
            if not changed:
                break

        return levels

    def needs_aggregation(self, output_columns: list, source_columns: list) -> dict:
        """
        출력 컬럼과 소스 컬럼의 level 비교로 집계 필요 여부 판단

        Args:
            output_columns: SELECT할 컬럼들
            source_columns: 계산에 사용될 컬럼들

        Returns:
            {
                "needs_agg": bool,
                "output_level": int,
                "source_level": int,
                "reason": str
            }
        """
        all_cols = output_columns + source_columns
        levels = self.get_relative_level(all_cols)

        if not levels:
            return {"needs_agg": False, "reason": "No PK info"}

        # 출력 컬럼들의 최소 level (가장 coarse)
        output_levels = []
        for col in output_columns:
            pk = self.get_column_pk(col)
            if pk:
                canon = self.get_canonical_pk(pk)
                if canon in levels:
                    output_levels.append(levels[canon])

        # 소스 컬럼들의 최대 level (가장 fine)
        source_levels = []
        for col in source_columns:
            pk = self.get_column_pk(col)
            if pk:
                canon = self.get_canonical_pk(pk)
                if canon in levels:
                    source_levels.append(levels[canon])

        if not output_levels or not source_levels:
            return {"needs_agg": False, "reason": "Missing level info"}

        out_level = min(output_levels)
        src_level = max(source_levels)

        needs = src_level > out_level

        return {
            "needs_agg": needs,
            "output_level": out_level,
            "source_level": src_level,
            "all_levels": levels,
            "reason": f"source({src_level}) > output({out_level})" if needs else ""
        }


# 싱글톤 인스턴스
_instance = None

def get_pk_lookup() -> PKLookup:
    global _instance
    if _instance is None:
        _instance = PKLookup()
    return _instance


def main():
    """테스트"""
    lookup = PKLookup()

    print("=== PK Lookup Test ===\n")

    # 테스트 1: 테이블 PK
    print("[1] Table PKs")
    for table in ["FCLT_ROOMS", "FCLT_BUILDING", "MASTER_DEPT_HIERARCHY"]:
        print(f"  {table} -> {lookup.get_table_pk(table)}")

    # 테스트 2: 컬럼의 PK
    print("\n[2] Column PKs")
    for col in ["FCLT_ROOMS.AREA_SQ_FT", "FCLT_BUILDING.FCLT_BUILDING_KEY"]:
        print(f"  {col} -> {lookup.get_column_pk(col)}")

    # 테스트 3: 상대적 level
    print("\n[3] Relative Levels")
    cols = ["FCLT_ROOMS.AREA_SQ_FT", "FCLT_BUILDING.FCLT_BUILDING_KEY", "MASTER_DEPT_HIERARCHY.DLC_NAME"]
    levels = lookup.get_relative_level(cols)
    print(f"  Columns: {cols}")
    print(f"  Levels: {levels}")

    # 테스트 4: 집계 필요 여부
    print("\n[4] Aggregation Need")

    # Case 1: 건물별 면적 합계 (필요)
    result = lookup.needs_aggregation(
        ["FCLT_BUILDING.FCLT_BUILDING_KEY"],
        ["FCLT_ROOMS.AREA_SQ_FT"]
    )
    print(f"  Building -> Room area: needs_agg={result['needs_agg']}")

    # Case 2: 방 정보 조회 (불필요)
    result = lookup.needs_aggregation(
        ["FCLT_ROOMS.AREA_SQ_FT"],
        ["FCLT_BUILDING.FCLT_BUILDING_KEY"]
    )
    print(f"  Room area with building: needs_agg={result['needs_agg']}")


if __name__ == "__main__":
    main()
