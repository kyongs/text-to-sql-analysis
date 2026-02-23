"""
PK Level Analyzer
- 테이블 간 1:1 vs 1:N 관계 분석
- 집계 필요 여부 판단을 위한 granularity 분석
"""
import os
import sys
import json
from collections import defaultdict

# Windows UTF-8 설정
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class PKLevelAnalyzer:
    """PK/FK 기반 테이블 레벨 분석기"""

    def __init__(self, data_path: str = "./data/beaver/dw"):
        self.data_path = data_path
        self.pk_candidates = {}  # table -> [pk_columns]
        self.join_keys = []      # [[table1.col, table2.col], ...]
        self.join_graph = defaultdict(set)  # table -> set of (other_table, join_col, other_col)
        self.true_pks = {}       # table -> [true_pk_columns] (refined)
        self.fk_map = {}         # (table, column) -> target_table

        self._load_data()
        self._build_join_graph()
        self._infer_true_pks()

    def _load_data(self):
        """PK 후보와 조인 키 로드"""
        pk_path = os.path.join(self.data_path, "pk_candidates_from_joins.json")
        join_path = os.path.join(self.data_path, "dw_join_keys.json")

        with open(pk_path, encoding='utf-8') as f:
            self.pk_candidates = json.load(f)

        with open(join_path, encoding='utf-8') as f:
            self.join_keys = json.load(f)

    def _build_join_graph(self):
        """조인 관계 그래프 구축"""
        for pair in self.join_keys:
            t1_col, t2_col = pair[0], pair[1]
            t1, c1 = t1_col.split('.', 1)
            t2, c2 = t2_col.split('.', 1)

            # 양방향 그래프
            self.join_graph[t1].add((t2, c1, c2))
            self.join_graph[t2].add((t1, c2, c1))

    def _infer_true_pks(self):
        """
        조인 패턴에서 실제 PK와 FK 구분
        - 컬럼명이 테이블명과 매칭되면 해당 테이블의 PK
        - 예: FCLT_ROOM_KEY -> FCLT_ROOMS의 PK
             FCLT_BUILDING_KEY -> FCLT_BUILDING의 PK (FCLT_ROOMS에서는 FK)
        """
        # 1. 테이블명과 컬럼명 매칭으로 PK 추론
        for table, candidates in self.pk_candidates.items():
            true_pks = []
            for col in candidates:
                # 컬럼이 자기 테이블을 가리키는지 (e.g., FCLT_ROOM_KEY for FCLT_ROOMS)
                col_base = col.replace('_KEY', '').replace('_ID', '').replace('_CODE', '')
                table_base = table.rstrip('S').replace('_HIST', '').replace('_ALL', '')

                # 매칭 체크: FCLT_ROOM -> FCLT_ROOM(S)
                if col_base == table_base or col_base == table or col_base + 'S' == table:
                    true_pks.append(col)
                # 특수 케이스: KEY가 테이블명에 포함
                elif table_base in col_base:
                    true_pks.append(col)

            self.true_pks[table] = true_pks if true_pks else candidates[:1]  # fallback to first

        # 2. FK 맵 구축: 다른 테이블의 PK를 참조하는 컬럼
        for pair in self.join_keys:
            t1, c1 = pair[0].split('.', 1)
            t2, c2 = pair[1].split('.', 1)

            # c1이 t2의 PK면, t1.c1은 t2를 가리키는 FK
            if c2 in self.true_pks.get(t2, []):
                self.fk_map[(t1, c1)] = t2
            # c2가 t1의 PK면, t2.c2는 t1을 가리키는 FK
            if c1 in self.true_pks.get(t1, []):
                self.fk_map[(t2, c2)] = t1

    def is_pk(self, table: str, column: str) -> bool:
        """해당 컬럼이 테이블의 실제 PK인지"""
        pks = self.true_pks.get(table, [])
        return column in pks

    def is_fk(self, table: str, column: str) -> str:
        """해당 컬럼이 FK면 참조하는 테이블명 반환, 아니면 None"""
        return self.fk_map.get((table, column))

    def get_relationship(self, table1: str, col1: str, table2: str, col2: str) -> str:
        """
        두 테이블 간 관계 분석
        Returns: "1:1", "1:N", "N:1", "N:M", "unknown"

        - 1:N: table1 is parent (one side), table2 is child (many side)
        - N:1: table1 is child (many side), table2 is parent (one side)
        """
        is_pk1 = self.is_pk(table1, col1)
        is_pk2 = self.is_pk(table2, col2)
        is_fk1 = self.is_fk(table1, col1)
        is_fk2 = self.is_fk(table2, col2)

        # FK -> PK 관계가 명확하면 사용
        if is_fk1 == table2 and is_pk2:
            return "N:1"  # table1(N) -> table2(1)
        if is_fk2 == table1 and is_pk1:
            return "1:N"  # table1(1) <- table2(N)

        # 일반적인 PK 기반 판단
        if is_pk1 and is_pk2:
            return "1:1"
        elif is_pk1 and not is_pk2:
            return "1:N"  # table1 is parent (1), table2 is child (N)
        elif not is_pk1 and is_pk2:
            return "N:1"  # table1 is child (N), table2 is parent (1)
        else:
            return "N:M"  # 둘 다 PK가 아님

    def find_join_path(self, from_table: str, to_table: str, visited: set = None) -> list:
        """
        BFS로 두 테이블 간 조인 경로 찾기
        Returns: [(t1, t2, c1, c2), ...] or empty list if no path
        """
        if visited is None:
            visited = set()

        if from_table == to_table:
            return []

        from collections import deque
        queue = deque([(from_table, [])])
        visited = {from_table}

        while queue:
            current, path = queue.popleft()

            for other_table, c1, c2 in self.join_graph.get(current, []):
                if other_table in visited:
                    continue

                new_path = path + [(current, other_table, c1, c2)]

                if other_table == to_table:
                    return new_path

                visited.add(other_table)
                queue.append((other_table, new_path))

        return []  # No path found

    def analyze_join_path(self, tables: list) -> dict:
        """
        테이블 리스트의 조인 경로 분석 (중간 테이블 경유 포함)
        Returns: {
            "path": [(t1, t2, relationship), ...],
            "granularity": "finest" | "coarsest" table,
            "needs_groupby": bool
        }
        """
        if len(tables) < 2:
            return {"path": [], "granularity": tables[0] if tables else None, "needs_groupby": False}

        all_tables = set(tables)
        path = []
        granularity_levels = {t: 0 for t in tables}  # 높을수록 더 세분화된 grain

        # 모든 테이블 쌍에 대해 경로 찾기
        for i, t1 in enumerate(tables):
            for t2 in tables[i+1:]:
                # 직접 연결 확인
                direct_found = False
                for other_table, c1, c2 in self.join_graph.get(t1, []):
                    if other_table == t2:
                        rel = self.get_relationship(t1, c1, t2, c2)
                        path.append((t1, t2, c1, c2, rel))
                        direct_found = True

                        # granularity 레벨 업데이트
                        if rel == "1:N":
                            granularity_levels[t2] += 1
                        elif rel == "N:1":
                            granularity_levels[t1] += 1
                        break

                # 직접 연결 없으면 경로 탐색
                if not direct_found:
                    join_path = self.find_join_path(t1, t2)
                    if join_path:
                        # 경로의 각 단계에서 granularity 변화 추적
                        cumulative_from_t1 = 0
                        for step_from, step_to, c1, c2 in join_path:
                            rel = self.get_relationship(step_from, c1, step_to, c2)

                            if rel == "N:1":
                                cumulative_from_t1 -= 1  # coarser
                            elif rel == "1:N":
                                cumulative_from_t1 += 1  # finer

                            # 중간 테이블도 추적에 추가
                            if step_to not in all_tables:
                                all_tables.add(step_to)

                        path.append((t1, t2, "via_path", join_path, f"diff={cumulative_from_t1}"))

                        # t2의 상대적 granularity 업데이트
                        if cumulative_from_t1 > 0:
                            granularity_levels[t2] += 1
                        elif cumulative_from_t1 < 0:
                            granularity_levels[t1] += 1

        # 가장 세분화된(finest) 테이블 찾기
        finest = max(granularity_levels, key=granularity_levels.get) if granularity_levels else None
        coarsest = min(granularity_levels, key=granularity_levels.get) if granularity_levels else None

        # GROUP BY 필요 여부: finest가 아닌 테이블 컬럼을 SELECT하면 필요
        needs_groupby = len(set(granularity_levels.values())) > 1

        return {
            "path": path,
            "granularity_levels": granularity_levels,
            "finest_table": finest,
            "coarsest_table": coarsest,
            "needs_groupby": needs_groupby
        }

    def analyze_columns(self, columns: list) -> dict:
        """
        컬럼 리스트 분석 (table.column 형식)

        Args:
            columns: ["FCLT_ROOMS.FCLT_ROOM_KEY", "FCLT_BUILDING.FCLT_BUILDING_KEY"]

        Returns:
            {
                "tables": ["FCLT_ROOMS", "FCLT_BUILDING"],
                "table_analysis": {
                    "FCLT_ROOMS": {"columns": [...], "has_pk": True, "grain_level": 1},
                    ...
                },
                "relationships": [...],
                "needs_groupby": bool,
                "groupby_columns": [...] if needed
            }
        """
        # 테이블별 컬럼 분류
        table_cols = defaultdict(list)
        for col in columns:
            if '.' in col:
                table, column = col.split('.', 1)
                table_cols[table].append(column)
            else:
                table_cols['_unknown'].append(col)

        tables = [t for t in table_cols.keys() if t != '_unknown']

        # 각 테이블 분석
        table_analysis = {}
        for table, cols in table_cols.items():
            if table == '_unknown':
                continue
            has_pk = any(self.is_pk(table, c) for c in cols)
            pk_cols = [c for c in cols if self.is_pk(table, c)]
            non_pk_cols = [c for c in cols if not self.is_pk(table, c)]

            table_analysis[table] = {
                "columns": cols,
                "pk_columns": pk_cols,
                "non_pk_columns": non_pk_cols,
                "has_pk": has_pk
            }

        # 조인 경로 분석
        join_analysis = self.analyze_join_path(tables)

        # GROUP BY 컬럼 추정
        groupby_columns = []
        if join_analysis["needs_groupby"] and join_analysis["coarsest_table"]:
            coarsest = join_analysis["coarsest_table"]
            if coarsest in table_analysis:
                # coarsest 테이블의 PK로 GROUP BY
                groupby_columns = [f"{coarsest}.{c}" for c in table_analysis[coarsest].get("pk_columns", [])]

        return {
            "tables": tables,
            "table_analysis": table_analysis,
            "relationships": join_analysis["path"],
            "granularity_levels": join_analysis.get("granularity_levels", {}),
            "finest_table": join_analysis.get("finest_table"),
            "coarsest_table": join_analysis.get("coarsest_table"),
            "needs_groupby": join_analysis["needs_groupby"],
            "groupby_columns": groupby_columns
        }

    def explain_aggregation_need(self, output_columns: list, source_columns: list) -> dict:
        """
        출력 컬럼과 소스 컬럼을 분석해서 집계 필요 여부 설명

        Args:
            output_columns: SELECT할 컬럼 ["BUILDINGS.BUILDING_NAME"]
            source_columns: 계산에 필요한 컬럼 ["FCLT_ROOMS.AREA_SQ_FT"]

        Returns:
            {
                "needs_aggregation": bool,
                "reason": str,
                "suggested_groupby": [...],
                "suggested_agg_function": str
            }
        """
        all_columns = output_columns + source_columns
        analysis = self.analyze_columns(all_columns)

        output_tables = set()
        source_tables = set()

        for col in output_columns:
            if '.' in col:
                output_tables.add(col.split('.')[0])

        for col in source_columns:
            if '.' in col:
                source_tables.add(col.split('.')[0])

        granularity = analysis.get("granularity_levels", {})

        # 출력 테이블이 source 테이블보다 coarse한지 확인
        needs_agg = False
        reason = ""

        for out_t in output_tables:
            for src_t in source_tables:
                out_level = granularity.get(out_t, 0)
                src_level = granularity.get(src_t, 0)

                if src_level > out_level:
                    needs_agg = True
                    reason = f"{src_t} is finer grain than {out_t} (N:1 relationship)"
                    break
            if needs_agg:
                break

        return {
            "needs_aggregation": needs_agg,
            "reason": reason,
            "output_tables": list(output_tables),
            "source_tables": list(source_tables),
            "granularity_levels": granularity,
            "suggested_groupby": analysis.get("groupby_columns", []),
            "analysis": analysis
        }


def test_analyzer():
    """테스트"""
    analyzer = PKLevelAnalyzer()

    print("=" * 60)
    print("PK Level Analyzer Test")
    print("=" * 60)

    # 테스트 0: 추론된 PK 확인
    print("\n[Test 0] Inferred True PKs (sample)")
    sample_tables = ["FCLT_ROOMS", "FCLT_BUILDING", "FCLT_BUILDING_ADDRESS",
                     "BUILDINGS", "MASTER_DEPT_HIERARCHY", "FCLT_ORGANIZATION"]
    for table in sample_tables:
        pks = analyzer.true_pks.get(table, [])
        print(f"  {table}: {pks}")

    # 테스트 1: PK vs FK 확인
    print("\n[Test 1] PK/FK Check")
    test_cases = [
        ("FCLT_ROOMS", "FCLT_ROOM_KEY"),
        ("FCLT_ROOMS", "FCLT_BUILDING_KEY"),
        ("FCLT_ROOMS", "FCLT_ORGANIZATION_KEY"),
        ("BUILDINGS", "BUILDING_KEY"),
        ("FCLT_BUILDING", "FCLT_BUILDING_KEY"),
        ("MASTER_DEPT_HIERARCHY", "DLC_KEY"),
    ]
    for table, col in test_cases:
        is_pk = analyzer.is_pk(table, col)
        fk_target = analyzer.is_fk(table, col)
        if is_pk:
            status = "PK"
        elif fk_target:
            status = f"FK -> {fk_target}"
        else:
            status = "neither"
        print(f"  {table}.{col}: {status}")

    # 테스트 2: 관계 분석
    print("\n[Test 2] Relationship Analysis")
    rel_cases = [
        ("FCLT_ROOMS", "FCLT_BUILDING_KEY", "FCLT_BUILDING", "FCLT_BUILDING_KEY"),
        ("FCLT_ROOMS", "FCLT_ORGANIZATION_KEY", "FCLT_ORGANIZATION", "FCLT_ORGANIZATION_KEY"),
        ("FCLT_ORG_DLC_KEY", "DLC_KEY", "MASTER_DEPT_HIERARCHY", "DLC_KEY"),
    ]
    for t1, c1, t2, c2 in rel_cases:
        rel = analyzer.get_relationship(t1, c1, t2, c2)
        print(f"  {t1}.{c1} -> {t2}.{c2}: {rel}")

    # 테스트 3: 컬럼 분석
    print("\n[Test 3] Column Analysis")
    columns = [
        "FCLT_BUILDING_ADDRESS.FCLT_BUILDING_KEY",
        "FCLT_BUILDING_ADDRESS.CITY",
        "FCLT_ROOMS.FCLT_ROOM_KEY",
        "FCLT_ROOMS.AREA_SQ_FT"
    ]
    result = analyzer.analyze_columns(columns)
    print(f"  Columns: {columns}")
    print(f"  Tables: {result['tables']}")
    print(f"  Granularity: {result['granularity_levels']}")
    print(f"  Finest: {result['finest_table']}, Coarsest: {result['coarsest_table']}")
    print(f"  Needs GROUP BY: {result['needs_groupby']}")
    print(f"  Suggested GROUP BY: {result['groupby_columns']}")

    # 테스트 4: 집계 필요 여부 분석
    print("\n[Test 4] Aggregation Need Analysis")
    output = ["FCLT_BUILDING_ADDRESS.CITY"]
    source = ["FCLT_ROOMS.AREA_SQ_FT"]
    agg_result = analyzer.explain_aggregation_need(output, source)
    print(f"  Output: {output}")
    print(f"  Source: {source}")
    print(f"  Needs Aggregation: {agg_result['needs_aggregation']}")
    print(f"  Reason: {agg_result['reason']}")

    # 테스트 5: 실제 질문 케이스
    print("\n[Test 5] Real Question Cases")

    # Case 1: "History 학과의 건물 주소 목록" - GROUP BY 불필요
    print("\n  Case 1: Building addresses for History dept (no aggregation)")
    output = ["FCLT_BUILDING_ADDRESS.CITY", "FCLT_BUILDING_ADDRESS.POSTAL_CODE"]
    source = ["MASTER_DEPT_HIERARCHY.DLC_NAME"]  # filter only
    result = analyzer.explain_aggregation_need(output, source)
    print(f"    Output: {output}")
    print(f"    Source: {source}")
    print(f"    Needs Agg: {result['needs_aggregation']} | Reason: {result['reason']}")

    # Case 2: "건물별 총 면적" - GROUP BY 필요
    print("\n  Case 2: Total area per building (needs SUM + GROUP BY)")
    output = ["FCLT_BUILDING.FCLT_BUILDING_KEY"]
    source = ["FCLT_ROOMS.AREA_SQ_FT"]
    result = analyzer.explain_aggregation_need(output, source)
    print(f"    Output: {output}")
    print(f"    Source: {source}")
    print(f"    Needs Agg: {result['needs_aggregation']} | Reason: {result['reason']}")

    # Case 3: "학과별 방 개수" - GROUP BY + COUNT 필요
    print("\n  Case 3: Room count per department (needs COUNT + GROUP BY)")
    output = ["MASTER_DEPT_HIERARCHY.DLC_NAME"]
    source = ["FCLT_ROOMS.FCLT_ROOM_KEY"]
    result = analyzer.explain_aggregation_need(output, source)
    print(f"    Output: {output}")
    print(f"    Source: {source}")
    print(f"    Needs Agg: {result['needs_aggregation']} | Reason: {result['reason']}")

    print("\n" + "=" * 60)


def evaluate_on_dataset():
    """데이터셋에서 GROUP BY 예측 정확도 평가"""
    import re
    import yaml

    # 데이터 로드
    config_path = "configs/beaver_dw_openai.yaml"
    with open(config_path, encoding='utf-8') as f:
        config = yaml.safe_load(f)

    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.data_loader import BeaverLoader

    loader = BeaverLoader(config)
    data = loader.load_data(load_views=False)

    analyzer = PKLevelAnalyzer()

    # 결과 저장
    results = []
    correct = 0
    total = 0

    for idx, item in enumerate(data):
        gold_sql = item.get('sql', item.get('SQL', item.get('query', '')))
        if not gold_sql:
            continue

        # Gold SQL에서 GROUP BY 여부 확인
        sql_upper = gold_sql.upper()
        has_groupby = 'GROUP BY' in sql_upper

        # Gold SQL에서 테이블 추출 (간단한 파싱)
        # FROM/JOIN 뒤의 테이블명 추출
        table_pattern = r'(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_]*)'
        tables = list(set(re.findall(table_pattern, sql_upper)))

        if len(tables) < 2:
            continue  # 단일 테이블은 스킵

        # 분석
        analysis = analyzer.analyze_join_path(tables)
        predicted_groupby = analysis.get('needs_groupby', False)

        match = (predicted_groupby == has_groupby)
        if match:
            correct += 1
        total += 1

        results.append({
            'idx': idx,
            'tables': tables,
            'gold_groupby': has_groupby,
            'pred_groupby': predicted_groupby,
            'match': match,
            'granularity': analysis.get('granularity_levels', {})
        })

    # 결과 출력
    print("=" * 60)
    print("GROUP BY Prediction Evaluation")
    print("=" * 60)
    print(f"Total multi-table queries: {total}")
    print(f"Correct predictions: {correct}")
    print(f"Accuracy: {correct/total*100:.1f}%")

    # Confusion matrix
    tp = sum(1 for r in results if r['gold_groupby'] and r['pred_groupby'])
    fp = sum(1 for r in results if not r['gold_groupby'] and r['pred_groupby'])
    fn = sum(1 for r in results if r['gold_groupby'] and not r['pred_groupby'])
    tn = sum(1 for r in results if not r['gold_groupby'] and not r['pred_groupby'])

    print(f"\nConfusion Matrix:")
    print(f"  Predicted GROUP BY | Predicted No GROUP BY")
    print(f"  TP: {tp:3d}            | FN: {fn:3d}   (Actual GROUP BY)")
    print(f"  FP: {fp:3d}            | TN: {tn:3d}   (Actual No GROUP BY)")

    if tp + fp > 0:
        precision = tp / (tp + fp)
        print(f"\nPrecision: {precision:.2f}")
    if tp + fn > 0:
        recall = tp / (tp + fn)
        print(f"Recall: {recall:.2f}")

    # 오류 케이스 샘플
    print("\n" + "-" * 60)
    print("Sample Errors (max 5):")
    errors = [r for r in results if not r['match']][:5]
    for e in errors:
        print(f"\n  idx={e['idx']}: tables={e['tables']}")
        print(f"    Gold: {'GROUP BY' if e['gold_groupby'] else 'No GROUP BY'}")
        print(f"    Pred: {'GROUP BY' if e['pred_groupby'] else 'No GROUP BY'}")
        print(f"    Granularity: {e['granularity']}")

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PK Level Analyzer")
    parser.add_argument("--test", action="store_true", help="Run tests")
    parser.add_argument("--eval", action="store_true", help="Evaluate on dataset")
    parser.add_argument("--columns", nargs="+", help="Columns to analyze (table.column format)")
    parser.add_argument("--output", nargs="+", help="Output columns")
    parser.add_argument("--source", nargs="+", help="Source columns")
    args = parser.parse_args()

    if args.test:
        test_analyzer()
    elif args.eval:
        evaluate_on_dataset()
    elif args.columns:
        analyzer = PKLevelAnalyzer()
        result = analyzer.analyze_columns(args.columns)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    elif args.output and args.source:
        analyzer = PKLevelAnalyzer()
        result = analyzer.explain_aggregation_need(args.output, args.source)
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        test_analyzer()


if __name__ == "__main__":
    main()
