"""
Parsing-based NoteTaker
Hints와 SQL을 파싱하여 누락된 항목을 탐지합니다.
iter별로 SQL, Schema Check, Refine Feedback을 관리합니다.
"""

import re
from typing import Dict, List, Set, Tuple, Any, Optional


class ParsingNoteTaker:
    """파싱 기반 NoteTaker - Hints vs SQL 비교, iter별 NOTE 관리"""

    def __init__(self, item: Dict[str, Any] = None):
        """
        Args:
            item: 데이터셋 아이템 (mapping, join_keys 포함)
        """
        self.item = item
        self.hints_parsed = self.parse_hints(item) if item else None
        self.iter_notes = []  # iter별 NOTE 저장: [{iter, sql, schema_check, refine_feedback}, ...]
        self.lookup_results = []  # lookup_val 결과 저장: [{table, column, search_term, found, similar_values}, ...]
        self.join_analysis_results = []  # join_inspector 결과 저장: [{table1, table2, cardinality, warning}, ...]

    def set_item(self, item: Dict[str, Any]):
        """데이터셋 아이템 설정"""
        self.item = item
        self.hints_parsed = self.parse_hints(item)
        self.iter_notes = []
        self.lookup_results = []
        self.join_analysis_results = []

    def add_lookup_result(self, table: str, column: str, search_term: str, found: bool, similar_values: List[str] = None):
        """
        lookup_column_values 결과 추가

        Args:
            table: 테이블명
            column: 컬럼명
            search_term: 검색한 값
            found: 값이 존재하는지 여부
            similar_values: 존재하지 않을 경우 유사한 값 목록
        """
        self.lookup_results.append({
            'table': table,
            'column': column,
            'search_term': search_term,
            'found': found,
            'similar_values': similar_values or []
        })

    def add_join_analysis_result(self, table1: str, table2: str, join_key1: str, join_key2: str,
                                  cardinality: str, join_result_count: int, warning: str = None):
        """
        inspect_join_relationship 결과 추가

        Args:
            table1: 첫 번째 테이블명
            table2: 두 번째 테이블명
            join_key1: table1의 조인 키
            join_key2: table2의 조인 키
            cardinality: 관계 유형 (1:1, 1:N, N:1, M:N)
            join_result_count: JOIN 결과 행 수
            warning: 경고 메시지 (fan-out 위험 등)
        """
        self.join_analysis_results.append({
            'table1': table1,
            'table2': table2,
            'join_key1': join_key1,
            'join_key2': join_key2,
            'cardinality': cardinality,
            'join_result_count': join_result_count,
            'warning': warning
        })

    def parse_hints(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        데이터셋 아이템에서 hints 정보 추출

        Returns:
            {
                'columns': set of TABLE.COLUMN,
                'tables': set of TABLE,
                'joins': list of (TABLE.COL, TABLE.COL)
            }
        """
        if not item:
            return {'columns': set(), 'tables': set(), 'joins': []}

        required_columns = set()
        required_tables = set()
        required_joins = []

        # mapping에서 추출
        mapping = item.get('mapping', {})
        for keyword, columns in mapping.items():
            for col in columns:
                if '.' in col:
                    required_columns.add(col.upper())
                    table = col.split('.')[0].upper()
                    required_tables.add(table)

        # join_keys에서 추출
        join_keys = item.get('join_keys', [])
        for pair in join_keys:
            if len(pair) == 2:
                col1 = pair[0].upper()
                col2 = pair[1].upper()
                required_joins.append((col1, col2))
                for col in [col1, col2]:
                    if '.' in col:
                        table = col.split('.')[0]
                        required_tables.add(table)

        return {
            'columns': required_columns,
            'tables': required_tables,
            'joins': required_joins
        }

    def parse_sql(self, sql: str) -> Dict[str, Any]:
        """
        SQL에서 사용된 테이블/컬럼/조인 추출

        Returns:
            {
                'columns': set of TABLE.COLUMN,
                'tables': set of TABLE,
                'joins': list of (TABLE.COL, TABLE.COL)
            }
        """
        if not sql:
            return {'columns': set(), 'tables': set(), 'joins': []}

        sql_upper = sql.upper()

        # SQL 키워드 목록 (별칭으로 잘못 인식되지 않도록)
        sql_keywords = {'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS',
                       'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'AND', 'OR', 'AS'}

        # 테이블 추출 (FROM, JOIN 뒤 첫 번째 단어)
        table_pattern = r'(?:FROM|(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN)\s+(\w+)'
        table_names = re.findall(table_pattern, sql_upper)

        # 별칭 추출 (테이블명 AS 별칭 또는 테이블명 별칭)
        alias_pattern = r'(?:FROM|(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN)\s+(\w+)\s+(?:AS\s+)?(\w+)'
        alias_matches = re.findall(alias_pattern, sql_upper)

        tables = {}  # 별칭 -> 실제 테이블
        # 먼저 모든 테이블을 자기 자신을 별칭으로 등록
        for table in table_names:
            tables[table] = table
        # 그 다음 별칭 매핑 추가
        for match in alias_matches:
            table = match[0]
            alias = match[1]
            if alias not in sql_keywords:
                tables[alias] = table

        # SELECT 절 컬럼 추출
        select_columns = set()
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.DOTALL)
        if select_match:
            select_part = select_match.group(1)
            col_pattern = r'(\w+)\.(\w+)'
            for alias, col in re.findall(col_pattern, select_part):
                real_table = tables.get(alias, alias)
                select_columns.add(f'{real_table}.{col}')

        # WHERE 절 컬럼 추출
        where_columns = set()
        where_match = re.search(r'WHERE\s+(.*?)(?:ORDER|GROUP|LIMIT|HAVING|$)', sql_upper, re.DOTALL)
        if where_match:
            where_part = where_match.group(1)
            col_pattern = r'(\w+)\.(\w+)'
            for alias, col in re.findall(col_pattern, where_part):
                real_table = tables.get(alias, alias)
                where_columns.add(f'{real_table}.{col}')

        # GROUP BY 절 컬럼 추출
        group_columns = set()
        group_match = re.search(r'GROUP\s+BY\s+(.*?)(?:ORDER|HAVING|LIMIT|$)', sql_upper, re.DOTALL)
        if group_match:
            group_part = group_match.group(1)
            col_pattern = r'(\w+)\.(\w+)'
            for alias, col in re.findall(col_pattern, group_part):
                real_table = tables.get(alias, alias)
                group_columns.add(f'{real_table}.{col}')

        # JOIN 조건 추출
        join_pattern = r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
        join_matches = re.findall(join_pattern, sql_upper)
        joins = []
        join_columns = set()  # JOIN 조건에서 사용된 컬럼들
        for m in join_matches:
            alias1, col1, alias2, col2 = m
            table1 = tables.get(alias1, alias1)
            table2 = tables.get(alias2, alias2)
            joins.append((f'{table1}.{col1}', f'{table2}.{col2}'))
            # JOIN에서 사용된 컬럼도 추가
            join_columns.add(f'{table1}.{col1}')
            join_columns.add(f'{table2}.{col2}')

        return {
            'tables': set(tables.values()),
            'columns': select_columns | where_columns | group_columns | join_columns,
            'joins': joins
        }

    def compare(self, hints_parsed: Dict[str, Any], sql_parsed: Dict[str, Any]) -> Dict[str, Any]:
        """
        Hints와 SQL 파싱 결과 비교

        Returns:
            {
                'missing_tables': set,
                'missing_columns': set,
                'missing_joins': set of tuples
            }
        """
        # 테이블 비교
        missing_tables = hints_parsed['tables'] - sql_parsed['tables']

        # 컬럼 비교
        missing_columns = hints_parsed['columns'] - sql_parsed['columns']

        # JOIN 비교 (순서 무시)
        hints_joins_set = set()
        for a, b in hints_parsed['joins']:
            hints_joins_set.add(tuple(sorted([a, b])))

        sql_joins_set = set()
        for a, b in sql_parsed['joins']:
            sql_joins_set.add(tuple(sorted([a, b])))

        missing_joins = hints_joins_set - sql_joins_set

        return {
            'missing_tables': missing_tables,
            'missing_columns': missing_columns,
            'missing_joins': missing_joins
        }

    def generate_schema_check(self, sql: str) -> str:
        """
        Schema Check 결과 생성

        Returns:
            Schema check 문자열 (☑/☐ 형식)
        """
        if not self.hints_parsed or not sql:
            return "Schema: (no hints)"

        sql_parsed = self.parse_sql(sql)
        comparison = self.compare(self.hints_parsed, sql_parsed)

        lines = []

        # 누락된 항목
        if comparison['missing_columns']:
            for col in sorted(comparison['missing_columns']):
                lines.append(f"  ☐ {col} - 누락, 확인 필요")

        if comparison['missing_joins']:
            for a, b in sorted(comparison['missing_joins']):
                lines.append(f"  ☐ JOIN {a} = {b} - 누락, 확인 필요")

        if comparison['missing_tables']:
            for table in sorted(comparison['missing_tables']):
                lines.append(f"  ☐ {table} - 테이블 누락, 확인 필요")

        # 사용된 항목 (일부만 표시)
        used_columns = self.hints_parsed['columns'] - comparison['missing_columns']
        for col in sorted(list(used_columns)[:5]):  # 최대 5개만 표시
            lines.append(f"  ☑ {col} - 사용됨")
        if len(used_columns) > 5:
            lines.append(f"  ... 외 {len(used_columns) - 5}개 사용됨")

        if not lines:
            return "Schema: ☑ 모든 hints 항목 사용됨"

        return "Schema:\n" + "\n".join(lines)

    def generate_refine_feedback(self, exec_result: Dict[str, Any]) -> str:
        """
        Refine Feedback 생성

        Args:
            exec_result: SQL 실행 결과 {success, error_type, error_message, row_count, ...}

        Returns:
            Refine feedback 문자열
        """
        if not exec_result:
            return "Refine: (not executed)"

        success = exec_result.get('success', False)
        error_type = exec_result.get('error_type', '')
        error_message = exec_result.get('error_message', '')
        row_count = exec_result.get('row_count', 0)

        if success and row_count > 0:
            return f"Refine: ✅ 실행 성공 ({row_count}행)"
        elif success and row_count == 0:
            return f"Refine: ⚠️ empty result (0행)"
        else:
            # 에러 메시지 줄이기
            short_error = error_message[:100] + "..." if len(error_message) > 100 else error_message
            return f"Refine: ❌ {error_type} - {short_error}"

    def add_iter_note(self, iter_num: int, sql: str, exec_result: Dict[str, Any] = None,
                      llm_feedback: str = None, question: str = None, use_rule_review: bool = False):
        """
        iter별 NOTE 추가

        Args:
            iter_num: iteration 번호
            sql: 생성된 SQL
            exec_result: SQL 실행 결과 (optional, --refine 시)
            llm_feedback: LLM 비판적 검토 결과 (optional, --llm_feedback 시)
            question: 원본 질문 (rule-based review용)
            use_rule_review: rule-based review 사용 여부
        """
        schema_check = self.generate_schema_check(sql)
        refine_feedback = self.generate_refine_feedback(exec_result) if exec_result else None
        rule_review = self.generate_rule_based_review(sql, question) if use_rule_review and question else None

        self.iter_notes.append({
            'iter': iter_num,
            'sql': sql,
            'schema_check': schema_check,
            'refine_feedback': refine_feedback,
            'llm_feedback': llm_feedback,
            'rule_review': rule_review
        })

    def update_llm_feedback(self, llm_feedback: str):
        """
        마지막 iter의 LLM feedback 업데이트

        Args:
            llm_feedback: LLM 비판적 검토 결과
        """
        if self.iter_notes:
            self.iter_notes[-1]['llm_feedback'] = llm_feedback

    def get_current_note(self) -> str:
        """
        현재까지의 NOTE를 문자열로 반환 (LLM에 전달용)

        Returns:
            NOTE 문자열
        """
        if not self.iter_notes and not self.lookup_results and not self.join_analysis_results:
            return ""

        lines = ["=== NOTE ==="]

        # Lookup 결과 추가
        if self.lookup_results:
            lines.append("\n[Value Lookup Results]")
            for lr in self.lookup_results:
                if lr['found']:
                    lines.append(f"  ✓ {lr['table']}.{lr['column']} = '{lr['search_term']}' - 존재함")
                else:
                    similar = ', '.join(lr['similar_values'][:3]) if lr['similar_values'] else '없음'
                    lines.append(f"  ✗ {lr['table']}.{lr['column']} = '{lr['search_term']}' - 존재안함 (유사값: {similar})")

        # JOIN 분석 결과 추가
        if self.join_analysis_results:
            lines.append("\n[JOIN Analysis Results]")
            for jr in self.join_analysis_results:
                cardinality = jr['cardinality']
                join_desc = f"{jr['table1']}.{jr['join_key1']} = {jr['table2']}.{jr['join_key2']}"
                if cardinality == "M:N":
                    lines.append(f"  ⚠️ {join_desc} → {cardinality} (fan-out 위험!)")
                elif cardinality in ["1:N", "N:1"]:
                    lines.append(f"  ℹ️ {join_desc} → {cardinality} ({jr['join_result_count']}행)")
                else:
                    lines.append(f"  ✓ {join_desc} → {cardinality} ({jr['join_result_count']}행)")
                if jr.get('warning'):
                    lines.append(f"     {jr['warning']}")

        # iter별 NOTE
        for note in self.iter_notes:
            lines.append(f"\n[iter {note['iter']}]")
            # SQL은 너무 길 수 있으므로 첫 줄만
            sql_first_line = note['sql'].split('\n')[0] if note['sql'] else "(no SQL)"
            lines.append(f"SQL: {sql_first_line}...")
            lines.append(note['schema_check'])
            if note['refine_feedback']:
                lines.append(note['refine_feedback'])
            if note.get('rule_review'):
                lines.append(note['rule_review'])
            if note.get('llm_feedback'):
                lines.append(f"LLM Review: {note['llm_feedback']}")

        return "\n".join(lines)

    def get_final_note(self) -> str:
        """
        최종 NOTE 반환 (로그 기록용, 더 상세함)

        Returns:
            최종 NOTE 문자열
        """
        if not self.iter_notes and not self.lookup_results and not self.join_analysis_results:
            return ""

        lines = ["=== FINAL NOTE ==="]

        # Lookup 결과 추가
        if self.lookup_results:
            lines.append("\n[Value Lookup Results]")
            for lr in self.lookup_results:
                if lr['found']:
                    lines.append(f"  ✓ {lr['table']}.{lr['column']} = '{lr['search_term']}' - 존재함")
                else:
                    similar = ', '.join(lr['similar_values'][:5]) if lr['similar_values'] else '없음'
                    lines.append(f"  ✗ {lr['table']}.{lr['column']} = '{lr['search_term']}' - 존재안함")
                    lines.append(f"    유사값: {similar}")

        # JOIN 분석 결과 추가
        if self.join_analysis_results:
            lines.append("\n[JOIN Analysis Results]")
            for jr in self.join_analysis_results:
                cardinality = jr['cardinality']
                join_desc = f"{jr['table1']}.{jr['join_key1']} = {jr['table2']}.{jr['join_key2']}"
                lines.append(f"  {join_desc} → {cardinality} ({jr['join_result_count']}행)")
                if jr.get('warning'):
                    lines.append(f"    ⚠️ {jr['warning']}")

        # iter별 NOTE
        for note in self.iter_notes:
            lines.append(f"\n[iter {note['iter']}]")
            lines.append(f"SQL:\n{note['sql']}")
            lines.append(note['schema_check'])
            if note['refine_feedback']:
                lines.append(note['refine_feedback'])
            if note.get('rule_review'):
                lines.append(note['rule_review'])
            if note.get('llm_feedback'):
                lines.append(f"LLM Review:\n{note['llm_feedback']}")

        return "\n".join(lines)

    def generate_rule_based_review(self, sql: str, question: str) -> str:
        """
        Rule-based review 수행
        - Aggregation check: 질문이 집계를 요구하면 COUNT/SUM 등이 있어야 함
        - DISTINCT check: 불필요한 DISTINCT 사용 확인

        Args:
            sql: 생성된 SQL
            question: 원본 질문

        Returns:
            Rule-based review 결과 문자열
        """
        if not sql or not question:
            return ""

        sql_upper = sql.upper()
        question_lower = question.lower()
        issues = []

        # 1. Aggregation Check
        # 질문에 집계 관련 키워드가 있으면 SQL에도 집계 함수가 있어야 함
        agg_keywords_kr = ['몇 개', '몇개', '몇 명', '몇명', '총', '합계', '평균', '최대', '최소', '개수']
        agg_keywords_en = ['how many', 'count', 'total', 'sum', 'average', 'avg', 'maximum', 'minimum', 'number of']

        has_agg_keyword = any(kw in question_lower for kw in agg_keywords_kr + agg_keywords_en)
        has_agg_function = any(fn in sql_upper for fn in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN('])

        if has_agg_keyword and not has_agg_function:
            # 어떤 키워드가 매칭됐는지 찾기
            matched_kw = [kw for kw in agg_keywords_kr + agg_keywords_en if kw in question_lower]
            issues.append(f"☐ Aggregation: 질문에 '{matched_kw[0]}' 키워드가 있으나 집계함수(COUNT/SUM/AVG 등) 없음")
        elif has_agg_keyword and has_agg_function:
            issues.append("☑ Aggregation: 집계 키워드와 집계함수 일치")

        # 2. DISTINCT Check
        has_distinct = 'DISTINCT' in sql_upper
        has_join = 'JOIN' in sql_upper
        has_group_by = 'GROUP BY' in sql_upper

        if has_distinct:
            if not has_join and not has_group_by:
                # JOIN도 없고 GROUP BY도 없는데 DISTINCT 사용 → 불필요할 가능성
                issues.append("☐ DISTINCT: JOIN/GROUP BY 없이 DISTINCT 사용됨 (불필요할 수 있음)")
            else:
                issues.append("☑ DISTINCT: JOIN/GROUP BY와 함께 사용됨")

        # 3. GROUP BY without aggregation check
        if has_group_by and not has_agg_function:
            issues.append("☐ GROUP BY: GROUP BY는 있으나 집계함수 없음 (의도 확인 필요)")

        if not issues:
            return ""

        return "Rule-Based Review:\n" + "\n".join(f"  {issue}" for issue in issues)

    def has_rule_issues(self, sql: str, question: str) -> bool:
        """
        Rule-based review에서 문제가 있는지 확인

        Returns:
            문제가 있으면 True
        """
        review = self.generate_rule_based_review(sql, question)
        return '☐' in review

    def has_issues(self) -> bool:
        """
        현재 NOTE에 문제(누락, 에러)가 있는지 확인

        Returns:
            문제가 있으면 True
        """
        if not self.iter_notes:
            return False

        last_note = self.iter_notes[-1]

        # Schema에 누락이 있는지
        if '☐' in last_note['schema_check']:
            return True

        # Refine에 에러가 있는지
        if last_note['refine_feedback']:
            if '❌' in last_note['refine_feedback'] or '⚠️' in last_note['refine_feedback']:
                return True

        # Note: rule_review는 정보 제공용으로만 사용하고 refine 트리거에서 제외
        # (false positive가 많아 성능 저하 유발)

        return False

    def get_issues_summary(self) -> Optional[str]:
        """
        현재 문제점 요약 (LLM에게 수정 요청할 때 사용)

        Returns:
            문제점 요약 문자열 또는 None
        """
        if not self.iter_notes:
            return None

        last_note = self.iter_notes[-1]
        issues = []

        # Schema 누락
        if '☐' in last_note['schema_check']:
            # 누락된 항목만 추출
            for line in last_note['schema_check'].split('\n'):
                if '☐' in line:
                    issues.append(line.strip())

        # Refine 에러
        if last_note['refine_feedback']:
            if '❌' in last_note['refine_feedback'] or '⚠️' in last_note['refine_feedback']:
                issues.append(last_note['refine_feedback'])

        if issues:
            return "\n".join(issues)
        return None

    # 기존 호환성을 위한 메서드들
    def generate_note(self, item: Dict[str, Any], sql: str) -> Optional[str]:
        """기존 호환성 메서드"""
        if not self.hints_parsed:
            self.set_item(item)
        sql_parsed = self.parse_sql(sql)
        comparison = self.compare(self.hints_parsed, sql_parsed)

        notes = []

        if comparison['missing_columns']:
            cols = ', '.join(sorted(comparison['missing_columns']))
            notes.append(f"[Column Check] 이 컬럼이 누락된건 아닌지 다시 확인해주세요: {cols}")

        if comparison['missing_joins']:
            joins = ', '.join([f"{a} = {b}" for a, b in sorted(comparison['missing_joins'])])
            notes.append(f"[Join Check] 이 JOIN이 누락된건 아닌지 다시 확인해주세요: {joins}")

        if comparison['missing_tables']:
            tables = ', '.join(sorted(comparison['missing_tables']))
            notes.append(f"[Table Check] 이 테이블이 누락된건 아닌지 다시 확인해주세요: {tables}")

        if notes:
            return "\n".join(notes)
        return None

    def check_and_generate_note(self, item: Dict[str, Any], sql: str) -> Tuple[bool, Optional[str]]:
        """기존 호환성 메서드"""
        note = self.generate_note(item, sql)
        return (note is not None, note)
