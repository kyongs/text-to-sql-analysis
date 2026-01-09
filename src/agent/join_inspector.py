# src/agent/join_inspector.py
"""
LLM이 SQL 생성 시 사용할 수 있는 JOIN 검사 도구
- 두 테이블을 JOIN하면 어떤 결과가 나오는지 미리 확인
- Cardinality(1:1, 1:N, N:1, M:N) 판별
- 샘플 데이터 확인
"""

import mysql.connector
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, asdict
import json


@dataclass
class JoinInspectionResult:
    """JOIN 검사 결과"""
    table1_name: str
    table2_name: str
    join_key1: str
    join_key2: str
    cardinality: str  # "1:1", "1:N", "N:1", "M:N"
    table1_row_count: int
    table2_row_count: int
    join_result_count: int
    sample_join_results: List[Dict[str, Any]]
    warning_message: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)
    
    def to_natural_language(self) -> str:
        """LLM이 읽기 쉬운 자연어 설명 생성"""
        desc = f"""
JOIN Information for {self.table1_name} and {self.table2_name}:

Join Condition: {self.table1_name}.{self.join_key1} = {self.table2_name}.{self.join_key2}

Cardinality: {self.cardinality}
- {self.table1_name} has {self.table1_row_count:,} rows
- {self.table2_name} has {self.table2_row_count:,} rows
- JOIN produces {self.join_result_count:,} rows

Interpretation:
"""
        if self.cardinality == "1:1":
            desc += f"- This is a ONE-to-ONE relationship\n"
            desc += f"- Each row in {self.table1_name} matches exactly one row in {self.table2_name}\n"
        elif self.cardinality == "1:N":
            desc += f"- This is a ONE-to-MANY relationship\n"
            desc += f"- Each row in {self.table1_name} can match multiple rows in {self.table2_name}\n"
            desc += f"- The result will have more rows than {self.table1_name}\n"
        elif self.cardinality == "N:1":
            desc += f"- This is a MANY-to-ONE relationship\n"
            desc += f"- Multiple rows in {self.table1_name} can match one row in {self.table2_name}\n"
            desc += f"- The result will have the same or fewer rows than {self.table1_name}\n"
        elif self.cardinality == "M:N":
            desc += f"- This is a MANY-to-MANY relationship\n"
            desc += f"- Both tables have duplicate keys, which can cause data multiplication\n"
            desc += f"- ⚠️ BE CAREFUL: This JOIN might produce more rows than expected\n"
        
        if self.warning_message:
            desc += f"\n⚠️ WARNING: {self.warning_message}\n"
        
        desc += f"\nSample JOIN results (first 3 rows):\n"
        for i, row in enumerate(self.sample_join_results[:3], 1):
            desc += f"  {i}. {row}\n"
        
        return desc


class JoinInspector:
    """LLM이 SQL 생성 시 JOIN 관계를 검사할 수 있는 도구"""
    
    def __init__(self, conn_info: Dict[str, Any], db_id: str):
        self.conn_info = conn_info
        self.db_id = db_id
    
    def _get_connection(self):
        return mysql.connector.connect(
            host=self.conn_info.get('host', '127.0.0.1'),
            port=self.conn_info.get('port', 3306),
            user=self.conn_info.get('user', 'root'),
            password=self.conn_info.get('password', ''),
            database=self.db_id
        )
    
    def inspect_join(self, table1: str, table2: str, 
                    join_key1: str, join_key2: str,
                    limit: int = 3) -> JoinInspectionResult:
        """
        두 테이블의 JOIN 관계를 검사합니다.
        
        Args:
            table1: 첫 번째 테이블명
            table2: 두 번째 테이블명
            join_key1: table1의 조인 키
            join_key2: table2의 조인 키
            limit: 샘플 결과 개수
            
        Returns:
            JoinInspectionResult
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            # 1. 각 테이블의 row count
            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table1}`")
            table1_count = cursor.fetchone()['cnt']

            cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table2}`")
            table2_count = cursor.fetchone()['cnt']

            # 2. 각 테이블의 distinct key count
            column_error_detected = False
            error_message = ""

            try:
                cursor.execute(f"SELECT COUNT(DISTINCT `{join_key1}`) as cnt FROM `{table1}`")
                distinct_key1 = cursor.fetchone()['cnt']
            except Exception as e:
                column_error_detected = True
                error_message = f"Column '{join_key1}' does not exist in table '{table1}'"

            if not column_error_detected:
                try:
                    cursor.execute(f"SELECT COUNT(DISTINCT `{join_key2}`) as cnt FROM `{table2}`")
                    distinct_key2 = cursor.fetchone()['cnt']
                except Exception as e:
                    column_error_detected = True
                    error_message = f"Column '{join_key2}' does not exist in table '{table2}'"

            # If column error detected, try to find correct JOIN path automatically
            if column_error_detected:
                try:
                    from src.agent.join_path_finder import find_join_path

                    # Automatically call join_path_finder to find correct JOIN keys
                    correct_path = find_join_path(
                        table1=table1,
                        table2=table2,
                        conn_info=self.conn_info,
                        db_id=self.db_id
                    )

                    return JoinInspectionResult(
                        table1_name=table1,
                        table2_name=table2,
                        join_key1=join_key1,
                        join_key2=join_key2,
                        cardinality="UNKNOWN",
                        table1_row_count=table1_count,
                        table2_row_count=table2_count,
                        join_result_count=0,
                        sample_join_results=[],
                        warning_message=f"""❌ {error_message}

🔍 Auto-discovered correct JOIN path:

{correct_path}

💡 Please use the JOIN keys shown above instead."""
                    )
                except Exception as path_error:
                    # Fallback if join_path_finder also fails
                    return JoinInspectionResult(
                        table1_name=table1,
                        table2_name=table2,
                        join_key1=join_key1,
                        join_key2=join_key2,
                        cardinality="UNKNOWN",
                        table1_row_count=table1_count,
                        table2_row_count=table2_count,
                        join_result_count=0,
                        sample_join_results=[],
                        warning_message=f"❌ {error_message}\n\n⚠️ Could not auto-discover correct JOIN path: {str(path_error)[:200]}"
                    )
            
            # 3. JOIN 결과
            try:
                join_sql = f"""
                    SELECT t1.*, t2.*
                    FROM `{table1}` t1
                    JOIN `{table2}` t2 ON t1.`{join_key1}` = t2.`{join_key2}`
                    LIMIT {limit}
                """
                cursor.execute(join_sql)
                sample_results = cursor.fetchall()

                # 4. JOIN 결과 count
                count_sql = f"""
                    SELECT COUNT(*) as cnt
                    FROM `{table1}` t1
                    JOIN `{table2}` t2 ON t1.`{join_key1}` = t2.`{join_key2}`
                """
                cursor.execute(count_sql)
                join_count = cursor.fetchone()['cnt']
            except Exception as e:
                # JOIN failed - likely column mismatch or other SQL error
                return JoinInspectionResult(
                    table1_name=table1,
                    table2_name=table2,
                    join_key1=join_key1,
                    join_key2=join_key2,
                    cardinality="UNKNOWN",
                    table1_row_count=table1_count,
                    table2_row_count=table2_count,
                    join_result_count=0,
                    sample_join_results=[],
                    warning_message=f"❌ JOIN query failed. The specified columns may not exist in the database. Error: {str(e)[:200]}"
                )
            
            # 5. Cardinality 결정
            is_table1_unique = (distinct_key1 == table1_count)
            is_table2_unique = (distinct_key2 == table2_count)
            
            if is_table1_unique and is_table2_unique:
                cardinality = "1:1"
            elif is_table1_unique and not is_table2_unique:
                cardinality = "1:N"
            elif not is_table1_unique and is_table2_unique:
                cardinality = "N:1"
            else:
                cardinality = "M:N"
            
            # 6. 경고 메시지 생성
            warning = None
            if cardinality == "M:N" and join_count > max(table1_count, table2_count) * 1.5:
                warning = f"JOIN produces {join_count:,} rows from {table1_count:,} and {table2_count:,} rows. Data multiplication detected!"
            elif join_count == 0:
                warning = "No matching rows found! Check if the join keys have common values."
            
            return JoinInspectionResult(
                table1_name=table1,
                table2_name=table2,
                join_key1=join_key1,
                join_key2=join_key2,
                cardinality=cardinality,
                table1_row_count=table1_count,
                table2_row_count=table2_count,
                join_result_count=join_count,
                sample_join_results=sample_results,
                warning_message=warning
            )

        except Exception as e:
            # Catch-all for any unexpected errors (table doesn't exist, connection issues, etc.)
            return JoinInspectionResult(
                table1_name=table1,
                table2_name=table2,
                join_key1=join_key1,
                join_key2=join_key2,
                cardinality="UNKNOWN",
                table1_row_count=0,
                table2_row_count=0,
                join_result_count=0,
                sample_join_results=[],
                warning_message=f"❌ Failed to inspect JOIN relationship. Error: {str(e)[:300]}"
            )

        finally:
            if conn:
                conn.close()
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """테이블의 모든 컬럼 리스트 반환"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (self.db_id, table_name))
            
            return [row[0] for row in cursor.fetchall()]
            
        finally:
            if conn:
                conn.close()
    
    def find_join_keys(self, table1: str, table2: str) -> List[Tuple[str, str]]:
        """
        두 테이블 간 가능한 조인 키 찾기 (컬럼명이 같은 것들)
        
        Returns:
            List of (col1, col2) tuples
        """
        cols1 = set(self.get_table_columns(table1))
        cols2 = set(self.get_table_columns(table2))
        
        common_cols = cols1.intersection(cols2)
        return [(col, col) for col in common_cols]
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """테이블의 샘플 데이터 반환"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit}")
            return cursor.fetchall()
            
        finally:
            if conn:
                conn.close()


# LLM이 사용할 Tool Function들
def inspect_join_relationship(
    table1: str,
    table2: str, 
    join_key1: str,
    join_key2: str,
    conn_info: Dict[str, Any],
    db_id: str
) -> str:
    """
    LLM Tool: 두 테이블을 JOIN했을 때의 관계를 조사합니다.
    
    Args:
        table1: 첫 번째 테이블명
        table2: 두 번째 테이블명
        join_key1: table1의 조인 키 컬럼명
        join_key2: table2의 조인 키 컬럼명
        conn_info: DB 연결 정보
        db_id: 데이터베이스 이름
        
    Returns:
        JOIN 관계에 대한 자연어 설명
    """
    inspector = JoinInspector(conn_info, db_id)
    result = inspector.inspect_join(table1, table2, join_key1, join_key2)
    return result.to_natural_language()


def find_possible_joins(
    table1: str,
    table2: str,
    conn_info: Dict[str, Any],
    db_id: str
) -> str:
    """
    LLM Tool: 두 테이블 간 가능한 JOIN 키들을 찾습니다.
    
    Args:
        table1: 첫 번째 테이블명
        table2: 두 번째 테이블명
        conn_info: DB 연결 정보
        db_id: 데이터베이스 이름
        
    Returns:
        가능한 JOIN 키 리스트
    """
    inspector = JoinInspector(conn_info, db_id)
    join_keys = inspector.find_join_keys(table1, table2)
    
    if not join_keys:
        return f"No common columns found between {table1} and {table2}. Cannot auto-detect join keys."
    
    result = f"Possible join keys between {table1} and {table2}:\n"
    for key1, key2 in join_keys:
        result += f"  - {table1}.{key1} = {table2}.{key2}\n"
    
    return result


def get_table_preview(
    table_name: str,
    conn_info: Dict[str, Any],
    db_id: str,
    limit: int = 5
) -> str:
    """
    LLM Tool: 테이블의 샘플 데이터를 확인합니다.
    
    Args:
        table_name: 테이블명
        conn_info: DB 연결 정보
        db_id: 데이터베이스 이름
        limit: 샘플 개수
        
    Returns:
        테이블 샘플 데이터
    """
    inspector = JoinInspector(conn_info, db_id)
    
    columns = inspector.get_table_columns(table_name)
    samples = inspector.get_table_sample(table_name, limit)
    
    result = f"Table: {table_name}\n"
    result += f"Columns: {', '.join(columns)}\n\n"
    result += f"Sample data (first {limit} rows):\n"
    
    for i, row in enumerate(samples, 1):
        result += f"  {i}. {row}\n"
    
    return result
