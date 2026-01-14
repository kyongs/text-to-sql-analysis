# src/agent/constraint_checker.py
"""
테이블의 PK/FK 제약조건 정보를 제공하는 도구
+ 스키마 존재성 검증, 타입 제약, 값 도메인 분석
"""

import json
import os
import mysql.connector
from typing import Dict, Any, List


def check_schema_constraints(
    tables: List[str],
    columns: List[str],
    conn_info: Dict[str, Any],
    db_id: str = "dw"
) -> Dict[str, Any]:
    """
    스키마 제약조건을 종합적으로 분석합니다.

    Args:
        tables: 확인할 테이블 리스트
        columns: 확인할 컬럼 리스트 (TABLE.COLUMN 형식)
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID

    Returns:
        {
            "schema_validation": {...},
            "key_constraints": {...},
            "type_info": {...},
            "value_domains": {...},
            "join_suggestions": [...],
            "warnings": [...]
        }
    """
    result = {
        "tables_requested": tables,
        "columns_requested": columns,
        "schema_validation": {},
        "key_constraints": {},
        "type_info": {},
        "value_domains": {},
        "join_suggestions": [],
        "warnings": []
    }

    conn = None
    try:
        conn = mysql.connector.connect(
            host=conn_info.get('host', '127.0.0.1'),
            port=conn_info.get('port', 3306),
            user=conn_info.get('user', 'root'),
            password=conn_info.get('password', ''),
            database=db_id
        )
        cursor = conn.cursor(dictionary=True)

        # 1. 테이블 존재 여부 확인
        for table in tables:
            cursor.execute("""
                SELECT COUNT(*) as cnt
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (db_id, table))
            exists = cursor.fetchone()['cnt'] > 0
            result["schema_validation"][table] = {"exists": exists, "type": "table"}
            if not exists:
                result["warnings"].append(f"Table '{table}' does NOT exist")

        # 2. 컬럼 존재 여부 및 타입 정보
        for col_spec in columns:
            if "." in col_spec:
                table_name, col_name = col_spec.split(".", 1)
            else:
                table_name = None
                col_name = col_spec

            if table_name:
                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
                           COLUMN_DEFAULT, COLUMN_KEY
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                """, (db_id, table_name, col_name))
            else:
                cursor.execute("""
                    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
                           COLUMN_DEFAULT, COLUMN_KEY
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND COLUMN_NAME = %s
                """, (db_id, col_name))

            col_info = cursor.fetchone()

            if col_info:
                result["schema_validation"][col_spec] = {"exists": True, "type": "column"}
                result["type_info"][col_spec] = {
                    "data_type": col_info.get("DATA_TYPE"),
                    "column_type": col_info.get("COLUMN_TYPE"),
                    "nullable": col_info.get("IS_NULLABLE") == "YES",
                    "default": col_info.get("COLUMN_DEFAULT"),
                    "key": col_info.get("COLUMN_KEY")
                }

                dtype = col_info.get("DATA_TYPE", "").lower()
                if dtype in ["date", "datetime", "timestamp"]:
                    result["warnings"].append(
                        f"'{col_spec}' is {dtype.upper()} - use proper date format"
                    )
            else:
                result["schema_validation"][col_spec] = {"exists": False, "type": "column"}
                result["warnings"].append(f"Column '{col_spec}' does NOT exist")

        # 3. PK/FK 제약조건 분석
        for table in tables:
            if not result["schema_validation"].get(table, {}).get("exists"):
                continue

            # Primary Key
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
            """, (db_id, table))
            pk_cols = [row['COLUMN_NAME'] for row in cursor.fetchall()]

            # Foreign Keys
            cursor.execute("""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (db_id, table))
            fks = []
            for row in cursor.fetchall():
                fks.append({
                    "column": row['COLUMN_NAME'],
                    "references_table": row['REFERENCED_TABLE_NAME'],
                    "references_column": row['REFERENCED_COLUMN_NAME']
                })
                result["join_suggestions"].append({
                    "from": f"{table}.{row['COLUMN_NAME']}",
                    "to": f"{row['REFERENCED_TABLE_NAME']}.{row['REFERENCED_COLUMN_NAME']}"
                })

            result["key_constraints"][table] = {
                "primary_key": pk_cols,
                "foreign_keys": fks
            }

        # 4. 값 도메인 분석 (string 컬럼)
        for col_spec in columns:
            if "." not in col_spec:
                continue

            table_name, col_name = col_spec.split(".", 1)
            type_info = result["type_info"].get(col_spec, {})
            dtype = type_info.get("data_type", "").lower()

            if dtype in ["varchar", "char", "enum", "text"]:
                try:
                    cursor.execute(f"""
                        SELECT `{col_name}` as val, COUNT(*) as cnt
                        FROM `{table_name}`
                        WHERE `{col_name}` IS NOT NULL
                        GROUP BY `{col_name}`
                        ORDER BY cnt DESC
                        LIMIT 15
                    """)
                    values = cursor.fetchall()

                    if len(values) <= 15:
                        result["value_domains"][col_spec] = {
                            "type": "enum_like",
                            "values": [v['val'] for v in values]
                        }
                    else:
                        result["value_domains"][col_spec] = {
                            "type": "high_cardinality",
                            "sample": [v['val'] for v in values[:10]]
                        }
                except Exception:
                    pass

        return result

    except Exception as e:
        result["error"] = str(e)[:300]
        return result

    finally:
        if conn:
            conn.close()


def format_constraint_check(result: Dict[str, Any]) -> str:
    """분석 결과를 LLM이 이해하기 쉬운 형태로 포맷팅"""

    lines = []

    if result.get("error"):
        return f"[CONSTRAINT CHECK] Error: {result['error']}"

    lines.append("[CONSTRAINT CHECK RESULTS]")
    lines.append("")

    # 스키마 검증
    validation = result.get("schema_validation", {})
    invalid = [k for k, v in validation.items() if not v.get("exists")]
    if invalid:
        lines.append("SCHEMA ERRORS:")
        for item in invalid:
            lines.append(f"  - '{item}' does NOT exist")
        lines.append("")
    else:
        lines.append("SCHEMA: All tables/columns exist")
        lines.append("")

    # 키 제약조건
    key_info = result.get("key_constraints", {})
    if key_info:
        lines.append("KEY CONSTRAINTS:")
        for table, constraints in key_info.items():
            pk = constraints.get("primary_key", [])
            if pk:
                lines.append(f"  {table} PK: {', '.join(pk)}")
            for fk in constraints.get("foreign_keys", []):
                lines.append(f"  {table}.{fk['column']} -> {fk['references_table']}.{fk['references_column']}")
        lines.append("")

    # 값 도메인
    domains = result.get("value_domains", {})
    for col, domain in domains.items():
        if domain.get("type") == "enum_like":
            lines.append(f"VALUE DOMAIN '{col}': {domain['values']}")

    # JOIN 제안
    suggestions = result.get("join_suggestions", [])
    if suggestions:
        lines.append("")
        lines.append("SUGGESTED JOINS:")
        for s in suggestions[:5]:
            lines.append(f"  {s['from']} = {s['to']}")

    # 경고
    warnings = result.get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("WARNINGS:")
        for w in warnings[:5]:
            lines.append(f"  - {w}")

    return "\n".join(lines)


def get_table_constraints(table_name: str, metadata_path: str = None) -> str:
    """
    특정 테이블의 Primary Key와 Foreign Key 정보를 반환합니다.
    
    Args:
        table_name: 조회할 테이블 이름
        metadata_path: pk_candidates_enhanced.json 파일 경로
        
    Returns:
        테이블의 제약조건 정보 (자연어 형식)
    """
    if metadata_path is None:
        metadata_path = os.path.join(
            os.path.dirname(__file__), 
            '../../data/beaver/dw/pk_candidates_enhanced.json'
        )
    
    # 메타데이터 로드
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            pk_metadata = json.load(f)
    except FileNotFoundError:
        return f"❌ Metadata file not found: {metadata_path}"
    
    # 대소문자 통일
    table_name_upper = table_name.upper()
    
    if table_name_upper not in pk_metadata:
        return f"❌ Table '{table_name}' not found in metadata"
    
    info = pk_metadata[table_name_upper]
    
    # PK가 없는 경우
    if not info.get('pk_candidates'):
        note = info.get('note', 'No primary key information available')
        return f"""
ℹ️  Table: {table_name}

{note}

💡 Note: This table may not be used in JOIN operations, or may require a composite key.
"""
    
    # PK 정보 포맷팅
    response = f"""
📋 Table Constraints: {table_name}

PRIMARY KEY Information:
"""
    
    for idx, pk in enumerate(info['pk_candidates'], 1):
        is_recommended = (idx == 1)
        marker = "⭐ RECOMMENDED" if is_recommended else "   Alternative"
        
        response += f"""
{marker} Primary Key #{idx}: {pk['column']}
  - Data Type: {pk['column_type']}
  - Uniqueness: {pk['uniqueness_percent']}%
  - Sample Values: {', '.join(pk['sample_values'][:3])}
"""
    
    # FK 정보 추가 (dw_join_keys.json에서)
    join_keys_path = os.path.join(
        os.path.dirname(__file__),
        '../../data/beaver/dw/dw_join_keys.json'
    )
    
    try:
        with open(join_keys_path, 'r', encoding='utf-8') as f:
            join_keys = json.load(f)
        
        # 이 테이블의 FK 찾기
        foreign_keys = []
        for pair in join_keys:
            table1, col1 = pair[0].split('.')
            table2, col2 = pair[1].split('.')
            
            if table1.upper() == table_name_upper:
                foreign_keys.append({
                    'column': col1,
                    'references_table': table2,
                    'references_column': col2
                })
            elif table2.upper() == table_name_upper:
                foreign_keys.append({
                    'column': col2,
                    'references_table': table1,
                    'references_column': col1
                })
        
        if foreign_keys:
            response += "\nFOREIGN KEY Relationships:\n"
            
            # 중복 제거
            seen = set()
            for fk in foreign_keys:
                key = (fk['column'], fk['references_table'], fk['references_column'])
                if key not in seen:
                    seen.add(key)
                    response += f"  - {fk['column']} → {fk['references_table']}.{fk['references_column']}\n"
    except:
        pass  # FK 정보 없어도 계속 진행
    
    response += """
💡 Usage Tips:
  - Use the RECOMMENDED primary key for unique identification
  - Use foreign keys to JOIN with related tables
  - Primary keys are guaranteed to be UNIQUE and NOT NULL
"""
    
    return response


# 테스트용
if __name__ == "__main__":
    # 샘플 테스트
    print("=" * 80)
    print("Testing get_table_constraints")
    print("=" * 80)
    
    test_tables = ['BUILDINGS', 'FCLT_ROOMS', 'MASTER_DEPT_HIERARCHY']
    
    for table in test_tables:
        print(f"\n{'='*80}")
        print(f"Table: {table}")
        print('='*80)
        result = get_table_constraints(table)
        print(result)
