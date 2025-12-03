# src/prompt_builder/schema_formatter.py
import re
import sqlite3
from typing import List, Dict, Any, Optional
import os


# (_get_schema_details, _format_foreign_keys, format_schema_basic 등 다른 함수들은 모두 변경 없음)
def _get_schema_details(db_info: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    table_names = db_info.get('table_names_original', [])
    columns = db_info.get('column_names_original', [])
    column_types = db_info.get('column_types', [])
    
    primary_keys_indices = set()
    for pk in db_info.get('primary_keys', []):
        if isinstance(pk, list):
            primary_keys_indices.update(pk)
        else:
            primary_keys_indices.add(pk)
            
    fk_references = {}
    for fk in db_info.get('foreign_keys', []):
        fk_col_idx, ref_col_idx = fk
        ref_tbl_idx, ref_col_name = columns[ref_col_idx]
        ref_tbl_name = table_names[ref_tbl_idx]
        fk_references[fk_col_idx] = f"Maps to {ref_tbl_name}({ref_col_name})"

    column_descriptions = db_info.get('column_descriptions', {})
    column_examples = db_info.get('column_examples', {})

    tables_data = {tbl_name: [] for tbl_name in table_names}
    
    for i, (tbl_idx, col_name) in enumerate(columns):
        if tbl_idx < 0: continue
        
        table_name = table_names[tbl_idx]
        col_info = {
            "name": col_name,
            "type": column_types[i].upper(),
            "is_pk": i in primary_keys_indices,
            "description": column_descriptions.get(str(i), ""),
            "examples": column_examples.get(str(i), []),
            "fk_reference": fk_references.get(i, "") 
        }
        tables_data[table_name].append(col_info)
        
    return tables_data

def _format_foreign_keys(db_info: Dict[str, Any]) -> str:
    foreign_key_texts = []
    table_names = db_info.get('table_names_original', [])
    column_names = db_info.get('column_names_original', [])
    
    for fk in db_info.get('foreign_keys', []):
        col_idx_1, col_idx_2 = fk
        tbl_idx_1, col_name_1 = column_names[col_idx_1]
        tbl_idx_2, col_name_2 = column_names[col_idx_2]
        tbl_name_1 = table_names[tbl_idx_1]
        tbl_name_2 = table_names[tbl_idx_2]
        foreign_key_texts.append(f"{tbl_name_1}.{col_name_1} = {tbl_name_2}.{col_name_2}")
    
    if not foreign_key_texts:
        return ""
    
    return '\n\n[Foreign Keys]\n' + '\n'.join(foreign_key_texts)

def format_schema_basic(db_info: Dict[str, Any]) -> str:
    tables_data = _get_schema_details(db_info)
    table_texts = []
    for tbl_name, cols in tables_data.items():
        col_list_str = ", ".join([col['name'] for col in cols])
        table_texts.append(f"{tbl_name}({col_list_str})")
    
    schema_text = '[Schema]\n' + '\n'.join(table_texts)
    schema_text += _format_foreign_keys(db_info)
    return schema_text

def format_schema_basic_plus_type(db_info: Dict[str, Any]) -> str:
    tables_data = _get_schema_details(db_info)
    table_texts = []
    for tbl_name, cols in tables_data.items():
        col_list_str = ", ".join([f"{col['name']}: {col['type']}" for col in cols])
        table_texts.append(f"{tbl_name}({col_list_str})")
        
    schema_text = '[Schema]\n' + '\n'.join(table_texts)
    schema_text += _format_foreign_keys(db_info)
    return schema_text

def format_schema_ddl(db_info: Dict[str, Any]) -> str:
    tables_data = _get_schema_details(db_info)
    table_texts = []
    
    foreign_keys_by_table: Dict[str, List[str]] = {}
    table_names = db_info.get('table_names_original', [])
    column_names = db_info.get('column_names_original', [])
    
    for fk in db_info.get('foreign_keys', []):
        col_idx_1, col_idx_2 = fk
        tbl_idx_1, col_name_1 = column_names[col_idx_1]
        tbl_idx_2, col_name_2 = column_names[col_idx_2]
        tbl_name_1 = table_names[tbl_idx_1]
        tbl_name_2 = table_names[tbl_idx_2]
        fk_constraint = f"FOREIGN KEY ({col_name_1}) REFERENCES {tbl_name_2}({col_name_2})"
        if tbl_name_1 not in foreign_keys_by_table:
            foreign_keys_by_table[tbl_name_1] = []
        foreign_keys_by_table[tbl_name_1].append(fk_constraint)

    for tbl_name, cols in tables_data.items():
        definitions, pk_cols = [], []
        for col in cols:
            definitions.append(f"  {col['name']} {col['type']}")
            if col['is_pk']: pk_cols.append(col['name'])
        if pk_cols: definitions.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")
        if tbl_name in foreign_keys_by_table:
            for fk_str in foreign_keys_by_table[tbl_name]: definitions.append(f"  {fk_str}")
        table_texts.append(f"CREATE TABLE {tbl_name} (\n" + ",\n".join(definitions) + "\n);")
        
    return '[Schema]\n' + '\n\n'.join(table_texts)

def format_schema_m_schema(db_info: Dict[str, Any], db_examples: Dict[str, Any] = None) -> str:
    db_id = db_info.get('db_id', '')
    tables_data = _get_schema_details(db_info)
    schema_parts = [f"[DB_ID] {db_id}", "# Schema"]
    for tbl_name, cols in tables_data.items():
        if not cols: continue
        schema_parts.append(f"# Table: {tbl_name}")
        col_texts = []
        for col in cols:
            parts = [f"({col['name']}:{col['type']}"]
            if col['is_pk']: parts.append(", Primary Key")
            description = col.get('description', '') 
            if description: parts.append(f", {description}")
            examples = db_examples.get((tbl_name, col['name']), []) if db_examples else []
            if examples:
                formatted_examples = [f"'{e}'" if isinstance(e, str) else str(e) for e in examples]
                parts.append(f", Examples: [{', '.join(formatted_examples)}]")
            if col.get('fk_reference'):
                if len(parts) > 1: parts.append(",")
                parts.append(f"\n {col['fk_reference']}")
            parts.append(")")
            col_texts.append("".join(parts))
        schema_parts.append("[\n" + ",\n".join(col_texts) + "\n]")
    schema_text = '\n'.join(schema_parts)
    schema_text += _format_foreign_keys(db_info)
    return schema_text

def _collect_beaver_tables(
    all_schema_info: dict,
    target_db_id: str,
    table_keys: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Gather Beaver tables for a db with optional filtering by table keys."""
    table_key_set = set(table_keys) if table_keys else None
    tables = []

    for key, table_info in all_schema_info.items():
        if not key.startswith(target_db_id + '#sep#'):
            continue
        if table_key_set and key not in table_key_set:
            continue

        table_name = table_info.get('table_name_original', '')
        columns = table_info.get('column_names_original', [])
        column_types = table_info.get('column_types', [])
        primary_keys = set(table_info.get('primary_key', []))
        foreign_keys = table_info.get('foreign_key', [])

        tables.append({
            "key": key,
            "name": table_name,
            "columns": columns,
            "types": column_types,
            "primary_keys": primary_keys,
            "foreign_keys": foreign_keys,
        })
    return tables


def _format_beaver_foreign_keys(
    tables: List[Dict[str, Any]],
    included_keys: set
) -> List[str]:
    """Format foreign key strings limited to the included tables."""
    fk_strings = set()
    for table in tables:
        tbl_name = table["name"]
        db_prefix = table["key"].split('#sep#')[0] if '#sep#' in table["key"] else ""
        for fk in table.get("foreign_keys", []):
            col_name = fk.get("column_name")
            ref_table_full = fk.get("referenced_table_name", "")
            ref_table_key = ref_table_full if '#sep#' in ref_table_full else (
                f"{db_prefix}#sep#{ref_table_full}" if db_prefix else ref_table_full
            )
            if included_keys and ref_table_key not in included_keys:
                continue
            ref_table = ref_table_full.split('#sep#')[-1] if '#sep#' in ref_table_full else ref_table_full
            ref_col = fk.get("referenced_column_name", "")
            if col_name and ref_table and ref_col:
                fk_strings.add(f"{tbl_name}.{col_name} = {ref_table}.{ref_col}")
    return sorted(fk_strings)


def format_schema_beaver(all_schema_info: dict, target_db_id: str) -> str:
    """Backward-compatible basic Beaver schema formatter."""
    return format_schema_beaver_by_style(all_schema_info, target_db_id, style="basic")

def format_schema_beaver_gold_tables(
    all_schema_info: dict, 
    target_db_id: str, 
    gold_tables: List[str],
    db_examples: Dict[str, Any] = None
) -> str:
    """
    gold_tables에 포함된 테이블만 필터링하여 m_schema 스타일로 포맷팅합니다.
    
    Args:
        all_schema_info: Beaver 전체 스키마 정보
        target_db_id: 대상 데이터베이스 ID (예: 'dw')
        gold_tables: 필터링할 테이블 리스트 (예: ["dw#sep#FCLT_BUILDING_ADDRESS", ...])
        db_examples: 컬럼별 예제 값 (Optional) - {(table_name, col_name): [values]}
    
    Returns:
        m_schema 스타일로 포맷된 스키마 문자열
    """
    # gold_tables를 set으로 변환하여 빠른 검색
    gold_tables_set = set(gold_tables)
    
    schema_parts = [f"[DB_ID] {target_db_id}", "# Schema"]
    
    # gold_tables에 포함된 테이블만 처리
    for key in sorted(gold_tables):  # 정렬하여 일관된 순서 유지
        if key not in all_schema_info:
            continue
            
        table_info = all_schema_info[key]
        table_name = table_info.get('table_name_original', '')
        
        schema_parts.append(f"# Table: {table_name}")
        
        columns = table_info.get('column_names_original', [])
        column_types = table_info.get('column_types', [])
        
        # Beaver의 foreign_key 구조:
        # [{"column_name": "...", "referenced_table_name": "...", "referenced_column_name": "..."}]
        foreign_keys = table_info.get('foreign_key', [])
        
        # Foreign Key 매핑 생성 (컬럼명 -> FK 정보)
        fk_map = {}
        for fk in foreign_keys:
            col_name = fk.get('column_name')
            ref_table_full = fk.get('referenced_table_name', '')
            ref_col = fk.get('referenced_column_name', '')
            ref_table_name = ref_table_full.split('#sep#')[-1] if '#sep#' in ref_table_full else ref_table_full
            fk_map[col_name] = f"{ref_table_name}({ref_col})"
        
        col_texts = []
        for i, col_name in enumerate(columns):
            col_type = column_types[i] if i < len(column_types) else 'TEXT'
            
            # 시작: (컬럼명:타입
            parts = [f"({col_name}:{col_type.upper()}"]
            
            # Foreign Key 관계 (Maps to ...)
            if col_name in fk_map:
                parts.append(f", Maps to {fk_map[col_name]}")
            
            # 예제 값 추가 (있으면)
            if db_examples:
                examples = db_examples.get((table_name, col_name), [])
                if examples:
                    # 문자열은 작은따옴표로, 숫자/날짜는 그대로
                    formatted_examples = []
                    for e in examples[:3]:  # 최대 3개만
                        if isinstance(e, str):
                            formatted_examples.append(f"'{e}'")
                        else:
                            formatted_examples.append(str(e))
                    parts.append(f", Examples: [{', '.join(formatted_examples)}]")
            
            parts.append(")")
            col_texts.append("".join(parts))
        
        schema_parts.append("[\n" + ",\n".join(col_texts) + "\n]")
    
    schema_text = '\n'.join(schema_parts)
    
    return schema_text


def _format_beaver_basic(tables: List[Dict[str, Any]], fk_strings: List[str]) -> str:
    table_texts = []
    for table in sorted(tables, key=lambda t: t["name"]):
        cols = table.get("columns", [])
        col_list_str = ", ".join(cols)
        table_texts.append(f"{table['name']}({col_list_str})")
    schema_text = '[Schema]\n' + '\n'.join(table_texts)
    if fk_strings:
        schema_text += '\n\n[Foreign Keys]\n' + '\n'.join(fk_strings)
    return schema_text


def _format_beaver_basic_plus_type(tables: List[Dict[str, Any]], fk_strings: List[str]) -> str:
    table_texts = []
    for table in sorted(tables, key=lambda t: t["name"]):
        cols_with_types = []
        col_types = table.get("types", [])
        for idx, col_name in enumerate(table.get("columns", [])):
            col_type_val = col_types[idx] if idx < len(col_types) else "TEXT"
            cols_with_types.append(f"{col_name}: {col_type_val}")
        table_texts.append(f"{table['name']}({', '.join(cols_with_types)})")
    schema_text = '[Schema]\n' + '\n'.join(table_texts)
    if fk_strings:
        schema_text += '\n\n[Foreign Keys]\n' + '\n'.join(fk_strings)
    return schema_text


def _format_beaver_ddl(tables: List[Dict[str, Any]], included_keys: set) -> str:
    table_texts = []
    for table in sorted(tables, key=lambda t: t["name"]):
        definitions = []
        col_types = table.get("types", [])
        db_prefix = table["key"].split('#sep#')[0] if '#sep#' in table["key"] else ""
        for idx, col_name in enumerate(table.get("columns", [])):
            col_type = col_types[idx] if idx < len(col_types) else "TEXT"
            definitions.append(f"  {col_name} {col_type}")

        pk_cols = [col for col in table.get("columns", []) if col in table.get("primary_keys", set())]
        if pk_cols:
            definitions.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")

        fk_constraints = []
        for fk in table.get("foreign_keys", []):
            ref_table_full = fk.get("referenced_table_name", "")
            ref_table_key = ref_table_full if '#sep#' in ref_table_full else (
                f"{db_prefix}#sep#{ref_table_full}" if db_prefix else ref_table_full
            )
            if included_keys and ref_table_key not in included_keys:
                continue
            col_name = fk.get("column_name")
            ref_table = ref_table_full.split('#sep#')[-1] if '#sep#' in ref_table_full else ref_table_full
            ref_col = fk.get("referenced_column_name", "")
            if col_name and ref_table and ref_col:
                fk_constraints.append(f"  FOREIGN KEY ({col_name}) REFERENCES {ref_table}({ref_col})")

        definitions.extend(fk_constraints)
        table_texts.append(f"CREATE TABLE {table['name']} (\n" + ",\n".join(definitions) + "\n);")

    return '[Schema]\n' + '\n\n'.join(table_texts)


def format_schema_beaver_by_style(
    all_schema_info: dict,
    target_db_id: str,
    style: str = "basic",
    table_keys: Optional[List[str]] = None,
    db_examples: Optional[Dict[str, Any]] = None
) -> str:
    """
    Format Beaver schemas across all supported representation styles.

    Args:
        all_schema_info: Raw Beaver schema dictionary from tables.json
        target_db_id: Database id (e.g., 'dw')
        style: One of {'basic', 'basic_plus_type', 'ddl', 'm_schema'}
        table_keys: Optional list of table keys (db#sep#table) to include
        db_examples: Column examples for m_schema formatting
    """
    style = style.lower()
    if style == "m_schema":
        gold_tables = table_keys or [k for k in all_schema_info if k.startswith(target_db_id + '#sep#')]
        return format_schema_beaver_gold_tables(
            all_schema_info=all_schema_info,
            target_db_id=target_db_id,
            gold_tables=gold_tables,
            db_examples=db_examples,
        )

    tables = _collect_beaver_tables(all_schema_info, target_db_id, table_keys)
    if not tables:
        return f"[Schema]\n# No tables found for database '{target_db_id}'."

    included_keys = {tbl["key"] for tbl in tables}
    fk_strings = _format_beaver_foreign_keys(tables, included_keys)

    if style == "basic":
        return _format_beaver_basic(tables, fk_strings)
    elif style == "basic_plus_type":
        return _format_beaver_basic_plus_type(tables, fk_strings)
    elif style == "ddl":
        return _format_beaver_ddl(tables, included_keys)
    else:
        raise ValueError(f"Unsupported Beaver schema style: {style}")


SCHEMA_FORMATTERS = {"basic": format_schema_basic, "basic_plus_type": format_schema_basic_plus_type, "ddl": format_schema_ddl, "m_schema": format_schema_m_schema}

def format_schema(db_info: dict, style: str = "basic", db_examples: Dict[str, Any] = None) -> str:
    formatter = SCHEMA_FORMATTERS.get(style)
    if not formatter: raise ValueError(f"Unknown schema style: {style}. Available styles are: {list(SCHEMA_FORMATTERS.keys())}")
    return formatter(db_info, db_examples=db_examples) if style == 'm_schema' else formatter(db_info)


#################################################################################
#                                       View                                    #
#################################################################################

def get_view_schemas_from_sqlite_db(db_path: str) -> List[str]:
    if not os.path.exists(db_path):
        return []
    
    view_schemas = []
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='view' AND name LIKE '%_rv';")
        views = cursor.fetchall()
        
        for view_name, create_view_sql in views:
            match = re.search(r'AS SELECT (.*) FROM', create_view_sql, re.IGNORECASE)
            if not match: continue
            columns_part = match.group(1)
            aliases = re.findall(r'AS "([^"]+)"', columns_part)
            if aliases:
                view_schemas.append(f"{view_name}({', '.join(aliases)})")
                
    except sqlite3.Error as e:
        print(f"DB Error while reading views from {db_path}: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
    return view_schemas

def get_view_schemas_from_mysql_db(db_config: dict, db_id: str) -> List[str]:
    view_schemas = []
    conn = None
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_id
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM information_schema.VIEWS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME LIKE '%_rv'
        """, (db_id,))
        
        view_names = [row[0] for row in cursor.fetchall()]
        
        for view_name in view_names:
            cursor.execute("""
                SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
                ORDER BY ORDINAL_POSITION;
            """, (db_id, view_name))
            
            columns = [row[0] for row in cursor.fetchall()]
            if columns:
                view_schemas.append(f"{view_name}({', '.join(columns)})")

    except Exception as e:
        print(f"DB Error while reading MySQL views from {db_id}: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
    return view_schemas

def _format_foreign_keys_with_mapping(db_info: Dict[str, Any], mapping: Dict[str, Dict[str, str]]) -> str:
    foreign_key_texts = []
    table_names = db_info.get('table_names_original', [])
    column_names = db_info.get('column_names_original', [])

    for fk in db_info.get('foreign_keys', []):
        try:
            col_idx_1, col_idx_2 = fk
            tbl_idx_1, original_col_1 = column_names[col_idx_1]
            tbl_idx_2, original_col_2 = column_names[col_idx_2]
            
            original_tbl_1 = table_names[tbl_idx_1]
            original_tbl_2 = table_names[tbl_idx_2]

            view_name_1 = original_tbl_1 + "_rv"
            view_name_2 = original_tbl_2 + "_rv"

            alias_col_1 = mapping.get(original_tbl_1, {}).get(original_col_1, original_col_1)
            alias_col_2 = mapping.get(original_tbl_2, {}).get(original_col_2, original_col_2)
            
            foreign_key_texts.append(f"{view_name_1}.{alias_col_1} = {view_name_2}.{alias_col_2}")
        except (IndexError, TypeError):
            continue
    return '\n'.join(foreign_key_texts)

def _format_beaver_foreign_keys_with_mapping(all_schema_info: dict, db_id: str, mapping: Dict[str, Dict[str, str]]) -> str:
    foreign_key_texts = []
    for key, table_info in all_schema_info.items():
        if not key.startswith(db_id + '#sep#'):
            continue
            
        original_tbl_1 = table_info['table_name_original']
        view_name_1 = original_tbl_1 + "_rv"
        
        for fk in table_info.get('foreign_key', []):
            original_col_1 = fk['column_name']
            
            ref_table_full = fk['referenced_table_name']
            original_tbl_2 = ref_table_full.split('#sep#')[-1]
            view_name_2 = original_tbl_2 + "_rv"
            original_col_2 = fk['referenced_column_name']

            alias_col_1 = mapping.get(original_tbl_1, {}).get(original_col_1, original_col_1)
            alias_col_2 = mapping.get(original_tbl_2, {}).get(original_col_2, original_col_2)
            
            foreign_key_texts.append(f"{view_name_1}.{alias_col_1} = {view_name_2}.{alias_col_2}")
            
    return '\n'.join(sorted(list(set(foreign_key_texts)))) # 중복 제거 및 정렬

def format_schema_views_basic(all_schema_info: dict, db_id: str, mapping: Dict[str, Dict[str, str]]) -> str:
    """
    Renamed View 스키마를 'VIEW_NAME(col1, col2, ...)' 기본 형식으로 포맷팅합니다.
    """
    view_texts = []
    
    # 원본 스키마 순서를 따르기 위해 all_schema_info를 순회합니다.
    for key, table_info in all_schema_info.items():
        if not key.startswith(db_id + '#sep#'):
            continue
        
        original_table_name = table_info['table_name_original']
        view_name = f"{original_table_name}_rv"
        
        original_columns = table_info.get('column_names_original', [])
        
        # 원본 컬럼 순서에 맞춰 매핑에서 새 컬럼 이름을 찾습니다.
        renamed_columns = []
        for original_col in original_columns:
            alias = mapping.get(original_table_name, {}).get(original_col, original_col)
            renamed_columns.append(alias)
        
        if renamed_columns:
            view_texts.append(f"{view_name}({', '.join(renamed_columns)})")
            
    schema_text = '[Schema]\n' + '\n'.join(sorted(view_texts))
    
    # Renamed View 기준의 외래 키 정보 추가
    foreign_key_text = _format_beaver_foreign_keys_with_mapping(all_schema_info, db_id, mapping)
    if foreign_key_text:
        schema_text += '\n\n[Foreign Keys]\n' + foreign_key_text
        
    return schema_text

def format_schema_with_views(
    db_id: str, 
    db_info: dict, 
    mapping: Dict[str, Dict[str, str]], 
    dataset_name: str,
    schema_sql_path: str
) -> str:
    """
    미리 생성된 DDL 파일을 읽고, FK 정보를 추가하여 최종 스키마를 구성합니다.
    이때 프로파일링 주석은 제거합니다.
    """
    # 1. 주석이 포함된 DDL 파일을 읽어옵니다.
    try:
        with open(schema_sql_path, 'r', encoding='utf-8') as f:
            raw_ddl_text = f.read()
    except FileNotFoundError:
        return f"[Schema]\n# Error: Schema file with profiles not found at {schema_sql_path}"

    # 2. '--'로 시작하는 프로파일링 주석 라인을 모두 제거합니다.
    #    re.MULTILINE 플래그는 ^ 기호가 각 줄의 시작에서 작동하도록 합니다.
    no_comments_text = re.sub(r'^\s*--.*\n', '', raw_ddl_text, flags=re.MULTILINE)
    
    # 3. CREATE TABLE을 CREATE VIEW로 치환합니다.
    view_schema_text = no_comments_text.replace("CREATE TABLE", "CREATE VIEW")
        
    # 4. 데이터셋 종류에 따라 FK 정보를 포맷팅합니다.
    foreign_key_text = ""
    if dataset_name in ['spider', 'bird']:
        foreign_key_text = _format_foreign_keys_with_mapping(db_info, mapping)
    elif dataset_name == 'beaver':
        foreign_key_text = _format_beaver_foreign_keys_with_mapping(db_info, db_id, mapping)
        
    # 5. 프롬프트 문자열을 조합합니다.
    schema_text = '[Schema]\n' + view_schema_text.strip()
    if foreign_key_text:
        schema_text += '\n\n[Foreign Keys]\n' + foreign_key_text

    return schema_text
