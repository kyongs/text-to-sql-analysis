# src/data_loader/beaver_loader.py

import os
import json
import re
from typing import List, Dict, Any
from .base_loader import BaseDataLoader
from src.prompt_builder.schema_formatter import (
    format_schema_beaver, 
    format_schema_beaver_by_style,
    format_schema_with_views, 
    format_schema_views_basic,
    format_schema_beaver_gold_tables
)

class BeaverLoader(BaseDataLoader):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        dataset_path = self.dataset_config['path']
        self.split = self.dataset_config['split']
        
        # 예제 값 캐시 (DB별로 한 번만 쿼리)
        self._examples_cache = {}
        self.preprocessed_schema_cache = None
        self.preprocessed_schema_path = os.path.join(dataset_path, f"{self.split}_preprocessed_schemas.json")
        
        # 원본 스키마 정보 로드
        table_json_path = os.path.join(dataset_path, "tables.json")
        self.raw_schema_info = {}
        try:
            with open(table_json_path, 'r', encoding='utf-8') as f:
                self.raw_schema_info = json.load(f)
            print(f"✅ Successfully loaded raw schemas from {table_json_path}")
        except (FileNotFoundError, TypeError) as e:
            print(f"🚨 FATAL: Raw tables file not found or invalid: {e}")

        # View 모드에 필요한 파일(매핑, DDL) 경로 로드
        self.view_mappings = {}
        self.view_schema_sql_paths = {}
        
        mapping_dir = self.config.get('evaluation', {}).get('mapping_dir')
        
        if mapping_dir and os.path.exists(mapping_dir):
            dataset_name = self.dataset_config['name']
            specific_mapping_dir = os.path.join(mapping_dir, dataset_name)
            if not os.path.exists(specific_mapping_dir):
                print(f"🚨 WARNING: Specific mapping dir not found: {specific_mapping_dir}. Looking in parent dir.")
                specific_mapping_dir = mapping_dir

            for filename in os.listdir(specific_mapping_dir):
                db_id = ""
                if filename.endswith('_mapping.json'):
                    db_id = filename.replace('_mapping.json', '')
                    filepath = os.path.join(specific_mapping_dir, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            self.view_mappings[db_id] = json.load(f)
                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode mapping file {filename}")
                
                elif filename.endswith('_views.sql'):
                    db_id = filename.replace('_views.sql', '')
                    self.view_schema_sql_paths[db_id] = os.path.join(specific_mapping_dir, filename)

            print(f"✅ Successfully loaded {len(self.view_mappings)} view mappings and {len(self.view_schema_sql_paths)} view DDLs for Beaver.")
        else:
            print(f"🚨 WARNING: View artifact directory not found or not specified. Looked for: {mapping_dir}")

    def _translate_column_reference(self, col_ref: str, view_mapping: Dict[str, Dict[str, str]]) -> str:
        """
        단일 컬럼 참조를 변환합니다.
        형식: "table.column" -> "table_rv.renamed_column"
        """
        try:
            # "table.column" 형태 파싱
            if '.' in col_ref:
                original_table, original_column = col_ref.split('.', 1)
                renamed_view = f"{original_table}_rv"
                renamed_column = view_mapping.get(original_table, {}).get(original_column, original_column)
                return f"{renamed_view}.{renamed_column}"
            else:
                # 테이블명 없이 컬럼명만 있는 경우
                return col_ref
        except ValueError:
            return col_ref

    def _translate_text_hints(self, text: str, view_mapping: Dict[str, Dict[str, str]]) -> str:
        """
        텍스트 내의 모든 테이블.컬럼 참조를 재귀적으로 변환합니다.
        정규표현식을 사용하여 table.column 패턴을 찾아서 변환합니다.
        """
        # 테이블.컬럼 패턴 찾기 (영문, 숫자, 언더스코어 허용)
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b'
        
        def replace_func(match):
            table_name = match.group(1)
            column_name = match.group(2)
            
            # view_mapping에 해당 테이블이 있는지 확인
            if table_name in view_mapping:
                renamed_view = f"{table_name}_rv"
                renamed_column = view_mapping[table_name].get(column_name, column_name)
                return f"{renamed_view}.{renamed_column}"
            return match.group(0)  # 매핑이 없으면 원본 유지
        
        return re.sub(pattern, replace_func, text)

    def _translate_hints_recursive(self, obj: Any, view_mapping: Dict[str, Dict[str, str]]) -> Any:
        """
        hints 객체를 재귀적으로 순회하며 모든 테이블.컬럼 참조를 변환합니다.
        """
        if isinstance(obj, dict):
            return {key: self._translate_hints_recursive(val, view_mapping) for key, val in obj.items()}
        elif isinstance(obj, list):
            return [self._translate_hints_recursive(item, view_mapping) for item in obj]
        elif isinstance(obj, str):
            # 문자열 내의 테이블.컬럼 참조 변환
            return self._translate_text_hints(obj, view_mapping)
        else:
            return obj

    def _translate_hints(self, item: Dict[str, Any], view_mapping: Dict[str, Dict[str, str]]):
        """
        item의 모든 hint 관련 필드를 변환합니다.
        """
        # 알려진 hint 필드들
        hint_fields = ['mapping', 'join_keys', 'hints', 'evidence', 'SQL']
        
        for field in hint_fields:
            if field in item and item[field]:
                item[field] = self._translate_hints_recursive(item[field], view_mapping)
        
        # 추가: 혹시 모를 다른 필드들도 검사
        # 'db_id', 'question', 'difficulty' 같은 메타데이터는 제외
        excluded_fields = {'db_id', 'question', 'difficulty', 'formatted_schema', 'question_id'}
        
        for key, value in item.items():
            if key not in excluded_fields and key not in hint_fields:
                # 문자열이나 리스트/딕셔너리인 경우에만 변환 시도
                if isinstance(value, (str, list, dict)):
                    item[key] = self._translate_hints_recursive(value, view_mapping)
        
        return item

    def _extract_column_examples(self, db_id: str, gold_tables: List[str]) -> Dict[tuple, List]:
        """
        gold_tables의 컬럼들에 대해 SQL 쿼리를 실행하여 예제 값을 추출합니다.
        캐싱을 사용하여 동일 DB에 대해 반복 쿼리를 방지합니다.
        
        Args:
            db_id: 데이터베이스 ID
            gold_tables: 예제를 추출할 테이블 리스트
            
        Returns:
            {(table_name, col_name): [example_values]} 형태의 딕셔너리
        """
        # 캐시 체크
        cache_key = f"{db_id}:{','.join(sorted(gold_tables))}"
        if cache_key in self._examples_cache:
            return self._examples_cache[cache_key]
        
        db_examples = {}
        db_type = self.dataset_config.get('db_type', 'sqlite')
        
        if db_type != 'mysql':
            # SQLite나 다른 DB는 아직 구현 안 함
            self._examples_cache[cache_key] = db_examples
            return db_examples
        
        conn = None
        try:
            import mysql.connector
            
            # MySQL 연결 정보
            conn_info = self.config.get('db_connection', {})
            conn = mysql.connector.connect(
                host=conn_info.get('host', '127.0.0.1'),
                port=conn_info.get('port', 3306),
                user=conn_info.get('user', 'root'),
                password=conn_info.get('password', ''),
                database=db_id
            )
            cursor = conn.cursor()
            
            # gold_tables의 각 테이블/컬럼에 대해 예제 추출
            for table_key in gold_tables:
                if table_key not in self.raw_schema_info:
                    continue
                
                table_info = self.raw_schema_info[table_key]
                table_name = table_info.get('table_name_original', '')
                columns = table_info.get('column_names_original', [])
                
                for col_name in columns:
                    try:
                        # DISTINCT 값 최대 3개 추출
                        query = f"SELECT DISTINCT `{col_name}` FROM `{table_name}` WHERE `{col_name}` IS NOT NULL LIMIT 3"
                        cursor.execute(query)
                        results = [row[0] for row in cursor.fetchall()]
                        
                        if results:
                            db_examples[(table_name, col_name)] = results
                    except Exception as e:
                        # 쿼리 실패 시 조용히 스킵 (예: 컬럼이 없거나 권한 문제 등)
                        pass
            
        except Exception as e:
            print(f"Warning: Failed to extract column examples from database: {e}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
        
        # 캐시에 저장
        self._examples_cache[cache_key] = db_examples
        return db_examples

    def _load_preprocessed_schema(self) -> Dict[str, Any]:
        """Load preprocessed schema cache for Beaver if available."""
        if self.preprocessed_schema_cache is not None:
            return self.preprocessed_schema_cache
        if not os.path.exists(self.preprocessed_schema_path):
            return None
        try:
            with open(self.preprocessed_schema_path, "r", encoding="utf-8") as f:
                self.preprocessed_schema_cache = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not decode preprocessed schema file at {self.preprocessed_schema_path}")
            self.preprocessed_schema_cache = None
        return self.preprocessed_schema_cache

    def load_data(self, load_views: bool = False) -> List[Dict[str, Any]]:
        split = self.dataset_config['split']
        json_path = os.path.join(self.dataset_config['path'], f"{split}.json")
        schema_style = self.dataset_config.get("schema_representation", "basic")   
        
        # Check if gold_schema mode is enabled
        mode = self.config.get('mode', 'baseline')
        is_gold_schema_mode = (mode == 'gold_schema')
        
        # 캐시된 formatted_data 확인 (gold_schema + m_schema 모드에서만)
        formatted_data_path = r"C:\Users\domir\Desktop\code\text-to-sql-analysis\data\beaver\dw\formatted_data.json"
        if is_gold_schema_mode and schema_style == "m_schema" and os.path.exists(formatted_data_path):
            try:
                with open(formatted_data_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"✅ Using cached formatted_data from {formatted_data_path}")
                return data
            except Exception as e:
                print(f"⚠️ Failed to load cached data: {e}. Proceeding with fresh generation...")
        
        if not self.raw_schema_info:
            print("Error: Raw schema info is required. Cannot proceed.")
            return []

        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            preprocessed_schemas = self._load_preprocessed_schema() if schema_style == "m_schema" else None

            for item in data:
                db_id = item['db_id']
                
                if load_views:
                    view_mapping = self.view_mappings.get(db_id)
                    if not view_mapping:
                        item['formatted_schema'] = f"Error: View mapping for '{db_id}' not found."
                        continue
                    
                    # hints 변환 (개선된 버전)
                    item = self._translate_hints(item, view_mapping)
                    
                    schema_style = self.dataset_config.get('schema_representation', 'ddl')
                    
                    if schema_style == 'basic':
                        item['formatted_schema'] = format_schema_views_basic(self.raw_schema_info, db_id, view_mapping)
                    else:
                        schema_sql_path = self.view_schema_sql_paths.get(db_id)
                        if not schema_sql_path:
                            item['formatted_schema'] = f"Error: View DDL file for '{db_id}' not found."
                            continue
                        
                        item['formatted_schema'] = format_schema_with_views(
                            db_id=db_id, db_info=self.raw_schema_info, mapping=view_mapping,
                            dataset_name='beaver', schema_sql_path=schema_sql_path
                        )
                        
                elif is_gold_schema_mode:
                    # Gold Schema mode: Use only gold_tables from the item
                    gold_tables = item.get('gold_tables', [])
                    
                    table_keys = gold_tables if gold_tables else None
                    if not gold_tables:
                        print(f"Warning: No gold_tables found for question '{item.get('question_id', '')}'. Using full schema.")
                    if schema_style == "m_schema":
                        db_examples = self._extract_column_examples(db_id, gold_tables) if gold_tables else {}
                        item['formatted_schema'] = format_schema_beaver_by_style(
                            all_schema_info=self.raw_schema_info,
                            target_db_id=db_id,
                            style="m_schema",
                            table_keys=table_keys,
                            db_examples=db_examples
                        )
                    else:
                        item['formatted_schema'] = format_schema_beaver_by_style(
                            all_schema_info=self.raw_schema_info,
                            target_db_id=db_id,
                            style=schema_style,
                            table_keys=table_keys
                        )
                        
                else:  # Baseline mode
                    if schema_style == "m_schema" and preprocessed_schemas and db_id in preprocessed_schemas:
                        item["formatted_schema"] = preprocessed_schemas[db_id].get("m_schema", "")
                    else:
                        item["formatted_schema"] = format_schema_beaver_by_style(
                            all_schema_info=self.raw_schema_info,
                            target_db_id=db_id,
                            style=schema_style
                        )

            

            mode_str = "'view'" if load_views else ("'gold_schema'" if is_gold_schema_mode else "'baseline'")
            print(f"Successfully loaded and processed {len(data)} examples for Beaver in {mode_str} mode.")
            with open(r"C:\Users\domir\Desktop\code\text-to-sql-analysis\data\beaver\dw\formatted_data.json", "w", encoding="utf-8") as f: 
                json.dump(data, f, ensure_ascii=False, indent=2)
                print("Formatted data saved to formatted_data.json")
            return data
        except FileNotFoundError:
            print(f"Error: Data file not found at {json_path}")
            return []

    def get_db_path(self, db_id: str) -> str:
        return db_id
