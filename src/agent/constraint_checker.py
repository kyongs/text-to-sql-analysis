# src/agent/constraint_checker.py
"""
테이블의 PK/FK 제약조건 정보를 제공하는 도구
"""

import json
import os
from typing import Dict, Any, List


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
