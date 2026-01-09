"""
PK 후보에 데이터 타입과 예시값 추가
"""
import json
import os
import mysql.connector
from collections import defaultdict, Counter
from dotenv import load_dotenv

load_dotenv()

# 1. 기존 PK 후보 로드
with open('data/beaver/dw/pk_candidates_from_joins.json') as f:
    pk_candidates = json.load(f)

print("=" * 100)
print("Adding data types and sample values to PK candidates")
print("=" * 100)

# 2. DB 연결
conn = mysql.connector.connect(
    host='127.0.0.1',
    port=3306,
    user='root',
    password=os.getenv('MYSQL_PASSWORD', ''),
    database='dw'
)
cursor = conn.cursor()

# 3. 각 테이블의 PK 후보에 대해 정보 추가
enhanced_pk_info = {}

for table, candidates in pk_candidates.items():
    print(f"\nProcessing: {table}")
    
    if not candidates:
        enhanced_pk_info[table] = {
            'pk_candidates': [],
            'note': 'Not used in JOINs'
        }
        continue
    
    candidate_details = []
    
    for col in candidates:
        # 컬럼 타입 가져오기
        cursor.execute(f"""
            SELECT DATA_TYPE, COLUMN_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dw'
              AND TABLE_NAME = '{table}'
              AND COLUMN_NAME = '{col}'
        """)
        type_info = cursor.fetchone()
        
        if not type_info:
            print(f"  ⚠️  Column {col} not found")
            continue
        
        data_type, column_type = type_info
        
        # 샘플 값 가져오기 (NULL 제외, DISTINCT, 최대 5개)
        try:
            cursor.execute(f"""
                SELECT DISTINCT `{col}`
                FROM `{table}`
                WHERE `{col}` IS NOT NULL
                LIMIT 5
            """)
            sample_values = [str(row[0]) for row in cursor.fetchall()]
        except:
            sample_values = []
        
        # 유니크 개수 확인
        try:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT `{col}`) as distinct_count
                FROM `{table}`
                WHERE `{col}` IS NOT NULL
            """)
            total, distinct = cursor.fetchone()
            uniqueness = (distinct / total * 100) if total > 0 else 0
        except:
            uniqueness = 0
        
        candidate_details.append({
            'column': col,
            'data_type': data_type,
            'column_type': column_type,
            'sample_values': sample_values,
            'uniqueness_percent': round(uniqueness, 2)
        })
        
        print(f"  ✓ {col:40s} {data_type:15s} - {len(sample_values)} samples, {uniqueness:.1f}% unique")
    
    enhanced_pk_info[table] = {
        'pk_candidates': candidate_details,
        'recommended_pk': candidate_details[0]['column'] if candidate_details else None
    }

cursor.close()
conn.close()

# 4. 저장
output_path = 'data/beaver/dw/pk_candidates_enhanced.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(enhanced_pk_info, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 100)
print(f"✅ Enhanced PK info saved to: {output_path}")
print("=" * 100)

# 5. 샘플 출력
print("\n" + "=" * 100)
print("SAMPLE: BUILDINGS table")
print("=" * 100)
if 'BUILDINGS' in enhanced_pk_info:
    print(json.dumps(enhanced_pk_info['BUILDINGS'], indent=2))

print("\n" + "=" * 100)
print("SAMPLE: FCLT_ROOMS table")
print("=" * 100)
if 'FCLT_ROOMS' in enhanced_pk_info:
    print(json.dumps(enhanced_pk_info['FCLT_ROOMS'], indent=2))
