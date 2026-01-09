"""
dw_join_keys.json에서 PK 후보 추출
- JOIN에 사용되는 컬럼 = PK 후보일 가능성 높음
- 테이블별로 정리
"""
import json
import os
import mysql.connector
from collections import defaultdict, Counter
from dotenv import load_dotenv

load_dotenv()

# 1. 모든 테이블 목록 가져오기
print("=" * 100)
print("STEP 1: Getting all tables from database")
print("=" * 100)

conn = mysql.connector.connect(
    host='127.0.0.1',
    port=3306,
    user='root',
    password=os.getenv('MYSQL_PASSWORD', ''),
    database='dw'
)
cursor = conn.cursor()

cursor.execute("""
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dw'
    ORDER BY TABLE_NAME
""")
all_tables = [row[0].upper() for row in cursor.fetchall()]  # 대문자로 변환
cursor.close()
conn.close()

print(f"Found {len(all_tables)} tables")

# 2. JOIN KEYS에서 PK 후보 추출
print("\n" + "=" * 100)
print("STEP 2: Extracting PK candidates from dw_join_keys.json")
print("=" * 100)

with open('data/beaver/dw/dw_join_keys.json') as f:
    join_keys = json.load(f)

# 테이블별로 JOIN에 사용된 컬럼 카운트
pk_candidates_count = defaultdict(Counter)

for pair in join_keys:
    # ["FCLT_ROOMS.FCLT_BUILDING_KEY", "FCLT_BUILDING_ADDRESS.FCLT_BUILDING_KEY"]
    table1, col1 = pair[0].split('.')
    table2, col2 = pair[1].split('.')
    
    # 대소문자 통일 (대문자로)
    table1 = table1.upper()
    table2 = table2.upper()
    
    # 각 컬럼이 JOIN에 사용된 횟수 카운트
    pk_candidates_count[table1][col1] += 1
    pk_candidates_count[table2][col2] += 1

print(f"Analyzed {len(join_keys)} join pairs")

# 3. 테이블별 PK 후보 딕셔너리 생성
pk_candidates_dict = {}

for table in all_tables:
    if table in pk_candidates_count:
        # JOIN에 사용된 컬럼들을 사용 빈도순으로 정렬
        candidates = pk_candidates_count[table].most_common()
        
        # _KEY로 끝나는 컬럼 우선
        def sort_key(item):
            col_name, count = item
            priority = 0 if col_name.endswith('_KEY') else 1
            return (priority, -count)  # 우선순위 높고, 사용 빈도 높은 순
        
        sorted_candidates = sorted(candidates, key=sort_key)
        
        # 가장 가능성 높은 후보들만 (최대 3개)
        top_candidates = [col for col, _ in sorted_candidates[:3]]
        pk_candidates_dict[table] = top_candidates
    else:
        # JOIN에 사용되지 않는 테이블 = PK 후보 없음
        pk_candidates_dict[table] = []

# 4. 결과 출력
print("\n" + "=" * 100)
print("STEP 3: Results")
print("=" * 100)

tables_with_pk = sum(1 for candidates in pk_candidates_dict.values() if candidates)
tables_without_pk = len(pk_candidates_dict) - tables_with_pk

print(f"\n📊 Total tables: {len(pk_candidates_dict)}")
print(f"✅ Tables with PK candidates: {tables_with_pk}")
print(f"❌ Tables without PK candidates: {tables_without_pk}")

print("\n" + "-" * 100)
print("TABLES WITH PK CANDIDATES:")
print("-" * 100)
for table, candidates in sorted(pk_candidates_dict.items()):
    if candidates:
        if len(candidates) == 1:
            print(f"  {table:50s} → {candidates[0]}")
        else:
            print(f"  {table:50s} → {candidates[0]} (also: {', '.join(candidates[1:])})")

print("\n" + "-" * 100)
print("TABLES WITHOUT PK CANDIDATES (not used in JOINs):")
print("-" * 100)
for table, candidates in sorted(pk_candidates_dict.items()):
    if not candidates:
        print(f"  {table}")

# 5. JSON으로 저장
output_path = 'data/beaver/dw/pk_candidates_from_joins.json'
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(pk_candidates_dict, f, indent=2, ensure_ascii=False)

print("\n" + "=" * 100)
print(f"✅ Saved to: {output_path}")
print("=" * 100)

# 6. 샘플 출력
print("\n" + "=" * 100)
print("SAMPLE RESULTS:")
print("=" * 100)
sample_tables = ['FCLT_ROOMS', 'BUILDINGS', 'FCLT_BUILDING_ADDRESS', 'MASTER_DEPT_HIERARCHY']
for table in sample_tables:
    if table in pk_candidates_dict:
        candidates = pk_candidates_dict[table]
        if candidates:
            print(f"\n{table}:")
            for i, col in enumerate(candidates, 1):
                print(f"  {i}. {col}")
        else:
            print(f"\n{table}: (no candidates)")
