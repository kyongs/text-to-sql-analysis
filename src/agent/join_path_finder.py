# src/agent/join_path_finder.py

import json
import os
import mysql.connector
from typing import List, Dict, Tuple, Set, Optional


def find_join_path(table1: str, table2: str, join_keys_file: str = None, 
                   pk_candidates_file: str = None, conn_info: Dict = None, db_id: str = "dw") -> str:
    """
    두 테이블 사이의 최적 JOIN 경로를 찾고, 실제 데이터 샘플을 보여주는 함수
    
    Args:
        table1: 시작 테이블 이름
        table2: 목표 테이블 이름
        join_keys_file: JOIN 키 정보가 담긴 JSON 파일 경로
        pk_candidates_file: PK 후보 정보가 담긴 JSON 파일 경로
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID
        
    Returns:
        JOIN 경로 정보 및 데이터 샘플 문자열
    """
    # Default paths
    if join_keys_file is None:
        join_keys_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'data', 'beaver', 'dw', 'dw_join_keys.json'
        )
    
    if pk_candidates_file is None:
        pk_candidates_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'data', 'beaver', 'dw', 'pk_candidates_enhanced.json'
        )
    
    # Load join relationships
    try:
        with open(join_keys_file, 'r') as f:
            join_keys = json.load(f)
    except Exception as e:
        return f"❌ Error loading join keys: {str(e)}"
    
    # Load PK candidates
    try:
        with open(pk_candidates_file, 'r') as f:
            pk_candidates = json.load(f)
    except Exception as e:
        return f"❌ Error loading PK candidates: {str(e)}"
    
    # Normalize table names (case-insensitive)
    table1_upper = table1.upper()
    table2_upper = table2.upper()
    
    # Build adjacency graph with enhanced metadata
    graph = build_enhanced_graph(join_keys, pk_candidates)
    
    # Find all possible paths (not just shortest)
    paths = find_all_paths(graph, table1_upper, table2_upper, max_length=4)
    
    if not paths:
        return f"""❌ No JOIN path found between {table1} and {table2}.

These tables may not be directly or indirectly joinable.
Please verify table names and join relationships."""
    
    # Sort paths by length and quality score
    paths = sorted(paths, key=lambda p: (len(p), -calculate_path_quality(graph, p)))
    
    # Get the best path (shortest with highest quality)
    best_path = paths[0]
    
    # Execute SQL and get sample data
    if conn_info:
        sample_data = execute_join_sample(best_path, graph, conn_info, db_id)
    else:
        sample_data = None
    
    # Format the result
    return format_join_path_result(best_path, graph, pk_candidates, sample_data, paths[:3])


def build_enhanced_graph(join_keys: List[Dict], pk_candidates: Dict) -> Dict[str, Dict[str, Dict]]:
    """
    JOIN 관계를 PK 정보로 강화된 그래프로 변환

    Returns:
        graph[table1][table2] = {
            'key1': col1,
            'key2': col2,
            'pk1': recommended_pk1,
            'pk2': recommended_pk2,
            'uniqueness1': float,
            'uniqueness2': float
        }
    """
    graph = {}

    for entry in join_keys:
        # entry format: ["TABLE1.COLUMN1", "TABLE2.COLUMN2"]
        if isinstance(entry, list) and len(entry) == 2:
            key1_full = entry[0]
            key2_full = entry[1]

            # Split "TABLE.COLUMN" into table and column
            t1, k1 = key1_full.split('.')
            t2, k2 = key2_full.split('.')

            t1 = t1.upper()
            t2 = t2.upper()
        elif isinstance(entry, dict):
            # Support dict format too: {"table1": "T1", "table2": "T2", "key1": "K1", "key2": "K2"}
            t1 = entry['table1'].upper()
            t2 = entry['table2'].upper()
            k1 = entry['key1']
            k2 = entry['key2']
        else:
            continue
        
        # Get PK information for quality scoring
        pk_info1 = pk_candidates.get(t1, {})
        pk_info2 = pk_candidates.get(t2, {})
        
        # Find uniqueness percentage for the join keys
        uniqueness1 = get_column_uniqueness(pk_info1, k1)
        uniqueness2 = get_column_uniqueness(pk_info2, k2)
        
        # Add bidirectional edges with metadata
        if t1 not in graph:
            graph[t1] = {}
        if t2 not in graph:
            graph[t2] = {}
        
        edge_data = {
            'key1': k1,
            'key2': k2,
            'pk1': pk_info1.get('recommended_pk'),
            'pk2': pk_info2.get('recommended_pk'),
            'uniqueness1': uniqueness1,
            'uniqueness2': uniqueness2
        }
        
        graph[t1][t2] = edge_data
        graph[t2][t1] = {
            'key1': k2,
            'key2': k1,
            'pk1': pk_info2.get('recommended_pk'),
            'pk2': pk_info1.get('recommended_pk'),
            'uniqueness1': uniqueness2,
            'uniqueness2': uniqueness1
        }
    
    return graph


def get_column_uniqueness(pk_info: Dict, column: str) -> float:
    """특정 컬럼의 uniqueness 퍼센티지 가져오기"""
    if not pk_info or 'pk_candidates' not in pk_info:
        return 0.0
    
    for candidate in pk_info['pk_candidates']:
        if candidate['column'] == column:
            return candidate.get('uniqueness_percent', 0.0)
    
    return 0.0


def find_all_paths(graph: Dict, start: str, end: str, max_length: int = 4) -> List[List[str]]:
    """
    DFS를 사용해 두 테이블 사이의 모든 가능한 경로 찾기 (max_length 제한)
    
    Returns:
        모든 경로 리스트
    """
    if start not in graph or end not in graph:
        return []
    
    if start == end:
        return [[start]]
    
    all_paths = []
    
    def dfs(current: str, target: str, visited: Set[str], path: List[str]):
        if len(path) > max_length:
            return
        
        if current == target:
            all_paths.append(path[:])
            return
        
        for neighbor in graph.get(current, {}):
            if neighbor not in visited:
                visited.add(neighbor)
                path.append(neighbor)
                dfs(neighbor, target, visited, path)
                path.pop()
                visited.remove(neighbor)
    
    dfs(start, end, {start}, [start])
    return all_paths


def calculate_path_quality(graph: Dict, path: List[str]) -> float:
    """
    경로 품질 점수 계산 (높을수록 좋음)
    - PK로 JOIN하면 가산점
    - Uniqueness가 높으면 가산점
    """
    quality = 0.0
    
    for i in range(len(path) - 1):
        edge = graph[path[i]][path[i+1]]
        
        # PK로 JOIN하면 점수 높임
        if edge['key1'] == edge['pk1']:
            quality += 10
        if edge['key2'] == edge['pk2']:
            quality += 10
        
        # Uniqueness 점수 추가
        quality += edge.get('uniqueness1', 0) * 0.1
        quality += edge.get('uniqueness2', 0) * 0.1
    
    return quality


def execute_join_sample(path: List[str], graph: Dict, conn_info: Dict, db_id: str) -> Optional[str]:
    """
    실제 JOIN SQL을 실행하고 샘플 데이터 3개 row 가져오기
    """
    try:
        # Build JOIN SQL
        sql = f"SELECT *\nFROM {path[0]} t1"
        
        for i in range(1, len(path)):
            edge = graph[path[i-1]][path[i]]
            sql += f"\nJOIN {path[i]} t{i+1} ON t{i}.{edge['key1']} = t{i+1}.{edge['key2']}"
        
        sql += "\nLIMIT 3"
        
        # Execute
        conn = mysql.connector.connect(**conn_info, database=db_id)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        conn.close()
        
        if not rows:
            return "⚠️ JOIN returns 0 rows (no matching data)"
        
        # Format as table
        result = f"Sample Data ({len(rows)} rows):\n"
        result += "  " + " | ".join(columns[:5]) + ("..." if len(columns) > 5 else "") + "\n"
        result += "  " + "-" * 80 + "\n"
        
        for row in rows:
            row_str = " | ".join(str(v)[:15] for v in row[:5])
            if len(row) > 5:
                row_str += "..."
            result += "  " + row_str + "\n"
        
        return result
        
    except Exception as e:
        return f"⚠️ Could not execute sample query: {str(e)}"


def format_join_path_result(best_path: List[str], graph: Dict, pk_candidates: Dict, 
                            sample_data: Optional[str], alternative_paths: List[List[str]]) -> str:
    """JOIN 경로 결과를 포맷팅"""
    
    if len(best_path) == 2:
        # Direct join
        edge = graph[best_path[0]][best_path[1]]
        
        result = f"""✅ DIRECT JOIN PATH:

{best_path[0]} → {best_path[1]}
  Join Key: {edge['key1']} = {edge['key2']}"""
        
        # Add PK information
        pk1 = pk_candidates.get(best_path[0], {}).get('recommended_pk')
        pk2 = pk_candidates.get(best_path[1], {}).get('recommended_pk')
        
        if pk1:
            result += f"\n  {best_path[0]} Primary Key: {pk1}"
        if pk2:
            result += f"\n  {best_path[1]} Primary Key: {pk2}"
        
        # Uniqueness info
        result += f"\n  Join Key Uniqueness: {edge['uniqueness1']:.1f}% ↔ {edge['uniqueness2']:.1f}%"
        
        # Recommended SQL
        result += f"\n\nRecommended SQL:\n  FROM {best_path[0]} t1\n  JOIN {best_path[1]} t2 ON t1.{edge['key1']} = t2.{edge['key2']}"
        
    else:
        # Multi-hop join
        result = f"""⚠️ MULTI-HOP JOIN PATH (requires intermediate tables):

Path: {' → '.join(best_path)}
Quality Score: {calculate_path_quality(graph, best_path):.1f}

"""
        # Detail each hop
        for i in range(len(best_path) - 1):
            edge = graph[best_path[i]][best_path[i+1]]
            result += f"""Step {i+1}: {best_path[i]} → {best_path[i+1]}
  Join Key: {edge['key1']} = {edge['key2']}
  Uniqueness: {edge['uniqueness1']:.1f}% ↔ {edge['uniqueness2']:.1f}%
"""
        
        # Recommended SQL
        result += f"\nRecommended SQL:\n  FROM {best_path[0]} t1"
        for i in range(1, len(best_path)):
            edge = graph[best_path[i-1]][best_path[i]]
            result += f"\n  JOIN {best_path[i]} t{i+1} ON t{i}.{edge['key1']} = t{i+1}.{edge['key2']}"
        
        result += "\n\n💡 TIP: Do NOT skip intermediate tables! Each hop is necessary for data integrity."
    
    # Add sample data if available
    if sample_data:
        result += f"\n\n{sample_data}"
    
    # Show alternative paths if exist
    if len(alternative_paths) > 1:
        result += f"\n\n📋 Alternative Paths ({len(alternative_paths)-1} more):"
        for i, alt_path in enumerate(alternative_paths[1:], 1):
            if i <= 2:  # Show max 2 alternatives
                result += f"\n  {i}. {' → '.join(alt_path)} (quality: {calculate_path_quality(graph, alt_path):.1f})"
    
    return result


# Test code
if __name__ == "__main__":
    # Test direct join
    print("=== Test 1: Direct JOIN ===")
    result = find_join_path("BUILDINGS", "FCLT_BUILDING")
    print(result)
    
    print("\n" + "="*80 + "\n")
    
    # Test multi-hop join
    print("=== Test 2: Multi-hop JOIN ===")
    result = find_join_path("SUBJECT_SUMMARY", "LIBRARY_SUBJECT_OFFERED")
    print(result)
    
    print("\n" + "="*80 + "\n")
    
    # Test with DB connection (requires credentials)
    print("=== Test 3: With Sample Data ===")
    conn_info = {
        'host': 'localhost',
        'user': 'root',
        'password': os.getenv('MYSQL_PASSWORD', '')
    }
    
    if conn_info['password']:
        result = find_join_path("BUILDINGS", "FCLT_BUILDING", conn_info=conn_info)
        print(result)
    else:
        print("Skipped: MYSQL_PASSWORD not set")
