"""
Column Value Lookup Tool
컬럼의 실제 값들을 조회하여 LLM이 정확한 WHERE 절을 작성하도록 돕습니다.
"""

import os
import mysql.connector
from mysql.connector import Error
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()


def lookup_column_values(
    table: str,
    column: str,
    conn_info: Dict[str, Any],
    db_id: str = "dw",
    search_term: Optional[str] = None,
    limit: int = 20
) -> Dict[str, Any]:
    """
    컬럼의 distinct 값들을 조회합니다.
    LLM이 WHERE 절에 사용할 정확한 값을 확인할 때 사용합니다.

    Args:
        table: 테이블명
        column: 컬럼명
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID
        search_term: 검색할 값 (있으면 해당 값 존재 여부 + 유사값 반환)
        limit: 반환할 최대 distinct 값 개수

    Returns:
        {
            "success": bool,
            "table": str,
            "column": str,
            "search_term": str or None,
            "exact_match": bool or None,  # search_term이 있을 때만
            "similar_values": List[dict] or None,  # search_term이 있을 때만
            "distinct_count": int,
            "values": List[str],
            "sample_with_count": List[dict],  # [{"value": "X", "count": 10}, ...]
            "error": str or None
        }
    """
    try:
        # 환경변수에서 비밀번호 가져오기
        password = os.getenv('MYSQL_PASSWORD') if conn_info.get('password') == 'from_env' else conn_info.get('password')

        connection = mysql.connector.connect(
            host=conn_info.get('host', '127.0.0.1'),
            port=conn_info.get('port', 3306),
            user=conn_info.get('user', 'root'),
            password=password,
            database=db_id
        )

        cursor = connection.cursor()

        # 1. Distinct 값 개수 조회
        count_sql = f"SELECT COUNT(DISTINCT `{column}`) FROM `{table}`"
        cursor.execute(count_sql)
        distinct_count = cursor.fetchone()[0]

        # 2. search_term이 있으면 존재 여부 + 유사값 검색
        exact_match = None
        similar_values = None
        word_matches = None  # 개별 단어 검색 결과

        if search_term:
            # Exact match 확인
            exact_sql = f"SELECT COUNT(*) FROM `{table}` WHERE `{column}` = %s"
            cursor.execute(exact_sql, (search_term,))
            exact_count = cursor.fetchone()[0]
            exact_match = exact_count > 0

            # 유사값 검색 (LIKE로 부분 일치)
            similar_sql = f"""
                SELECT `{column}`, COUNT(*) as cnt
                FROM `{table}`
                WHERE `{column}` LIKE %s
                GROUP BY `{column}`
                ORDER BY cnt DESC
                LIMIT 10
            """
            cursor.execute(similar_sql, (f"%{search_term}%",))
            similar_rows = cursor.fetchall()
            similar_values = [{"value": str(row[0]), "count": row[1]} for row in similar_rows]

            # NOT FOUND이고 유사값도 없으면 → 개별 단어로 검색
            if not exact_match and not similar_values:
                # 쉼표, 공백으로 분리하여 개별 단어 검색
                import re
                words = [w.strip() for w in re.split(r'[,\s]+', search_term) if w.strip() and len(w.strip()) >= 2]

                word_matches = {}

                # 먼저 모든 단어가 포함된 값 찾기 (가장 높은 우선순위)
                if len(words) >= 2:
                    # 모든 단어 AND 조건
                    like_conditions = " AND ".join([f"`{column}` LIKE %s" for _ in words])
                    all_words_sql = f"""
                        SELECT `{column}`, COUNT(*) as cnt
                        FROM `{table}`
                        WHERE {like_conditions}
                        GROUP BY `{column}`
                        ORDER BY cnt DESC
                        LIMIT 5
                    """
                    like_params = tuple(f"%{w}%" for w in words)
                    cursor.execute(all_words_sql, like_params)
                    all_words_rows = cursor.fetchall()
                    if all_words_rows:
                        word_matches["ALL_WORDS"] = [{"value": str(row[0]), "count": row[1]} for row in all_words_rows]

                # 개별 단어로도 검색
                for word in words:
                    word_sql = f"""
                        SELECT `{column}`, COUNT(*) as cnt
                        FROM `{table}`
                        WHERE `{column}` LIKE %s
                        GROUP BY `{column}`
                        ORDER BY cnt DESC
                        LIMIT 5
                    """
                    cursor.execute(word_sql, (f"%{word}%",))
                    word_rows = cursor.fetchall()
                    if word_rows:
                        word_matches[word] = [{"value": str(row[0]), "count": row[1]} for row in word_rows]

        # 3. Distinct 값 + 빈도수 조회 (상위 N개)
        values_sql = f"""
            SELECT `{column}`, COUNT(*) as cnt
            FROM `{table}`
            WHERE `{column}` IS NOT NULL
            GROUP BY `{column}`
            ORDER BY cnt DESC
            LIMIT {limit}
        """
        cursor.execute(values_sql)
        rows = cursor.fetchall()

        values = [str(row[0]) for row in rows]
        sample_with_count = [{"value": str(row[0]), "count": row[1]} for row in rows]

        cursor.close()
        connection.close()

        return {
            "success": True,
            "table": table,
            "column": column,
            "search_term": search_term,
            "exact_match": exact_match,
            "similar_values": similar_values,
            "word_matches": word_matches,  # 개별 단어 매칭 결과
            "distinct_count": distinct_count,
            "values": values,
            "sample_with_count": sample_with_count,
            "error": None
        }

    except Error as e:
        return {
            "success": False,
            "table": table,
            "column": column,
            "distinct_count": 0,
            "values": [],
            "sample_with_count": [],
            "error": str(e)
        }


def format_lookup_result(result: Dict[str, Any]) -> str:
    """
    lookup 결과를 LLM이 읽기 쉬운 형태로 포맷팅
    """
    if not result["success"]:
        return f"❌ Error looking up {result['table']}.{result['column']}: {result['error']}"

    output = []
    search_term = result.get("search_term")

    # search_term이 있으면 존재 여부 먼저 표시
    if search_term:
        exact_match = result.get("exact_match", False)
        if exact_match:
            output.append(f"✅ FOUND: '{search_term}' exists in {result['table']}.{result['column']}")
            output.append(f"   You can use this value in your WHERE clause.")
        else:
            output.append(f"❌ NOT FOUND: '{search_term}' does NOT exist in {result['table']}.{result['column']}")

            # 유사값이 있으면 제안
            similar = result.get("similar_values", [])
            word_matches = result.get("word_matches", {})

            if similar:
                output.append(f"")
                output.append(f"🔍 Similar values containing '{search_term}':")
                for item in similar[:5]:
                    output.append(f"   → '{item['value']}' ({item['count']} rows)")
                output.append(f"")
                output.append(f"💡 Did you mean one of these?")
            elif word_matches:
                # ALL_WORDS 결과가 있으면 가장 먼저 표시 (가장 관련성 높음)
                if "ALL_WORDS" in word_matches:
                    output.append(f"")
                    output.append(f"🔍 Possible matches (contains all words):")
                    for item in word_matches["ALL_WORDS"][:3]:
                        output.append(f"   → '{item['value']}' ({item['count']} rows)")
                    output.append(f"")
                    output.append(f"💡 This might be what you're looking for. Check if the format matches (e.g., 'Last, First').")
                else:
                    # 개별 단어 매칭 결과 표시
                    output.append(f"")
                    output.append(f"🔍 Partial matches for individual words:")
                    for word, matches in word_matches.items():
                        output.append(f"   '{word}':")
                        for item in matches[:3]:
                            output.append(f"      → '{item['value']}' ({item['count']} rows)")
                    output.append(f"")
                    output.append(f"💡 No exact match. Consider one of the above values.")
            else:
                output.append(f"   ⚠️ No similar values found. Check the column name or table.")
        output.append("")

    # 상위 값들 표시
    output.append(f"📊 Top values in {result['table']}.{result['column']} (Total: {result['distinct_count']} distinct):")
    for item in result["sample_with_count"][:15]:
        output.append(f"   - '{item['value']}' ({item['count']} rows)")

    if result["distinct_count"] > len(result["values"]):
        output.append(f"   ... and {result['distinct_count'] - len(result['values'])} more values")

    return "\n".join(output)
