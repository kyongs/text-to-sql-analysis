# src/agent/sql_checker.py
"""
LLM이 SQL을 제출하기 전에 미리 실행하여 오류를 확인하는 도구
"""

import mysql.connector
from typing import Dict, Any, List, Optional
import traceback


def check_sql(sql: str, conn_info: Dict[str, Any], db_id: str, limit: int = 5) -> str:
    """
    SQL을 실행하여 오류 여부와 결과 샘플을 반환합니다.
    
    Args:
        sql: 실행할 SQL 쿼리
        conn_info: DB 연결 정보
        db_id: 데이터베이스 ID
        limit: 반환할 최대 row 수
        
    Returns:
        실행 결과 또는 오류 메시지 (자연어 형식)
    """
    try:
        # DB 연결
        conn = mysql.connector.connect(
            host=conn_info.get('host', '127.0.0.1'),
            port=conn_info.get('port', 3306),
            user=conn_info.get('user', 'root'),
            password=conn_info.get('password', ''),
            database=db_id
        )
        cursor = conn.cursor(dictionary=True)
        
        # SQL 실행
        cursor.execute(sql)
        
        # 결과 가져오기
        results = cursor.fetchmany(limit)
        total_rows = cursor.rowcount
        
        cursor.close()
        conn.close()
        
        # 자연어로 결과 포맷팅
        if not results:
            # 0개 결과일 때 경고 메시지
            response = f"""
⚠️ ZERO ROWS RETURNED - POSSIBLE ISSUE

Your query executed without syntax errors, but returned 0 rows.

Result Summary:
- Total rows returned: 0
- Sample rows: (No rows returned)

⚠️ CRITICAL WARNINGS:
1. If the question asks to "list", "show", or "find" something, 0 rows is likely WRONG
2. Check if your WHERE conditions are too restrictive
3. Verify that the values you're filtering by actually exist in the database
4. Check if you're using the correct column names and table relationships

🔍 DEBUGGING STEPS:
1. Remove the WHERE clause and check if base tables have data
2. Use inspect_join_relationship to verify JOIN keys exist
3. Check for typos in column values (case sensitivity, spaces, etc.)
4. Try using LIKE with wildcards more carefully (e.g., '%value%' vs 'value%')

💡 RECOMMENDATION:
- DO NOT submit this query as-is unless you're CERTAIN the answer should be empty
- REVISE the query to investigate why no rows are returned
- Consider removing filters one by one to find which condition eliminates all rows
"""
        else:
            # 정상 결과일 때
            response = f"""
✅ SQL Execution Successful

Query executed without errors and returned data.

Result Summary:
- Total rows returned: {total_rows if total_rows >= 0 else 'Unknown (SELECT query)'}
- Sample rows (first {min(limit, len(results))} of potentially more):

"""
            # 컬럼명 출력
            columns = list(results[0].keys())
            response += f"  Columns: {', '.join(columns)}\n\n"
            
            # 샘플 데이터 출력
            for i, row in enumerate(results, 1):
                response += f"  Row {i}:\n"
                for col, val in row.items():
                    # 값이 너무 길면 자르기
                    val_str = str(val)
                    if len(val_str) > 100:
                        val_str = val_str[:100] + "..."
                    response += f"    {col}: {val_str}\n"
                response += "\n"
            
            response += """
💡 Recommendation:
- If the results look correct, you can submit this SQL as your final answer.
- If there are issues (wrong columns, unexpected row count, etc.), revise the query.
- For questions asking for counts or aggregations, verify the numbers match expectations.
"""
        
        return response
        
    except mysql.connector.Error as e:
        # SQL 실행 오류
        error_msg = f"""
❌ SQL Execution Error

The query failed with the following error:

Error Code: {e.errno}
Error Message: {e.msg}

SQL Query:
{sql}

💡 Common Issues:
- Syntax errors: Check for sql syntax mistakes
- Unknown column: Refer to the provided schema
- JOIN errors: Check join conditions and table relationships

Please revise your SQL query to fix the error before submitting.
"""
        return error_msg
        
    except Exception as e:
        # 기타 오류
        error_msg = f"""
❌ Unexpected Error

An unexpected error occurred:

{str(e)}

Stack trace:
{traceback.format_exc()}

Please revise your query or contact support if the issue persists.
"""
        return error_msg
