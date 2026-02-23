"""
SQL 채점기 - 두 SQL의 실행 결과가 같은지 비교
"""
import os
import sys
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=os.getenv("MYSQL_PASSWORD", ""),
        database="dw"
    )

def execute_sql(sql: str, timeout: int = 30, with_columns: bool = False):
    """SQL 실행하고 결과 반환"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout * 1000}")
        cursor.execute(sql)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        cursor.close()
        conn.close()
        result = {"success": True, "results": results, "count": len(results)}
        if with_columns:
            result["columns"] = columns
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}

def compare_results(res1, res2) -> bool:
    """두 결과 집합이 같은지 비교 (순서 무관)"""
    if not res1["success"] or not res2["success"]:
        return False

    set1 = set(tuple(row) for row in res1["results"])
    set2 = set(tuple(row) for row in res2["results"])
    return set1 == set2


def analyze_mismatch(sql1: str, sql2: str) -> dict:
    """불일치 상세 분석 - 컬럼명, 예시 결과, 차이점"""
    res1 = execute_sql(sql1, with_columns=True)
    res2 = execute_sql(sql2, with_columns=True)

    analysis = {
        "sql1_ok": res1["success"],
        "sql2_ok": res2["success"],
    }

    if not res1["success"] or not res2["success"]:
        analysis["error"] = res1.get("error") or res2.get("error")
        return analysis

    cols1 = res1.get("columns", [])
    cols2 = res2.get("columns", [])
    rows1 = res1["results"]
    rows2 = res2["results"]

    analysis["sql1_columns"] = cols1
    analysis["sql2_columns"] = cols2
    analysis["sql1_count"] = len(rows1)
    analysis["sql2_count"] = len(rows2)

    # 컬럼 개수 비교
    if len(cols1) != len(cols2):
        analysis["column_count_diff"] = True
        analysis["message"] = f"Column count differs: {len(cols1)} vs {len(cols2)}"
    else:
        analysis["column_count_diff"] = False

        # 컬럼별 값 차이 분석 (첫 몇 행 기준)
        if rows1 and rows2:
            # 결과 집합으로 변환
            set1 = set(tuple(row) for row in rows1)
            set2 = set(tuple(row) for row in rows2)

            only_in_sql1 = set1 - set2
            only_in_sql2 = set2 - set1

            analysis["only_in_sql1_count"] = len(only_in_sql1)
            analysis["only_in_sql2_count"] = len(only_in_sql2)
            analysis["only_in_sql1_sample"] = list(only_in_sql1)[:3]
            analysis["only_in_sql2_sample"] = list(only_in_sql2)[:3]

            # 컬럼별 차이 분석 (동일 row count일 때)
            if len(rows1) == len(rows2) and len(cols1) == len(cols2):
                # 첫 행들 비교해서 어느 컬럼이 다른지 찾기
                diff_columns = set()
                sample_diffs = []

                def _sort_key(row):
                    return tuple((0, str(v)) if v is not None else (1, '') for v in row)
                sorted1 = sorted(rows1, key=_sort_key)
                sorted2 = sorted(rows2, key=_sort_key)

                for i, (r1, r2) in enumerate(zip(sorted1[:100], sorted2[:100])):
                    if r1 != r2:
                        for j, (v1, v2) in enumerate(zip(r1, r2)):
                            if v1 != v2:
                                diff_columns.add(cols1[j] if j < len(cols1) else f"col_{j}")
                                if len(sample_diffs) < 3:
                                    sample_diffs.append({
                                        "row": i,
                                        "column": cols1[j] if j < len(cols1) else f"col_{j}",
                                        "sql1_value": str(v1)[:50],
                                        "sql2_value": str(v2)[:50]
                                    })

                analysis["diff_columns"] = list(diff_columns)
                analysis["sample_diffs"] = sample_diffs

    # 예시 결과 (첫 3행)
    analysis["sql1_sample"] = rows1[:3] if rows1 else []
    analysis["sql2_sample"] = rows2[:3] if rows2 else []

    return analysis


def print_mismatch_analysis(sql1: str, sql2: str):
    """불일치 분석 결과 출력"""
    analysis = analyze_mismatch(sql1, sql2)

    print("\n" + "=" * 60)
    print("🔍 MISMATCH ANALYSIS")
    print("=" * 60)

    if not analysis.get("sql1_ok") or not analysis.get("sql2_ok"):
        print(f"❌ Error: {analysis.get('error')}")
        return

    print(f"\n📊 Row counts: SQL1={analysis['sql1_count']}, SQL2={analysis['sql2_count']}")
    print(f"📋 Columns SQL1: {analysis['sql1_columns']}")
    print(f"📋 Columns SQL2: {analysis['sql2_columns']}")

    if analysis.get("column_count_diff"):
        print(f"\n⚠️  {analysis['message']}")
    else:
        # 차이나는 컬럼
        if analysis.get("diff_columns"):
            print(f"\n🔴 Differing columns: {analysis['diff_columns']}")

        # 샘플 차이
        if analysis.get("sample_diffs"):
            print("\n📝 Sample differences:")
            for diff in analysis["sample_diffs"]:
                print(f"   Row {diff['row']}, {diff['column']}: '{diff['sql1_value']}' vs '{diff['sql2_value']}'")

        # SQL1에만 있는 행
        if analysis.get("only_in_sql1_count", 0) > 0:
            print(f"\n➕ Only in SQL1: {analysis['only_in_sql1_count']} rows")
            for row in analysis.get("only_in_sql1_sample", []):
                print(f"   {row}")

        # SQL2에만 있는 행
        if analysis.get("only_in_sql2_count", 0) > 0:
            print(f"\n➕ Only in SQL2: {analysis['only_in_sql2_count']} rows")
            for row in analysis.get("only_in_sql2_sample", []):
                print(f"   {row}")

    print("\n" + "-" * 60)

def grade(sql1: str, sql2: str, verbose: bool = True, analyze: bool = True) -> int:
    """두 SQL 비교하여 1(같음) or 0(다름) 반환"""
    res1 = execute_sql(sql1)
    res2 = execute_sql(sql2)

    if verbose:
        print(f"SQL1: {'OK' if res1['success'] else 'ERROR'} ({res1.get('count', 0)} rows)")
        print(f"SQL2: {'OK' if res2['success'] else 'ERROR'} ({res2.get('count', 0)} rows)")

        if not res1["success"]:
            print(f"  SQL1 Error: {res1['error']}")
        if not res2["success"]:
            print(f"  SQL2 Error: {res2['error']}")

    match = compare_results(res1, res2)

    if verbose:
        print(f"Result: {'MATCH' if match else 'MISMATCH'}")

        # 불일치 시 상세 분석
        if not match and analyze and res1["success"] and res2["success"]:
            print_mismatch_analysis(sql1, sql2)

    return 1 if match else 0

def main():
    import argparse
    parser = argparse.ArgumentParser(description="SQL Grader - Compare two SQL queries")
    parser.add_argument("--sql1", type=str, help="First SQL query")
    parser.add_argument("--sql2", type=str, help="Second SQL query")
    parser.add_argument("--file", type=str, help="File containing two SQLs separated by '==='")
    args = parser.parse_args()

    sql1 = args.sql1
    sql2 = args.sql2

    if args.file:
        with open(args.file, encoding='utf-8') as f:
            content = f.read()

        if '===' in content:
            parts = content.split('===')
            sql1 = parts[0].strip()
            sql2 = parts[1].strip()
        else:
            print("Error: File must contain two SQLs separated by '==='")
            return

    if not sql1 or not sql2:
        print("Usage: python sql_grader.py --sql1 'SELECT ...' --sql2 'SELECT ...'")
        print("   or: python sql_grader.py --file compare.sql")
        print("\nFile format (separate with '==='):")
        print("  SELECT * FROM table1")
        print("  ===")
        print("  SELECT * FROM table1")
        return

    print("=" * 50)
    print("SQL1:")
    print(sql1[:200] + "..." if len(sql1) > 200 else sql1)
    print()
    print("SQL2:")
    print(sql2[:200] + "..." if len(sql2) > 200 else sql2)
    print("=" * 50)

    result = grade(sql1, sql2)
    sys.exit(0 if result == 1 else 1)

if __name__ == "__main__":
    main()
