"""
각 Structure 패턴에 해당하는 SQL + NLQ를 LLM으로 생성하는 스크립트
"""

import json
import os
import sys
from openai import OpenAI

# 프로젝트 루트 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

client = OpenAI()

# Structure 패턴 목록 (주요 패턴만)
STRUCTURES = [
    "SIMPLE",       # 단순 조회
    "AGG_G",        # 집계 + GROUP BY
    "AGG_G_SQ",     # 집계 + GROUP BY + 서브쿼리
    "AGG_G_CTE",    # 집계 + GROUP BY + CTE
    "AGG_G_CASE",   # 집계 + GROUP BY + CASE
    "AGG_G_W",      # 집계 + GROUP BY + WINDOW
    "AGG_G_U",      # 집계 + GROUP BY + UNION
    "AGG_G_H",      # 집계 + GROUP BY + HAVING
    "AGG_G_SQ_CASE_W",  # 복합
]

# Structure 설명
STRUCTURE_DESC = {
    "SIMPLE": "단순 SELECT 조회 (집계, 서브쿼리, CTE 없음)",
    "AGG_G": "집계 함수(SUM, COUNT, AVG 등)와 GROUP BY 사용",
    "AGG_G_SQ": "집계 + GROUP BY + 서브쿼리 (SELECT 안에 SELECT)",
    "AGG_G_CTE": "집계 + GROUP BY + CTE (WITH 절 사용)",
    "AGG_G_CASE": "집계 + GROUP BY + CASE WHEN 조건문",
    "AGG_G_W": "집계 + GROUP BY + WINDOW 함수 (OVER, PARTITION BY)",
    "AGG_G_U": "집계 + GROUP BY + UNION (결과 합치기)",
    "AGG_G_H": "집계 + GROUP BY + HAVING (집계 조건)",
    "AGG_G_SQ_CASE_W": "집계 + GROUP BY + 서브쿼리 + CASE + WINDOW (복합)",
}


def load_schema(schema_path: str = "data/beaver/dw/tables.json") -> str:
    """스키마 정보 로드"""
    with open(schema_path, 'r', encoding='utf-8') as f:
        tables = json.load(f)

    schema_text = []
    # dw DB 테이블만 필터
    for key, table in tables.items():
        if not key.startswith('dw#sep#'):
            continue
        table_name = table.get('table_name_original', '')
        columns = table.get('column_names_original', [])
        types = table.get('column_types', [])

        col_str = ', '.join([f"{c} ({types[i] if i < len(types) else '?'})" for i, c in enumerate(columns[:8])])
        if len(columns) > 8:
            col_str += f" ... (+{len(columns)-8} more)"
        schema_text.append(f"- {table_name}: {col_str}")

    return '\n'.join(schema_text[:25])  # 상위 25개 테이블만


def generate_sql_for_structure(structure: str, schema: str, n: int = 5) -> list:
    """특정 structure에 맞는 SQL + NLQ 생성"""

    desc = STRUCTURE_DESC.get(structure, structure)

    prompt = f"""You are a SQL expert. Given the database schema below, generate {n} pairs of:
1. A natural language question (NLQ) in English
2. A valid SQL query that answers the question

The SQL must follow this structure pattern: **{structure}**
Pattern description: {desc}

Important constraints:
- Use ONLY tables and columns from the schema below
- SQL must be syntactically correct for MySQL
- Questions should be realistic business questions
- Each example should be different and meaningful

Schema:
{schema}

Output format (JSON array):
[
  {{"nlq": "What is...", "sql": "SELECT ..."}},
  ...
]

Generate exactly {n} examples:"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7,
        max_tokens=4000
    )

    content = response.choices[0].message.content

    # JSON 파싱
    try:
        # ```json ... ``` 제거
        if '```json' in content:
            content = content.split('```json')[1].split('```')[0]
        elif '```' in content:
            content = content.split('```')[1].split('```')[0]

        examples = json.loads(content.strip())
        return examples
    except json.JSONDecodeError as e:
        print(f"JSON parsing error for {structure}: {e}")
        print(f"Raw content: {content[:500]}")
        return []


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--structures', nargs='+', default=STRUCTURES, help='Structure patterns to generate')
    parser.add_argument('--n', type=int, default=5, help='Number of examples per structure')
    parser.add_argument('--output', default='analysis/generated_sql_by_structure.json', help='Output file')
    args = parser.parse_args()

    print("Loading schema...")
    schema = load_schema()
    print(f"Schema loaded ({len(schema)} chars)")

    results = {}

    for structure in args.structures:
        print(f"\nGenerating for {structure}...")
        examples = generate_sql_for_structure(structure, schema, args.n)
        results[structure] = {
            'description': STRUCTURE_DESC.get(structure, ''),
            'examples': examples
        }
        print(f"  Generated {len(examples)} examples")

    # 저장
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to: {args.output}")

    # 요약 출력
    total = sum(len(r['examples']) for r in results.values())
    print(f"Total examples generated: {total}")


if __name__ == '__main__':
    main()
