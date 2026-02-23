"""
NLQ를 보고 SQL 구조 요소(G, CASE, SQ, CTE, W, U, H) 필요 여부 예측
LLM baseline 실험
"""

import json
import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI()

ELEMENTS = ['G', 'SQ', 'CTE', 'H', 'U', 'CASE', 'W']

ELEMENT_DESC = {
    'G': 'GROUP BY - 데이터를 그룹화하여 집계',
    'SQ': 'Subquery - SELECT 안에 SELECT (서브쿼리)',
    'CTE': 'CTE - WITH 절 사용 (Common Table Expression)',
    'H': 'HAVING - GROUP BY 결과에 조건 필터',
    'U': 'UNION - 여러 쿼리 결과 합치기',
    'CASE': 'CASE WHEN - 조건부 값 변환',
    'W': 'WINDOW - OVER, PARTITION BY, RANK 등 윈도우 함수'
}


def predict_elements(nlq: str) -> dict:
    """LLM으로 NLQ에서 필요한 SQL 구조 요소 예측"""

    prompt = f"""You are a SQL expert. Given the natural language question below, predict which SQL structural elements would be needed to answer it.

Question: "{nlq}"

For each element, answer 1 (needed) or 0 (not needed):
- G (GROUP BY): grouping data for aggregation like "per department", "for each building"
- SQ (Subquery): nested SELECT inside another SELECT
- CTE (WITH clause): Common Table Expression for complex queries
- H (HAVING): filtering after GROUP BY, like "having more than 5"
- U (UNION): combining results from multiple queries
- CASE (CASE WHEN): conditional value transformation
- W (WINDOW): window functions like RANK, ROW_NUMBER, running totals, "within each group"

Return ONLY a JSON object like this, no explanation:
{{"G": 0, "SQ": 0, "CTE": 0, "H": 0, "U": 0, "CASE": 0, "W": 0}}"""

    response = client.chat.completions.create(
        model="gpt-5.2-2025-12-11",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_completion_tokens=100
    )

    content = response.choices[0].message.content.strip()

    try:
        # JSON 파싱
        if '```' in content:
            content = content.split('```')[1].replace('json', '').strip()
        result = json.loads(content)
        return {e: result.get(e, 0) for e in ELEMENTS}
    except:
        print(f"Parse error: {content}")
        return {e: 0 for e in ELEMENTS}


def parse_gold_elements(structure: str) -> dict:
    """Gold structure에서 각 요소 추출"""
    if structure == 'SIMPLE':
        elems = set()
    else:
        elems = set(structure.split('_'))

    # AGG는 G로 매핑 (AGG_G에서 G만 체크)
    return {e: 1 if e in elems else 0 for e in ELEMENTS}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=None, help='Test first N samples')
    parser.add_argument('--output', default='analysis/structure_prediction_results.json')
    args = parser.parse_args()

    # 데이터 로드
    with open('outputs/sql_skeletons.json', encoding='utf-8') as f:
        data = json.load(f)['results']

    if args.n:
        data = data[:args.n]

    results = []
    element_stats = {e: {'tp': 0, 'fp': 0, 'tn': 0, 'fn': 0} for e in ELEMENTS}

    print(f"Processing {len(data)} samples...")

    for i, item in enumerate(data):
        nlq = item['question']
        gold_structure = item.get('structure', 'SIMPLE')
        gold = parse_gold_elements(gold_structure)

        print(f"[{i+1}/{len(data)}] {nlq[:50]}...")

        pred = predict_elements(nlq)

        # 통계 업데이트
        for e in ELEMENTS:
            g, p = gold[e], pred[e]
            if g == 1 and p == 1:
                element_stats[e]['tp'] += 1
            elif g == 0 and p == 1:
                element_stats[e]['fp'] += 1
            elif g == 0 and p == 0:
                element_stats[e]['tn'] += 1
            else:  # g == 1 and p == 0
                element_stats[e]['fn'] += 1

        results.append({
            'idx': item['idx'],
            'question': nlq,
            'gold_structure': gold_structure,
            'gold': gold,
            'pred': pred
        })

    # 결과 계산
    print("\n=== Results ===")
    print(f"| Element | Gold+ | Pred+ | TP | FP | FN | Precision | Recall | F1 |")
    print(f"|---------|-------|-------|----|----|----|-----------|---------|----|")

    summary = {}
    for e in ELEMENTS:
        s = element_stats[e]
        tp, fp, fn, tn = s['tp'], s['fp'], s['fn'], s['tn']
        gold_pos = tp + fn
        pred_pos = tp + fp
        precision = tp / pred_pos if pred_pos > 0 else 0
        recall = tp / gold_pos if gold_pos > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        print(f"| {e:7} | {gold_pos:5} | {pred_pos:5} | {tp:2} | {fp:2} | {fn:2} | {precision:.2f}      | {recall:.2f}    | {f1:.2f} |")

        summary[e] = {
            'gold_positive': gold_pos,
            'pred_positive': pred_pos,
            'tp': tp, 'fp': fp, 'fn': fn, 'tn': tn,
            'precision': precision,
            'recall': recall,
            'f1': f1
        }

    # 저장
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump({
            'summary': summary,
            'results': results
        }, f, indent=2, ensure_ascii=False)

    print(f"\nSaved to: {args.output}")


if __name__ == '__main__':
    main()
