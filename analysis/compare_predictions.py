"""
4o vs 5.2 예측 비교 CSV 생성
"""
import json
import csv

ELEMENTS = ['G', 'SQ', 'CTE', 'H', 'U', 'CASE', 'W']

# 데이터 로드
with open('analysis/structure_prediction_results_4o.json', encoding='utf-8') as f:
    data_4o = json.load(f)['results']

with open('analysis/structure_prediction_results_5.2.json', encoding='utf-8') as f:
    data_52 = json.load(f)['results']

# CSV 생성
rows = []
for i, (r4o, r52) in enumerate(zip(data_4o, data_52)):
    row = {
        'idx': r4o['idx'],
        'question': r4o['question'][:50] + '...' if len(r4o['question']) > 50 else r4o['question'],
        'gold_structure': r4o['gold_structure']
    }

    # 전체 집합 정확도 체크
    all_correct_4o = True
    all_correct_52 = True

    for e in ELEMENTS:
        gold = r4o['gold'][e]
        pred_4o = r4o['pred'][e]
        pred_52 = r52['pred'][e]

        correct_4o = (gold == pred_4o)
        correct_52 = (gold == pred_52)

        if not correct_4o:
            all_correct_4o = False
        if not correct_52:
            all_correct_52 = False

        if correct_4o and correct_52:
            row[e] = '4o, 5.2'
        elif correct_4o and not correct_52:
            row[e] = '4o'
        elif not correct_4o and correct_52:
            row[e] = '5.2'
        else:
            row[e] = ''

    row['4o_all'] = 'O' if all_correct_4o else 'X'
    row['5.2_all'] = 'O' if all_correct_52 else 'X'

    rows.append(row)

# 저장
with open('analysis/prediction_comparison.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['idx', 'question', 'gold_structure', '4o_all', '5.2_all'] + ELEMENTS)
    writer.writeheader()
    writer.writerows(rows)

print("Saved to: analysis/prediction_comparison.csv")

# 통계 출력
print("\n=== Stats ===")
print(f"{'Element':<8} {'Both':>6} {'4o only':>8} {'5.2 only':>9} {'Neither':>8}")
print("-" * 42)
for e in ELEMENTS:
    both = sum(1 for r in rows if r[e] == '4o, 5.2')
    only_4o = sum(1 for r in rows if r[e] == '4o')
    only_52 = sum(1 for r in rows if r[e] == '5.2')
    neither = sum(1 for r in rows if r[e] == '')
    print(f"{e:<8} {both:>6} {only_4o:>8} {only_52:>9} {neither:>8}")

# 전체 집합 정확도
print("\n=== All Elements Correct ===")
correct_4o = sum(1 for r in rows if r['4o_all'] == 'O')
correct_52 = sum(1 for r in rows if r['5.2_all'] == 'O')
both_correct = sum(1 for r in rows if r['4o_all'] == 'O' and r['5.2_all'] == 'O')
print(f"4o: {correct_4o}/121 ({correct_4o/121*100:.1f}%)")
print(f"5.2: {correct_52}/121 ({correct_52/121*100:.1f}%)")
print(f"Both: {both_correct}/121 ({both_correct/121*100:.1f}%)")
