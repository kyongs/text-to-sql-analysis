"""PK Level 문항별 확인 스크립트"""
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import json, yaml
sys.path.insert(0, '.')

from src.data_loader import BeaverLoader
from src.utils.pk_lookup import PKLookup
from src.utils.agg_hint import generate_agg_hint_from_item

with open('configs/beaver_dw_openai.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

loader = BeaverLoader(config)
data = loader.load_data(load_views=False)
lookup = PKLookup()

print(f'Total questions: {len(data)}')
print()

# Full distribution check
hint_count = 0
no_hint_count = 0
no_mapping_count = 0
has_groupby_with_hint = 0
has_groupby_no_hint = 0
max_level_seen = 0
hint_items = []

for idx, item in enumerate(data):
    mapping = item.get('mapping', {})
    gold_sql = item.get('sql', item.get('SQL', ''))
    has_groupby = 'GROUP BY' in gold_sql.upper()

    all_cols = []
    for cols in mapping.values():
        all_cols.extend(cols)

    if not all_cols:
        no_mapping_count += 1
        continue

    levels = lookup.get_relative_level(all_cols)
    hint = generate_agg_hint_from_item(item)
    max_l = max(levels.values()) if levels else 0
    max_level_seen = max(max_level_seen, max_l)

    if hint:
        hint_count += 1
        if has_groupby:
            has_groupby_with_hint += 1
        hint_items.append(idx)
    else:
        no_hint_count += 1
        if has_groupby:
            has_groupby_no_hint += 1

groupby_total = sum(1 for item in data if 'GROUP BY' in item.get('sql', item.get('SQL', '')).upper())

print("=== PK Level Distribution ===")
print(f"Total: {len(data)}")
print(f"No mapping: {no_mapping_count}")
print(f"With hint: {hint_count}")
print(f"Without hint: {no_hint_count}")
print(f"Max level seen: {max_level_seen}")
print()
print(f"GROUP BY queries total: {groupby_total}")
print(f"  - with hint: {has_groupby_with_hint}")
print(f"  - without hint: {has_groupby_no_hint}")
print()
print(f"Hint generated for indices: {hint_items}")
print()

# Show details for hint items
print("=== Items with Hints ===")
for idx in hint_items:
    item = data[idx]
    q = item['question'][:80]
    mapping = item.get('mapping', {})
    gold_sql = item.get('sql', item.get('SQL', ''))
    has_groupby = 'GROUP BY' in gold_sql.upper()

    all_cols = []
    for cols in mapping.values():
        all_cols.extend(cols)

    levels = lookup.get_relative_level(all_cols)
    hint = generate_agg_hint_from_item(item)

    print(f'[{idx}] {q}')
    print(f'  GROUP BY: {has_groupby}')
    print(f'  Levels: {levels}')
    for line in hint.split('\n'):
        print(f'  | {line}')
    print()

# Also show GROUP BY items WITHOUT hint (sample)
print("=== GROUP BY without Hint (sample) ===")
count = 0
for idx, item in enumerate(data):
    gold_sql = item.get('sql', item.get('SQL', ''))
    if 'GROUP BY' not in gold_sql.upper():
        continue
    if idx in hint_items:
        continue
    if count >= 5:
        break

    mapping = item.get('mapping', {})
    all_cols = []
    for cols in mapping.values():
        all_cols.extend(cols)
    levels = lookup.get_relative_level(all_cols)

    tables = set()
    for col in all_cols:
        if '.' in col:
            tables.add(col.split('.')[0])

    print(f'[{idx}] {item["question"][:80]}')
    print(f'  Tables: {tables}')
    print(f'  Levels: {levels}')
    # Check why no hierarchy detected
    for t in tables:
        pk = lookup.get_table_pk(t)
        if pk:
            canon = lookup.get_canonical_pk(pk)
            parent = lookup.pk_parents.get(pk) or lookup.pk_parents.get(canon)
            children = lookup.pk_children.get(pk, []) or lookup.pk_children.get(canon, [])
            print(f'    {t}: PK={pk}, canon={canon}, parent={parent}, children={children}')
        else:
            print(f'    {t}: No PK found!')
    print()
