# src/utils/fragment_mapper.py
"""
Fragment to Schema Column Mapper
- Maps extracted SELECT fragments to actual schema columns
- Step 1: 1:1 direct mapping (exact match, hint match, partial match)
- Step 2: Aggregate/computed columns (future)
"""

import re
from typing import List, Dict, Optional, Tuple


def normalize_text(text: str) -> str:
    """Normalize text for comparison: lowercase, remove underscores/spaces"""
    return re.sub(r'[_\s]+', '', text.lower())


def extract_columns_from_schema(schema: str) -> Dict[str, List[str]]:
    """
    Parse schema string to extract table -> columns mapping.
    Handles m_schema format like:
    TABLE_NAME: COLUMN1 (type, examples), COLUMN2 (type, examples), ...
    """
    tables = {}
    current_table = None

    for line in schema.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Table header: "TABLE_NAME:" or "TABLE_NAME :"
        if ':' in line and not line.startswith(' ') and not line.startswith('-'):
            # Check if it's a table definition (not a column with type)
            parts = line.split(':')
            potential_table = parts[0].strip()

            # Skip if it looks like a column definition (has parentheses before colon)
            if '(' not in potential_table and potential_table.isupper():
                current_table = potential_table
                tables[current_table] = []

                # Rest of the line might have columns
                if len(parts) > 1:
                    col_part = ':'.join(parts[1:]).strip()
                    if col_part:
                        cols = parse_columns_from_line(col_part)
                        tables[current_table].extend(cols)
        elif current_table:
            # Continuation of columns
            cols = parse_columns_from_line(line)
            tables[current_table].extend(cols)

    return tables


def parse_columns_from_line(line: str) -> List[str]:
    """Parse column names from a schema line"""
    columns = []

    # Split by comma, but handle parentheses (for types and examples)
    depth = 0
    current = ""

    for char in line:
        if char == '(':
            depth += 1
            current += char
        elif char == ')':
            depth -= 1
            current += char
        elif char == ',' and depth == 0:
            col = extract_column_name(current.strip())
            if col:
                columns.append(col)
            current = ""
        else:
            current += char

    # Last column
    if current.strip():
        col = extract_column_name(current.strip())
        if col:
            columns.append(col)

    return columns


def extract_column_name(col_str: str) -> Optional[str]:
    """Extract column name from string like 'COLUMN_NAME (type, examples)'"""
    # Remove leading dashes or bullets
    col_str = re.sub(r'^[-•]\s*', '', col_str)

    # Get the part before parenthesis
    match = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)', col_str)
    if match:
        return match.group(1)
    return None


def map_fragments_to_columns(
    fragments: List[str],
    schema: str,
    mapping_hints: Dict[str, List[str]] = None,
    table_filter: List[str] = None
) -> Dict:
    """
    Map extracted fragments to schema columns.

    Args:
        fragments: List of extracted SELECT fragments
        schema: Schema string (m_schema format)
        mapping_hints: Optional mapping hints from data (phrase -> columns)
        table_filter: Optional list of tables to consider (gold_tables)

    Returns:
        {
            "mapped": [
                {"fragment": "building name", "column": "BUILDING_NAME", "table": "BUILDINGS", "match_type": "exact"},
                ...
            ],
            "unmapped": ["total count", ...],  # likely aggregates or complex
            "aggregates": ["total count", ...],  # detected aggregates
            "mapping_log": [...]  # detailed log for debugging
        }
    """
    # Parse schema
    all_tables = extract_columns_from_schema(schema)

    # Filter tables if specified
    if table_filter:
        tables = {t: cols for t, cols in all_tables.items() if t in table_filter}
    else:
        tables = all_tables

    # Build column lookup: normalized_name -> [(table, original_name), ...]
    column_lookup = {}
    for table, columns in tables.items():
        for col in columns:
            normalized = normalize_text(col)
            if normalized not in column_lookup:
                column_lookup[normalized] = []
            column_lookup[normalized].append((table, col))

    # Build hint lookup: normalized_phrase -> columns
    hint_lookup = {}
    if mapping_hints:
        for phrase, columns in mapping_hints.items():
            hint_lookup[normalize_text(phrase)] = columns

    mapped = []
    unmapped = []
    aggregates = []
    mapping_log = []

    # Aggregate keywords
    aggregate_patterns = [
        r'\b(count|total|sum|avg|average|min|max|number of|amount of)\b',
        r'\b(최대|최소|합계|평균|개수|총)\b'
    ]

    for fragment in fragments:
        fragment_lower = fragment.lower()
        fragment_normalized = normalize_text(fragment)

        log_entry = {
            "fragment": fragment,
            "attempts": [],
            "result": None
        }

        # Check if aggregate
        is_aggregate = any(re.search(p, fragment_lower) for p in aggregate_patterns)
        if is_aggregate:
            aggregates.append(fragment)
            log_entry["attempts"].append({"type": "aggregate_check", "matched": True})
            log_entry["result"] = {"type": "aggregate", "note": "Requires aggregate function"}
            mapping_log.append(log_entry)
            continue

        match_found = False

        # 1. Exact match with column name
        if fragment_normalized in column_lookup:
            matches = column_lookup[fragment_normalized]
            if len(matches) == 1:
                table, col = matches[0]
                mapped.append({
                    "fragment": fragment,
                    "column": col,
                    "table": table,
                    "match_type": "exact"
                })
                log_entry["attempts"].append({"type": "exact", "matched": True, "column": col, "table": table})
                log_entry["result"] = {"type": "mapped", "column": f"{table}.{col}", "match_type": "exact"}
                match_found = True
            else:
                # Multiple matches - ambiguous
                log_entry["attempts"].append({
                    "type": "exact",
                    "matched": "ambiguous",
                    "candidates": [f"{t}.{c}" for t, c in matches]
                })

        # 2. Hint-based match
        if not match_found and mapping_hints:
            for phrase, columns in mapping_hints.items():
                phrase_normalized = normalize_text(phrase)
                # Check if fragment matches the phrase
                if phrase_normalized == fragment_normalized or phrase_normalized in fragment_normalized:
                    if columns:
                        # Use first column from hint
                        col_ref = columns[0]
                        # Parse table.column format
                        if '.' in col_ref:
                            table, col = col_ref.split('.', 1)
                        else:
                            # Find table for this column
                            table = None
                            col = col_ref
                            for t, cols in tables.items():
                                if col in cols:
                                    table = t
                                    break

                        if table:
                            mapped.append({
                                "fragment": fragment,
                                "column": col,
                                "table": table,
                                "match_type": "hint",
                                "hint_phrase": phrase
                            })
                            log_entry["attempts"].append({
                                "type": "hint",
                                "matched": True,
                                "column": col,
                                "table": table,
                                "hint_phrase": phrase
                            })
                            log_entry["result"] = {"type": "mapped", "column": f"{table}.{col}", "match_type": "hint"}
                            match_found = True
                            break

        # 3. Partial match (fragment words in column name)
        if not match_found:
            # Split fragment into words
            words = re.findall(r'\w+', fragment_lower)
            best_match = None
            best_score = 0

            for normalized_col, table_cols in column_lookup.items():
                col_words = re.findall(r'\w+', normalized_col)

                # Count matching words
                matching_words = sum(1 for w in words if w in normalized_col or any(w in cw for cw in col_words))
                score = matching_words / max(len(words), 1)

                if score > best_score and score >= 0.5:  # At least 50% match
                    best_score = score
                    best_match = table_cols[0]  # Take first if multiple tables

            if best_match:
                table, col = best_match
                mapped.append({
                    "fragment": fragment,
                    "column": col,
                    "table": table,
                    "match_type": "partial",
                    "confidence": best_score
                })
                log_entry["attempts"].append({
                    "type": "partial",
                    "matched": True,
                    "column": col,
                    "table": table,
                    "confidence": best_score
                })
                log_entry["result"] = {"type": "mapped", "column": f"{table}.{col}", "match_type": "partial", "confidence": best_score}
                match_found = True
            else:
                log_entry["attempts"].append({"type": "partial", "matched": False})

        # Not matched
        if not match_found:
            unmapped.append(fragment)
            log_entry["result"] = {"type": "unmapped"}

        mapping_log.append(log_entry)

    return {
        "mapped": mapped,
        "unmapped": unmapped,
        "aggregates": aggregates,
        "mapping_log": mapping_log,
        "stats": {
            "total_fragments": len(fragments),
            "mapped_count": len(mapped),
            "unmapped_count": len(unmapped),
            "aggregate_count": len(aggregates)
        }
    }


def format_mapping_result(result: Dict) -> str:
    """Format mapping result for logging/display"""
    lines = ["Fragment → Column Mapping:"]

    # Mapped
    if result["mapped"]:
        lines.append(f"  ✓ Mapped ({len(result['mapped'])}):")
        for m in result["mapped"]:
            match_info = f"[{m['match_type']}]"
            if m.get('confidence'):
                match_info += f" ({m['confidence']:.0%})"
            lines.append(f"    - \"{m['fragment']}\" → {m['table']}.{m['column']} {match_info}")

    # Aggregates
    if result["aggregates"]:
        lines.append(f"  ∑ Aggregates ({len(result['aggregates'])}):")
        for a in result["aggregates"]:
            lines.append(f"    - \"{a}\" (requires aggregate function)")

    # Unmapped
    if result["unmapped"]:
        lines.append(f"  ? Unmapped ({len(result['unmapped'])}):")
        for u in result["unmapped"]:
            lines.append(f"    - \"{u}\"")

    return "\n".join(lines)


def format_mapping_hint(result: Dict) -> str:
    """Format mapping result as a hint for the prompt"""
    if not result or not result.get("mapped"):
        return ""

    lines = ["SELECT Column Mapping (use these exact columns):"]

    for m in result["mapped"]:
        lines.append(f"  - \"{m['fragment']}\" → {m['table']}.{m['column']}")

    # Note about aggregates
    if result.get("aggregates"):
        lines.append("  Aggregates (apply appropriate function):")
        for a in result["aggregates"]:
            lines.append(f"    - \"{a}\"")

    return "\n".join(lines)
