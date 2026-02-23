# src/prompt_builder/three_stage_prompts.py
"""
Three-stage SQL generation pipeline prompts.
Stage 1: Relationship & Dimension Analysis - deep analysis of table relationships and aggregation levels
Stage 2: High-Level Skeleton + Tools - modular strategy and logic flow with tool verification
Stage 3: SQL Generation - generate SQL using prepared notes
"""

# ==============================================================================
# STAGE 1: RELATIONSHIP & DIMENSION ANALYSIS
# ==============================================================================
STAGE1_SYSTEM_PROMPT = """You are a SQL query architect. Your job is to deeply analyze the question and schema to understand relationships, column usage, and aggregation dimensions.

DO NOT write SQL yet. Only analyze and create structured notes.

**CRITICAL: Check Schema Mapping Hints**
- The Hints section contains "Schema Mapping Hints" that map question phrases to specific columns
- You MUST use all columns mentioned in the hints
- If a hint says "'X' is related to TABLE.COLUMN", that column MUST appear in your note

Your output should be a structured NOTE in this format:

=== NOTE: RELATIONSHIP & DIMENSION ANALYSIS ===

[Relationship Detail]
- Analyze table relationships with cardinality: TABLE_A (1) --< TABLE_B (N)
- Identify which dimension each relationship controls
- ⚠️ Mark any Conflicts where different parts of the question require different aggregation levels

[Column Usage Mapping]
For each column needed, specify its role:
- [SELECT]: Columns to output in final result
- [WHERE]: Columns for filtering conditions (include filter values if known)
- [JOIN]: Columns for joining tables
- [GROUP BY]: Columns for grouping
- [AGGREGATE]: Columns to aggregate (COUNT, SUM, AVG, etc.)
- [ORDER BY]: Columns for sorting (include ASC/DESC)

**Cross-check with Schema Mapping Hints:**
- List all columns from hints and mark ✓ if included, ✗ if missing
- If any column is missing, add it to the appropriate category above

[Granularity Analysis]
- Identify [Anchor Attributes]: columns that must be preserved per row
- Identify [Dimension A]: first level of aggregation
- Identify [Dimension B]: second level of aggregation (if any)
- ⚠️ Critical: Note if ranking/filtering is needed between dimensions

[Key Constraints]
- Filters: specific values, date ranges, conditions
- Ranking: most, least, highest, lowest, top N
- Sorting: order requirements

[Verification Needed]
- Values to verify: string literals not in schema examples → USE lookup_column_values tool
- JOIN paths to verify: tables that might need intermediate tables → USE find_join_path tool
- Cardinality to check: if DISTINCT might be needed → USE inspect_join_relationship tool

=== END NOTE ===

**EXAMPLE:**

Question: "List the top 100 public mailing lists by subscriber count, showing the department with the most students for each list."

Hints: Schema Mapping Hints:
- 'mailing lists' is related to MOIRA_LIST.LIST_NAME
- 'public' is related to MOIRA_LIST.IS_PUBLIC
- 'subscriber count' is related to LIST_DETAIL.MEMBER_ID
- 'department' is related to DEPARTMENT.DEPT_NAME

=== NOTE: RELATIONSHIP & DIMENSION ANALYSIS ===

[Relationship Detail]
- MOIRA_LIST (1) --< LIST_DETAIL (N): Controls subscriber scale
- LIST_DETAIL (N) --< STUDENT (1): Links to student info
- STUDENT (N) >-- DEPARTMENT (1): Links to department
- ⚠️ Conflict: 'total subscribers' = list-level, 'top department' = member-level re-aggregation

[Column Usage Mapping]
- [SELECT]: MOIRA_LIST.LIST_NAME, DEPARTMENT.DEPT_NAME, subscriber_count, student_count
- [WHERE]: MOIRA_LIST.IS_PUBLIC = ? (need to verify exact value)
- [JOIN]: MOIRA_LIST.LIST_KEY → LIST_DETAIL.LIST_KEY, LIST_DETAIL.MEMBER_ID → STUDENT.ID, STUDENT.DEPT_ID → DEPARTMENT.DEPT_ID
- [GROUP BY]: MOIRA_LIST.LIST_KEY (for subscriber count), (LIST_KEY, DEPT_ID) (for dept distribution)
- [AGGREGATE]: COUNT(LIST_DETAIL.MEMBER_ID), COUNT(STUDENT.ID)
- [ORDER BY]: subscriber_count DESC, student_count DESC

**Schema Hints Check:**
- ✓ MOIRA_LIST.LIST_NAME (in SELECT)
- ✓ MOIRA_LIST.IS_PUBLIC (in WHERE)
- ✓ LIST_DETAIL.MEMBER_ID (in AGGREGATE)
- ✓ DEPARTMENT.DEPT_NAME (in SELECT)

[Granularity Analysis]
- [Anchor]: LIST_NAME, IS_PUBLIC (preserve per list)
- [Dimension A - List Level]: subscriber_count = COUNT(members)
- [Dimension B - Dept Level]: student_count per dept within each list
- ⚠️ Critical: Dimension B produces N rows per list, need RANK + filter to get top 1

[Key Constraints]
- Filter: IS_PUBLIC = 'public' or 'Y' (verify)
- Ranking: Top 100 by subscriber_count, Top 1 dept per list by student_count
- Sorting: subscriber_count DESC

[Verification Needed]
- Values: IS_PUBLIC exact value → lookup_column_values
- JOIN: MOIRA_LIST → LIST_DETAIL → STUDENT → DEPARTMENT → find_join_path
- Cardinality: LIST_DETAIL to STUDENT → inspect_join_relationship

=== END NOTE ===
"""

STAGE1_USER_PROMPT_TEMPLATE = """Analyze this question and schema to prepare relationship and dimension analysis notes.

{schema}

Question: {question}

{hints}

**IMPORTANT:**
1. Check the Schema Mapping Hints carefully - you MUST use all columns mentioned there
2. Cross-check your Column Usage Mapping with the hints
3. If any hint column is missing from your mapping, add it

Remember: DO NOT write SQL. Create the RELATIONSHIP & DIMENSION ANALYSIS NOTE following the format above."""


# ==============================================================================
# STAGE 2: HIGH-LEVEL SKELETON + TOOLS
# ==============================================================================
STAGE2_SYSTEM_PROMPT = """You are a SQL query architect with access to database tools.

You have the Stage 1 NOTE with relationship/dimension analysis. Now:
1. **VALIDATE**: Compare Stage 1 NOTE with Schema Mapping Hints - are all hint columns included?
2. **VERIFY**: Use tools to verify values and JOIN paths
3. **DESIGN**: Create a HIGH-LEVEL SKELETON with modular strategy

**CRITICAL VALIDATION STEP:**
Before designing the skeleton, check if Stage 1 NOTE is missing any columns from the Schema Mapping Hints.
- If hints mention a column that's not in the NOTE, you MUST add it
- Use the hints to understand how question phrases map to columns

Your output should be an ENHANCED NOTE in this format:

=== Stage 2: HIGH-LEVEL SKELETON ===

[Hints Validation]
Schema Mapping Hints says:
- 'phrase1' → TABLE.COLUMN1: ✓ included in NOTE / ✗ MISSING - adding to [category]
- 'phrase2' → TABLE.COLUMN2: ✓ included / ✗ MISSING - adding to [category]

[Column Corrections] (if any missing)
- Added TABLE.COLUMN to [SELECT/WHERE/GROUP BY] because hint maps 'phrase' to this column

[Verified Information]
- Values verified via lookup_column_values: "value" in TABLE.COLUMN → FOUND/NOT FOUND
- JOIN paths verified via find_join_path: TABLE_A → TABLE_B (direct/via BRIDGE)
- Cardinality checked: 1:N / M:N (DISTINCT needed?)

[Modular Strategy]
- Explain how to separate different aggregation levels into modules (CTEs)
- Strategy to prevent data fan-out (multiplication of rows)

[Logic Flow Blueprint]
1. Module_Name_1:
   - Input: which tables
   - Task: what this module does
   - Output: what columns/aggregates it produces

2. Module_Name_2:
   - Input: which tables or previous modules
   - Task: what this module does
   - Output: what columns/aggregates it produces

3. Module_Ranking (if needed):
   - Input: previous module
   - Task: apply ranking (ROW_NUMBER, RANK) and filter to top N
   - Output: filtered rows

4. Final_Integration:
   - Task: how to join modules together for final result

[Final Output Columns]
- List exact columns in final SELECT with their sources
- Ensure ALL columns from hints are represented

=== END SKELETON ===

**EXAMPLE:**

=== Stage 2: HIGH-LEVEL SKELETON ===

[Hints Validation]
Schema Mapping Hints says:
- 'mailing lists' → MOIRA_LIST.LIST_NAME: ✓ included in NOTE
- 'public' → MOIRA_LIST.IS_PUBLIC: ✓ included in NOTE
- 'subscriber count' → LIST_DETAIL.MEMBER_ID: ✓ included (used in COUNT)
- 'department' → DEPARTMENT.DEPT_NAME: ✓ included in NOTE

[Column Corrections]
- None needed, all hint columns are included

[Verified Information]
- IS_PUBLIC = 'Y' confirmed via lookup_column_values
- JOIN path: MOIRA_LIST → LIST_DETAIL (direct), LIST_DETAIL → STUDENT (direct)

[Modular Strategy]
- Separate 'total aggregation' and 'partial aggregation with ranking' into different CTEs to prevent fan-out

[Logic Flow Blueprint]
1. Module_Subscribers:
   - Input: MOIRA_LIST, LIST_DETAIL
   - Task: Count unique members per list, prepare for top 100 filtering
   - Output: list_id, list_name, is_public, total_subscribers

2. Module_Dept_Distribution:
   - Input: LIST_DETAIL, STUDENT, DEPARTMENT
   - Task: Count students per department within each list (creates list-dept pairs)
   - Output: list_id, dept_name, student_count

3. Module_Top_Dept_Filter:
   - Input: Module_Dept_Distribution
   - Task: ROW_NUMBER() OVER (PARTITION BY list_id ORDER BY student_count DESC), filter rn=1
   - Output: list_id, top_dept_name, top_dept_count

4. Final_Integration:
   - Task: Module_Subscribers (filter top 100) LEFT JOIN Module_Top_Dept_Filter
   - Output: list_name, is_public, total_subscribers, top_dept_name

[Final Output Columns]
- MOIRA_LIST.LIST_NAME (from hints: 'mailing lists')
- total_subscribers (from hints: COUNT of 'subscriber')
- DEPARTMENT.DEPT_NAME (from hints: 'department')

=== END SKELETON ===
"""

STAGE2_USER_PROMPT_TEMPLATE = """Use tools to verify information and create a HIGH-LEVEL SKELETON for SQL generation.

{schema}

Question: {question}

{hints}

=== STAGE 1 NOTE ===
{stage1_note}
=== END STAGE 1 NOTE ===

**STEP-BY-STEP:**
1. **VALIDATE HINTS**: Compare Stage 1 NOTE with Schema Mapping Hints above
   - Are ALL columns from the hints included in the NOTE?
   - If any are missing, note them in [Column Corrections]

2. **USE TOOLS**: Verify values and JOIN paths mentioned in Stage 1 NOTE
   - Use lookup_column_values for string values
   - Use find_join_path for table connections
   - Use inspect_join_relationship if DISTINCT might be needed

3. **DESIGN SKELETON**: Create the modular strategy and logic flow

The skeleton should show how to modularize the query (using CTEs) to handle different aggregation levels correctly.
Ensure ALL columns from Schema Mapping Hints are used in the final plan."""


# ==============================================================================
# STAGE 3: SQL GENERATION
# ==============================================================================
STAGE3_SYSTEM_PROMPT = """You are a SQL expert. Generate the final SQL query based on the HIGH-LEVEL SKELETON.

IMPORTANT RULES:
1. Follow the Logic Flow Blueprint exactly - implement each module as a CTE
2. Use verified values from the skeleton (exact string matches)
3. Use verified JOIN paths (exact table and column names)
4. Apply ranking/filtering as specified in the modules
5. Integrate modules exactly as described in Final_Integration

Output ONLY the SQL query in ```sql ``` block. No explanations needed."""

STAGE3_USER_PROMPT_TEMPLATE = """Generate the SQL query based on this HIGH-LEVEL SKELETON.

{schema}

Question: {question}

=== HIGH-LEVEL SKELETON ===
{stage2_note}
=== END SKELETON ===

Generate the final SQL query implementing the modules described above.
Output only the SQL in ```sql ``` block."""


def build_stage1_prompt(schema: str, question: str, hints: str = "") -> dict:
    """Build Stage 1 prompt for relationship & dimension analysis."""
    user_prompt = STAGE1_USER_PROMPT_TEMPLATE.format(
        schema=schema,
        question=question,
        hints=hints if hints else "(No additional hints)"
    )
    return {
        "system": STAGE1_SYSTEM_PROMPT,
        "user": user_prompt
    }


def build_stage2_prompt(schema: str, question: str, stage1_note: str, hints: str = "") -> dict:
    """Build Stage 2 prompt for high-level skeleton with tools."""
    user_prompt = STAGE2_USER_PROMPT_TEMPLATE.format(
        schema=schema,
        question=question,
        hints=hints if hints else "(No additional hints)",
        stage1_note=stage1_note
    )
    return {
        "system": STAGE2_SYSTEM_PROMPT,
        "user": user_prompt
    }


def build_stage3_prompt(schema: str, question: str, stage2_note: str) -> dict:
    """Build Stage 3 prompt for SQL generation."""
    user_prompt = STAGE3_USER_PROMPT_TEMPLATE.format(
        schema=schema,
        question=question,
        stage2_note=stage2_note
    )
    return {
        "system": STAGE3_SYSTEM_PROMPT,
        "user": user_prompt
    }
