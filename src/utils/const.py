# src/utils/const.py

# This file contains constant strings for building prompts to keep the main logic clean.

# Detailed rules for the LLM to follow when generating SQL queries.
# RULES_SECTION_origin = """
# ***************************
# ###Rules###
# - You can have nested SQL, but the final answer must be a single SQL statement, not multiple.
# - Hints, if provided, is very important for correct column references and mathematical computation.
# - If the hints provide a mathematical computation, make sure you closely follow the mathematical compuation.
# - Column values/literals: Make sure that column values and literals are correct. Consider the column example values and hints provided.
# - Table Aliases: Use aliases to avoid duplicate table name conflicts.
# - Column References: Verify column names and use table_name.column_name format.
# - Functions: Use correct SQLite functions for the intended data types.
# - HAVING Clause: Employ boolean expressions (comparisons, AND, OR, NOT). Consider subqueries for top values.
# - Table Joins: Ensure table names are correct and use appropriate joins.
# - Arithmetic: Use basic operators (+, -, *, /) if dedicated functions are missing.
# - Put double quotations around column names and table names, especially when there is a space in between words.
# - Use double quotations for string literals.
# - A single quote within the string can be encoded by putting two single quotes in a row (''): "Men's basketball" should be "Men''s basketball"
# - When comparing string/text type in filter criteria, use LIKE operator and surround the text with wildcards %.
# - If the question doesn't specify exactly which columns to select, between name column and id column, prefer to select id column.
# - Never use || to concatenate columns in the SELECT. Rather output the columns as they are.
# ***************************"""

RULES_SECTION = """
###RULES###
Remember the following caution and do not make same mistakes:
-  If there are blank space between column name do not concatenate the words. Instead, use backtick(`) around the column name.
- Make sure that the column belongs to the corresponding table in given [Schema].
- When doing division, type cast numerical values into REAL type using 'CAST'.
- Avoid choosing columns that are not specifically requested in the question. 
- Return sqlite SQL query only without any explanation.
- Try to use DISTINCT if possible.
- Please make sure to verify that the table-column matches in the generated SQL statement are correct. Every column used must exist in the corresponding table.
- If there is no example of a column value, it likely indicates that the column is of a numeric type—please make sure to take this into account. 

"""


# PROMPT_TEMPLATE_origin = """\
# You are a SQLite SQL expert.
# Your job is to write a SQLite SQL query to answer the user's question.
# You need to understand the question in natural language and good understanding of the underlying database schema and structure to get it right.
# {db_name_str} structure is defined by the following table schemas.

# Given the "Table creation statements" and the "Question", you need understand the database and columns.
# Consider the natural language question to SQL query "Examples".

# {rules_mention}
# {rules_section}

# {schema}

# ###Question###
# {question}

# (Hints: {hints})

# Now generate SQLite SQL query to answer the given "Question" without any explanation.

# {final_instruction}
# """

PROMPT_TEMPLATE = """\
### System Instructions ###
You are an expert SQL developer.
You must generate an accurate and efficient {sql_dialect} query based on the provided database schema and natural language question.
Never use tables or columns that are not specified in the schema.
Do not add any explanations other than the final SQL query.
Now generate {sql_dialect} SQL query to answer the given "Question" without any explanation.

### Schema ###
{schema}

### Question ###
{question}

(Hints: {hints})

### Generated SQL Query ###
Generated SQL:
"""

# Tool-specific instructions
TOOL_INSTRUCTIONS = {
    'join_inspector': """- **inspect_join_relationship**: When you already know which columns to JOIN on, use this tool to verify:
  * Cardinality (1:1, 1:N, N:1, M:N) - prevents data multiplication bugs
  * Sample joined data - confirms keys actually match
  * Row counts - helps detect unexpected duplicates
  Required: table1, table2, join_key1, join_key2""",

    'join_path_finder': """- **find_join_path**: When you DON'T know how to join two tables, use this tool to discover:
  * Optimal JOIN path (including intermediate tables if needed)
  * Correct join keys between tables
  * Quality score for each path option
  Required: table1, table2""",

    'lookup_column_values': """- **lookup_column_values**: **MUST USE** this tool when:
  * The question mentions a category (role, department, status, type) but doesn't give the EXACT database value
  * You're about to write WHERE column = 'some string' without seeing that exact string in the schema examples
  * The column stores categorical data (names, types, statuses, roles, departments)
  * Even if schema shows example values, they may be incomplete - ALWAYS verify

  Required: table, column"""
}

def build_tool_guidelines(enabled_tools: list) -> str:
    """
    Dynamically build tool usage guidelines based on which tools are enabled.

    Args:
        enabled_tools: List of enabled tool names (e.g., ['join_inspector', 'join_path_finder'])

    Returns:
        Formatted tool usage guidelines string
    """
    if not enabled_tools:
        return ""

    guidelines = ["**IMPORTANT: Tool Usage Guidelines**"]
    guidelines.append("You have access to the following tools for JOIN analysis:\n")

    # Add instructions for each enabled tool
    for tool in enabled_tools:
        if tool in TOOL_INSTRUCTIONS:
            guidelines.append(TOOL_INSTRUCTIONS[tool])

    # Add workflow guidance based on tool combination
    guidelines.append("\n**Recommended Workflow:**")
    workflow_steps = []

    if 'lookup_column_values' in enabled_tools:
        workflow_steps.append("If your WHERE clause needs a string value (role, department, status, etc.) → Use `lookup_column_values` to find the exact value")

    if 'join_path_finder' in enabled_tools:
        workflow_steps.append("If unsure how to JOIN two tables → Use `find_join_path` to discover the path")

    if 'join_inspector' in enabled_tools:
        workflow_steps.append("Once you have JOIN keys → Use `inspect_join_relationship` to verify cardinality")

    workflow_steps.append("Write the final SQL query with validated information")

    for i, step in enumerate(workflow_steps, 1):
        guidelines.append(f"{i}. {step}")

    return "\n".join(guidelines)


# ### SPIDER 용 DIN-SQL Prompt
# PROMPT_TEMPLATE = """### Complete SQLite SQL QUERY only and with no explanation
# ### SQLite SQL tables, with their properties:
# {schema}
# ### {question}"""