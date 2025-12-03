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


# ### SPIDER 용 DIN-SQL Prompt
# PROMPT_TEMPLATE = """### Complete SQLite SQL QUERY only and with no explanation
# ### SQLite SQL tables, with their properties:
# {schema}
# ### {question}"""