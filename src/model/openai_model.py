# src/model/openai_model.py

import os
import re
import json
import threading
import mysql.connector
from openai import OpenAI
from typing import Dict, Any, List, Optional, Tuple


class OpenAIModel:
    """
    OpenAI 모델 클래스 - tool calling 기능 통합
    tool flag가 활성화되면 자동으로 tool calling 사용
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_config = config['model']
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set.")
        self.client = OpenAI(api_key=api_key)

        # DB 연결 정보 저장 (tool 호출 시 필요)
        self.conn_info = config.get('db_connection', {})
        if self.conn_info.get('password') == 'from_env':
            self.conn_info['password'] = os.getenv('MYSQL_PASSWORD', '')
        self.db_type = config['dataset'].get('db_type', 'sqlite')

        # 개별 tool 활성화 여부 (CLI argument에서 전달됨)
        enabled_tools = config.get('enabled_tools', {})
        self.enable_join_inspector = enabled_tools.get('join_inspector', False)
        self.enable_join_path_finder = enabled_tools.get('join_path_finder', False)
        self.enable_lookup_column_values = enabled_tools.get('lookup_column_values', False)
        self.enable_aggregation_advisor = enabled_tools.get('aggregation_advisor', False)
        self.enable_distinct_advisor = enabled_tools.get('distinct_advisor', False)
        self.enable_distinct_comparator = enabled_tools.get('distinct_comparator', False)
        self.enable_constraint_checker = enabled_tools.get('constraint_checker', False)
        self.enable_ask_granularity = enabled_tools.get('ask_granularity', False)
        self.enable_execute_sql = enabled_tools.get('execute_sql', False)

        # User Agent 설정 (ask_granularity에서 LLM2가 자동 선택)
        user_agent_config = config.get('user_agent', {})
        self.use_user_agent = user_agent_config.get('enabled', False)
        self.user_agent_model = user_agent_config.get('model', 'gpt-4o-mini')

        # 현재 처리 중인 gold_sql (thread-local storage for multi-threading safety)
        self._thread_local = threading.local()

        # Refine agent 활성화 여부
        refine_agents = config.get('refine_agents', {})
        self.enable_syntax_fixer = refine_agents.get('syntax_fixer', False)
        self.enable_empty_handler = refine_agents.get('empty_handler', False)
        self.max_refine_iterations = refine_agents.get('max_iterations', 1)

        # Note-taking 활성화 여부 (실제 인스턴스는 generate()에서 스레드 로컬로 생성)
        self.enable_note_taking = config.get('note_taking', False)

        # LLM Feedback 활성화 여부 (note_taking과 함께 사용)
        self.enable_llm_feedback = config.get('llm_feedback', False)

        # Rule-based Review 활성화 여부 (note_taking과 함께 사용)
        self.enable_rule_review = config.get('rule_review', False)

        # Fresh refine 활성화 여부 (note_taking과 함께 사용)
        self.enable_fresh_refine = config.get('fresh_refine', False)

        # Forced refine 활성화 여부 (생성 후 항상 실행되는 검증 단계)
        self.enable_forced_refine = config.get('forced_refine', False)

        # Tool 정의 (활성화된 tool만)
        self.tools = self._initialize_tools()
        self.use_tools = len(self.tools) > 0

    def _initialize_tools(self) -> List[Dict[str, Any]]:
        """Initialize tool definitions based on enabled flags."""
        tools = []

        # Add inspect_join_relationship if enabled
        if self.enable_join_inspector:
            tools.append({
                "type": "function",
                "function": {
                    "name": "inspect_join_relationship",
                    "description": "Analyze the relationship between two tables when joined. Returns cardinality (1:1, 1:N, N:1, M:N), row counts, and sample data. Use this before writing JOIN queries to understand data multiplication risks.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table1": {
                                "type": "string",
                                "description": "The first table name"
                            },
                            "table2": {
                                "type": "string",
                                "description": "The second table name"
                            },
                            "join_key1": {
                                "type": "string",
                                "description": "The column name in table1 used for joining"
                            },
                            "join_key2": {
                                "type": "string",
                                "description": "The column name in table2 used for joining"
                            }
                        },
                        "required": ["table1", "table2", "join_key1", "join_key2"]
                    }
                }
            })

        # Add find_join_path if enabled
        if self.enable_join_path_finder:
            tools.append({
                "type": "function",
                "function": {
                    "name": "find_join_path",
                    "description": "Find the optimal JOIN path between two tables. **IMPORTANT: Use this BEFORE joining tables that are not directly related.** Returns the shortest path including any necessary intermediate tables. Prevents errors from skipping required bridge tables.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table1": {
                                "type": "string",
                                "description": "The starting table name"
                            },
                            "table2": {
                                "type": "string",
                                "description": "The target table name"
                            }
                        },
                        "required": ["table1", "table2"]
                    }
                }
            })

        # Add lookup_column_values if enabled
        if self.enable_lookup_column_values:
            tools.append({
                "type": "function",
                "function": {
                    "name": "lookup_column_values",
                    "description": "Verify if a specific value exists in a database column. Use this tool ONLY when the string value you want to use in WHERE clause is NOT shown in the schema Examples. If the value is already in Examples, use it directly. If NOT FOUND, do NOT use that value - check the similar values returned or re-read the Hints.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table": {
                                "type": "string",
                                "description": "The table name to query"
                            },
                            "column": {
                                "type": "string",
                                "description": "The column name to check"
                            },
                            "search_term": {
                                "type": "string",
                                "description": "The exact literal value you want to use in WHERE clause. Example: If you plan to write WHERE department = 'Computer Science', then search_term should be 'Computer Science'. NOT the column name, NOT keywords from the question."
                            }
                        },
                        "required": ["table", "column", "search_term"]
                    }
                }
            })

        # Add check_aggregation_pattern if enabled
        if self.enable_aggregation_advisor:
            tools.append({
                "type": "function",
                "function": {
                    "name": "check_aggregation_pattern",
                    "description": "Analyze the question to determine whether to use GROUP BY or Window Function. **USE THIS FIRST** when the question asks for both individual details (names, titles, addresses) AND aggregated values (total, count, sum). Returns recommendation with confidence level and example SQL pattern.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The natural language question to analyze"
                            },
                            "tables": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of table names that will be used in the query"
                            }
                        },
                        "required": ["question", "tables"]
                    }
                }
            })

        # Add check_distinct_need if enabled
        if self.enable_distinct_advisor:
            tools.append({
                "type": "function",
                "function": {
                    "name": "check_distinct_need",
                    "description": "Analyze JOIN relationships to determine if DISTINCT is needed. **USE THIS** when joining multiple tables to check for duplicate row risks. Returns risk level (high/medium/low) and whether to use SELECT DISTINCT or COUNT(DISTINCT).",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tables": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of table names to be joined"
                            },
                            "join_pairs": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "left": {"type": "string", "description": "Left side of join: TABLE.COLUMN"},
                                        "right": {"type": "string", "description": "Right side of join: TABLE.COLUMN"}
                                    }
                                },
                                "description": "List of JOIN conditions, e.g., [{left: 'EMPLOYEE.DEPT_ID', right: 'DEPARTMENT.ID'}]"
                            },
                            "select_columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Columns to be selected (optional)"
                            }
                        },
                        "required": ["tables", "join_pairs"]
                    }
                }
            })

        # Add compare_distinct_results if enabled
        if self.enable_distinct_comparator:
            tools.append({
                "type": "function",
                "function": {
                    "name": "compare_distinct_results",
                    "description": "Compare query results WITH and WITHOUT DISTINCT. **USE THIS AFTER writing your SQL** to verify if DISTINCT is needed. Shows row count difference, duplicate ratio, and concrete duplicate examples. Helps decide whether to add/remove DISTINCT.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "The SQL query to test (with or without DISTINCT)"
                            }
                        },
                        "required": ["sql"]
                    }
                }
            })

        # Add check_schema_constraints if enabled
        if self.enable_constraint_checker:
            tools.append({
                "type": "function",
                "function": {
                    "name": "check_schema_constraints",
                    "description": "Verify schema constraints before writing SQL. Checks: (1) table/column existence, (2) PK/FK relationships, (3) column data types, (4) value domains for ENUM-like columns. Use this to validate your SQL plan.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tables": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of table names to check"
                            },
                            "columns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of columns to check (format: TABLE.COLUMN)"
                            }
                        },
                        "required": ["tables", "columns"]
                    }
                }
            })

        # Add ask_granularity if enabled
        if self.enable_ask_granularity:
            tools.append({
                "type": "function",
                "function": {
                    "name": "ask_granularity",
                    "description": """**PROACTIVELY USE THIS TOOL** whenever you need to write SQL with aggregation (COUNT, SUM, AVG, MAX, MIN, GROUP BY).

**ALWAYS ask the user** about their preferred result format BEFORE writing the SQL. Don't assume - let the user choose.

YOU generate 2-4 concrete options showing:
1. Different GROUP BY granularities (single column vs multiple columns)
2. WINDOW FUNCTION option if they might want detail rows WITH aggregates
3. Each option with clear result preview

**MUST USE when:**
- Writing COUNT(*), SUM(), AVG() or any aggregate function
- Question mentions "how many", "total", "count", "average", "per", "for each", "by"
- Multiple entities mentioned that could be grouping levels

Example: "List instructors and material count"
→ STOP and ask: GROUP BY instructor only? Or GROUP BY instructor, subject?

Options format:
[
  {"id": "A", "sql_pattern": "GROUP BY instructor", "description": "강사당 1행 - 총계만", "result_preview": "| instructor | total |\\n| Kim | 15 |"},
  {"id": "B", "sql_pattern": "GROUP BY instructor, subject", "description": "강사+과목 조합당 1행", "result_preview": "| instructor | subject | cnt |\\n| Kim | Math | 10 |"},
  {"id": "C", "sql_pattern": "COUNT(*) OVER (PARTITION BY instructor)", "description": "자료별 1행 + 강사별 합계 컬럼", "result_preview": "| material | instructor | inst_total |\\n| Book1 | Kim | 15 |"}
]""",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "The original natural language question"
                            },
                            "options": {
                                "type": "array",
                                "description": "Array of granularity options YOU generated",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "string", "description": "Option ID: A, B, C, etc."},
                                        "sql_pattern": {"type": "string", "description": "SQL pattern: GROUP BY x / GROUP BY x,y / SUM() OVER(...) / WITH ROLLUP"},
                                        "description": {"type": "string", "description": "What this option means in plain language"},
                                        "result_preview": {"type": "string", "description": "Example result table (use | for columns, \\n for rows)"}
                                    },
                                    "required": ["id", "sql_pattern", "description"]
                                }
                            },
                            "reason": {
                                "type": "string",
                                "description": "Why clarification is needed (what's ambiguous)"
                            }
                        },
                        "required": ["question", "options"]
                    }
                }
            })

        # Add execute_sql if enabled
        if self.enable_execute_sql:
            tools.append({
                "type": "function",
                "function": {
                    "name": "execute_sql",
                    "description": "Execute a SQL query against the database and return results. Use this to test your SQL before submitting the final answer. Returns: success/error status, row count, and first 5 rows of data. If error occurs, read the error message, fix the SQL, and try again.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "sql": {
                                "type": "string",
                                "description": "The SQL query to execute"
                            }
                        },
                        "required": ["sql"]
                    }
                }
            })

        return tools

    def _execute_tool_call(self, tool_name: str, arguments: Dict[str, Any], db_id: str) -> str:
        """Tool call 실행"""
        # Lazy import to avoid circular imports
        from src.agent.join_inspector import inspect_join_relationship
        from src.agent.join_path_finder import find_join_path
        from src.agent.column_value_lookup import lookup_column_values, format_lookup_result
        from src.agent.aggregation_advisor import check_aggregation_pattern, format_aggregation_advice
        from src.agent.distinct_advisor import check_distinct_need, format_distinct_advice
        from src.agent.distinct_comparator import compare_distinct_results, format_distinct_comparison
        from src.agent.constraint_checker import check_schema_constraints, format_constraint_check
        from src.agent.ask_granularity import ask_granularity

        if tool_name == "inspect_join_relationship":
            return inspect_join_relationship(
                table1=arguments["table1"],
                table2=arguments["table2"],
                join_key1=arguments["join_key1"],
                join_key2=arguments["join_key2"],
                conn_info=self.conn_info,
                db_id=db_id
            )
        elif tool_name == "find_join_path":
            return find_join_path(
                table1=arguments["table1"],
                table2=arguments["table2"],
                conn_info=self.conn_info,
                db_id=db_id
            )
        elif tool_name == "lookup_column_values":
            result = lookup_column_values(
                table=arguments["table"],
                column=arguments["column"],
                conn_info=self.conn_info,
                db_id=db_id,
                search_term=arguments.get("search_term")
            )
            return format_lookup_result(result)
        elif tool_name == "check_aggregation_pattern":
            result = check_aggregation_pattern(
                question=arguments["question"],
                tables=arguments.get("tables", []),
                conn_info=self.conn_info,
                db_id=db_id
            )
            return format_aggregation_advice(result)
        elif tool_name == "check_distinct_need":
            result = check_distinct_need(
                tables=arguments.get("tables", []),
                join_pairs=arguments.get("join_pairs", []),
                select_columns=arguments.get("select_columns", []),
                conn_info=self.conn_info,
                db_id=db_id
            )
            return format_distinct_advice(result)
        elif tool_name == "compare_distinct_results":
            result = compare_distinct_results(
                sql=arguments["sql"],
                conn_info=self.conn_info,
                db_id=db_id
            )
            return format_distinct_comparison(result)
        elif tool_name == "check_schema_constraints":
            result = check_schema_constraints(
                tables=arguments.get("tables", []),
                columns=arguments.get("columns", []),
                conn_info=self.conn_info,
                db_id=db_id
            )
            return format_constraint_check(result)
        elif tool_name == "ask_granularity":
            return ask_granularity(
                question=arguments["question"],
                options=arguments["options"],
                reason=arguments.get("reason", ""),
                gold_sql=getattr(self._thread_local, 'gold_sql', None),
                use_user_agent=self.use_user_agent,
                user_agent_model=self.user_agent_model
            )
        elif tool_name == "execute_sql":
            result = self._execute_sql(arguments["sql"], db_id)
            return self._format_execute_sql_result(result)
        else:
            return f"Unknown tool: {tool_name}"

    def generate(self, prompt: str, db_id: str = "dw", max_iterations: int = 10, question: str = None, item: Dict[str, Any] = None, gold_sql: str = None):
        """
        OpenAI API를 호출하고 필요시 tool calling 수행
        Refine agent가 활성화된 경우 SQL 실행 후 자동 수정 루프 실행
        Note-taking이 활성화된 경우 iter별 NOTE 관리

        Args:
            prompt: 사용자 프롬프트
            db_id: 데이터베이스 ID
            max_iterations: 최대 tool call 반복 횟수
            question: 원본 질문 (refine agent에서 사용)
            item: 데이터셋 아이템 (note_taking에서 hints 비교용)
            gold_sql: 정답 SQL (User Agent 모드에서 ask_granularity tool이 사용)

        Returns:
            response 객체 (tool 사용 시 tool_call_log 포함)
        """
        # User Agent 모드에서 사용할 gold_sql 저장 (thread-local)
        self._thread_local.gold_sql = gold_sql
        # Note-taking 초기화 (각 호출마다 새로운 NoteTaker 생성 - 멀티스레드 안전)
        local_note_taker = None
        if self.enable_note_taking and item:
            import sys
            from pathlib import Path
            src_dir = Path(__file__).parent.parent
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            from note_taker import ParsingNoteTaker
            skeleton_hint = item.get('skeleton_hint') if item else None
            local_note_taker = ParsingNoteTaker(item, skeleton_hint=skeleton_hint)

        # Tool이 있으면 상세 시스템 메시지 (활성화된 tool에 따라 동적 생성)
        if self.use_tools:
            # execute_sql 전용 모드: execute_sql만 활성화된 경우 전용 프롬프트 사용
            only_execute_sql = self.enable_execute_sql and not any([
                self.enable_join_inspector, self.enable_join_path_finder,
                self.enable_lookup_column_values, self.enable_aggregation_advisor,
                self.enable_distinct_advisor, self.enable_distinct_comparator,
                self.enable_constraint_checker, self.enable_ask_granularity
            ])

            if only_execute_sql:
                system_message = """You are a MySQL SQL expert. You have access to the execute_sql tool which runs SQL against the real database and returns results (row count + first 5 rows) or error details.

## How to use execute_sql
1. **Check values first**: When filtering by a string (WHERE col = '...'), run SELECT DISTINCT col FROM table to verify the exact value exists
2. **Verify before submitting**: Run your final query with LIMIT 100 and check that the columns and values match what the question asks for. If not, fix and re-run
3. **Multi-granularity aggregation**: When the question needs both detail rows and aggregated values (or aggregation at a different grain than the detail), use CTE or subquery to pre-compute the aggregation, then JOIN back. Execute the intermediate CTE/subquery first to verify correctness before writing the full query

## SQL Guidelines
- Prefer INNER JOIN over LEFT JOIN unless the question explicitly requires all rows from one side
- Only SELECT columns that the question asks for. Grouping keys used solely for aggregation do NOT need to appear in SELECT
- Follow the Hints/Mappings faithfully: use the exact columns specified rather than inventing alternatives (e.g., use COUNT(DISTINCT mapped_column) instead of COUNT(*) when a specific column is mapped)
- Do NOT fabricate or transform values beyond what is asked. If the question says "list names", return the name column as-is. Do NOT add COALESCE, IFNULL, or NULL-to-0 conversions unless explicitly requested
- Hints are strong guidance. Follow them closely, but adapt when the data tells you otherwise
"""
            else:
                system_parts = ["You are a MySQL SQL expert. Your job is to write a MySQL SQL query to answer the user's question.\n"]
                system_parts.append("You have access to tools that help you write better SQL:\n")

                tool_num = 1
                if self.enable_join_path_finder:
                    system_parts.append(f"""{tool_num}. **find_join_path**: Find the optimal JOIN path between two tables
   - **USE THIS FIRST** when you need to join tables that might not be directly related
   - Returns the shortest path including any necessary intermediate (bridge) tables
   - **CRITICAL**: Do NOT skip intermediate tables - each hop is required for data integrity
""")
                    tool_num += 1

                if self.enable_join_inspector:
                    system_parts.append(f"""{tool_num}. **inspect_join_relationship**: Analyze JOIN relationships between tables
   - Check cardinality (1:1, 1:N, M:N) before writing JOIN queries
   - Identify potential data multiplication issues
""")
                    tool_num += 1

                if self.enable_lookup_column_values:
                    system_parts.append(f"""{tool_num}. **lookup_column_values**: Verify exact column values before using in WHERE clause
   - **USE THIS** when you need to filter by a string value (department, role, status, type, name)
   - If the exact value is NOT shown in schema Examples, ALWAYS verify it exists first
   - Returns whether the value exists + similar values if not found
   - **CRITICAL**: If NOT FOUND, do NOT use that value - check similar values or re-read hints
""")
                    tool_num += 1

                if self.enable_aggregation_advisor:
                    system_parts.append(f"""{tool_num}. **check_aggregation_pattern**: Determine GROUP BY vs Window Function
   - **USE THIS FIRST** when the question asks for BOTH detail columns (names, titles, ISBN) AND aggregated values (total, count, sum)
   - Returns whether to use GROUP BY or Window Function with example pattern
   - **CRITICAL**: If it recommends Window Function, use SUM/COUNT(...) OVER (PARTITION BY ...) instead of GROUP BY
""")
                    tool_num += 1

                if self.enable_distinct_advisor:
                    system_parts.append(f"""{tool_num}. **check_distinct_need**: Check if DISTINCT is needed for JOIN queries
   - **USE THIS** when joining multiple tables to check duplicate row risks
   - Returns risk level (high/medium/low) based on JOIN cardinality analysis
   - **CRITICAL**: If risk is HIGH (M:N relationship), use SELECT DISTINCT or COUNT(DISTINCT ...)
""")
                    tool_num += 1

                if self.enable_distinct_comparator:
                    system_parts.append(f"""{tool_num}. **compare_distinct_results**: Compare results WITH vs WITHOUT DISTINCT
   - **USE THIS AFTER writing SQL** to verify if DISTINCT actually changes the result
   - Shows: row count difference, duplicate ratio, concrete duplicate examples
   - If no difference (0 duplicates), you can safely omit DISTINCT
   - If high duplicate ratio, DISTINCT is likely needed
""")
                    tool_num += 1

                if self.enable_constraint_checker:
                    system_parts.append(f"""{tool_num}. **check_schema_constraints**: Verify schema constraints
   - Check if tables/columns exist before using them
   - Get PK/FK relationships for correct JOIN conditions
   - Get data types (DATE, TIMESTAMP) for proper comparisons
   - Get allowed values for ENUM-like columns
""")
                    tool_num += 1

                if self.enable_ask_granularity:
                    system_parts.append(f"""{tool_num}. **ask_granularity**: Clarify aggregation granularity with the user
   - **PROACTIVELY USE** when writing SQL with COUNT, SUM, AVG, GROUP BY
   - Generate 2-4 options showing different GROUP BY levels or Window Functions
   - Wait for user selection before writing the final SQL
   - **MUST USE** when question mentions "how many", "total", "count", "for each", "per"
""")
                    tool_num += 1

                if self.enable_execute_sql:
                    system_parts.append(f"""{tool_num}. **execute_sql**: Execute SQL against the database to test your query
   - **USE THIS** after writing your SQL to verify it runs correctly
   - Returns row count and sample data (first 5 rows)
   - If error occurs, read the error message, fix the SQL, and try again
   - If result is empty (0 rows), reconsider your WHERE conditions or JOIN logic
""")
                    tool_num += 1

                system_parts.append("""When writing SQL queries:
- **Multi-hop JOINs**: If find_join_path shows intermediate tables, you MUST include ALL of them in your query
- **DISTINCT usage**: If the tool shows M:N (many-to-many) cardinality, consider using SELECT DISTINCT or COUNT(DISTINCT ...) to avoid duplicate rows
- **JOIN type selection**: Logically determine whether to use INNER JOIN or LEFT JOIN based on:
  * Whether you need all rows from the left table (LEFT JOIN) or only matching rows (INNER JOIN)
  * The cardinality information from the tool
  * The business logic of the question
- **GROUP BY optimization**: For M:N relationships, use GROUP BY with appropriate aggregate functions (COUNT DISTINCT, MAX, MIN, etc.)
""")
                system_message = "\n".join(system_parts)
        else:
            system_message = "You are a SQLite SQL expert. Your job is to write a SQLite SQL query to answer the user's question."

        if self.db_type == 'mysql':
            system_message = system_message.replace("SQLite", "MySQL")

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt}
        ]

        tool_call_log = []  # Tool call 중간 과정 로깅

        try:
            for iteration in range(max_iterations):
                # API 호출 - tools 리스트가 비어있지 않으면 tool calling 활성화
                if self.use_tools:
                    response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=messages,
                        tools=self.tools,
                        tool_choice="auto",
                        temperature=0
                    )
                else:
                    response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=messages,
                        temperature=0
                    )

                response_message = response.choices[0].message

                # Tool call이 없으면 → Final SQL로 간주
                if not response_message.tool_calls:
                    final_content = response_message.content

                    # 최종 응답 로깅
                    tool_call_log.append({
                        "iteration": iteration + 1,
                        "type": "final_response",
                        "content": final_content
                    })

                    # SQL 추출
                    sql = self._extract_sql_from_response(final_content)

                    # Note-taking이 활성화되어 있으면 iter별 NOTE 루프 사용
                    if self.enable_note_taking and local_note_taker and sql:
                        # iter별 NOTE 루프
                        note_iter = 1
                        max_note_iterations = self.max_refine_iterations + 1  # refine 횟수 + 1

                        for note_iter in range(1, max_note_iterations + 1):
                            # SQL 실행 (refine agent 활성화 여부와 관계없이)
                            exec_result = self._execute_sql(sql, db_id)

                            # LLM Feedback 요청 (활성화된 경우)
                            # 반환값: (피드백, 확신도) 또는 None
                            llm_feedback_result = None
                            llm_feedback_text = None
                            llm_confidence = 0
                            if self.enable_llm_feedback and question and item:
                                current_note_for_feedback = local_note_taker.get_current_note() if local_note_taker.iter_notes else None
                                llm_feedback_result = self._get_llm_feedback(sql, question, item, current_note_for_feedback)
                                if llm_feedback_result:
                                    llm_feedback_text, llm_confidence = llm_feedback_result

                            # NOTE에 iter 기록 추가 (llm_feedback은 텍스트만 저장)
                            local_note_taker.add_iter_note(
                                note_iter, sql, exec_result,
                                f"[확신도: {llm_confidence}] {llm_feedback_text}" if llm_feedback_text else None,
                                question=question, use_rule_review=self.enable_rule_review
                            )

                            # 로깅 (확신도 포함)
                            tool_call_log.append({
                                "iteration": f"note_iter_{note_iter}",
                                "type": "note_taking_iter",
                                "sql": sql,
                                "exec_result": {
                                    "success": exec_result["success"],
                                    "row_count": exec_result["row_count"],
                                    "error_type": exec_result.get("error_type")
                                },
                                "schema_check": local_note_taker.iter_notes[-1]["schema_check"],
                                "refine_feedback": local_note_taker.iter_notes[-1]["refine_feedback"],
                                "rule_review": local_note_taker.iter_notes[-1].get("rule_review"),
                                "llm_feedback": llm_feedback_text,
                                "llm_confidence": llm_confidence
                            })

                            # 성공이고 문제없으면 종료
                            # LLM Feedback은 확신도 4 이상일 때만 refine 트리거
                            has_llm_issues = llm_confidence >= 4
                            if exec_result["success"] and exec_result["row_count"] > 0 and not local_note_taker.has_issues() and not has_llm_issues:
                                break

                            # 마지막 iter면 종료
                            if note_iter >= max_note_iterations:
                                break

                            # 문제가 있으면 NOTE와 함께 refine 요청
                            issues_summary = local_note_taker.get_issues_summary()
                            current_note = local_note_taker.get_current_note()

                            # LLM Feedback이 있고 확신도 4 이상이면 issues에 추가
                            if llm_feedback_text and llm_confidence >= 4:
                                if issues_summary:
                                    issues_summary += f"\n\n[LLM Review (확신도: {llm_confidence})]\n{llm_feedback_text}"
                                else:
                                    issues_summary = f"[LLM Review (확신도: {llm_confidence})]\n{llm_feedback_text}"

                            # Refine prompt 생성 (NOTE 포함)
                            note_refine_prompt = f"""{current_note}

위 NOTE를 참고하여 SQL을 수정해주세요.
특히 다음 사항을 확인해주세요:
{issues_summary if issues_summary else "- 특별한 문제 없음"}

현재 SQL:
```sql
{sql}
```

수정된 SQL을 제공해주세요."""

                            # Fresh refine: 메시지 초기화하고 원본 prompt + NOTE만 사용
                            if self.enable_fresh_refine:
                                fresh_refine_prompt = f"""{prompt}

{current_note}

위 NOTE를 참고하여 SQL을 작성해주세요.
특히 다음 사항을 확인해주세요:
{issues_summary if issues_summary else "- 특별한 문제 없음"}

수정된 SQL을 제공해주세요."""
                                messages = [
                                    {"role": "system", "content": system_message},
                                    {"role": "user", "content": fresh_refine_prompt}
                                ]
                            else:
                                # 기존 방식: 메시지 누적
                                messages.append(response_message)
                                messages.append({
                                    "role": "user",
                                    "content": note_refine_prompt
                                })

                            # 재생성
                            if self.use_tools:
                                response = self.client.chat.completions.create(
                                    model=self.model_config['name'],
                                    messages=messages,
                                    tools=self.tools,
                                    tool_choice="auto",
                                    temperature=0
                                )
                            else:
                                response = self.client.chat.completions.create(
                                    model=self.model_config['name'],
                                    messages=messages,
                                    temperature=0
                                )

                            response_message = response.choices[0].message
                            new_sql = self._extract_sql_from_response(response_message.content)

                            if new_sql:
                                sql = new_sql
                            else:
                                break

                        # 최종 NOTE 로깅
                        tool_call_log.append({
                            "iteration": "note_taking_final",
                            "type": "note_taking_final",
                            "final_note": local_note_taker.get_final_note()
                        })

                    # Note-taking이 비활성화된 경우 기존 Refine agent 로직
                    elif sql and (self.enable_syntax_fixer or self.enable_empty_handler):
                        # Refine loop
                        for refine_iter in range(self.max_refine_iterations):
                            exec_result = self._execute_sql(sql, db_id)

                            # 성공 (row_count > 0) 이면 종료
                            if exec_result["success"] and exec_result["row_count"] > 0:
                                tool_call_log.append({
                                    "iteration": refine_iter + 1,
                                    "type": "refine_trigger",
                                    "reason": "success",
                                    "analysis": f"SQL 실행 성공: {exec_result['row_count']}행 반환"
                                })
                                break

                            # Refine agent 실행
                            refine_feedback = self._run_refine_agent(sql, exec_result, db_id, question)

                            if not refine_feedback:
                                # Refine agent가 피드백을 생성하지 않으면 종료
                                break

                            # Refine prompt 생성
                            refine_prompt = f"""Your SQL query had an issue. Please fix it based on the analysis below.

{refine_feedback}

Original SQL:
```sql
{sql}
```

Please provide a corrected SQL query."""

                            # 피드백 로깅 (refine_prompt 포함)
                            tool_call_log.append({
                                "iteration": refine_iter + 1,
                                "type": "refine_trigger",
                                "reason": exec_result["error_type"],
                                "analysis": refine_feedback,
                                "refine_prompt": refine_prompt,
                                "original_sql": sql
                            })

                            # LLM에게 피드백과 함께 재생성 요청
                            messages.append(response_message)
                            messages.append({
                                "role": "user",
                                "content": refine_prompt
                            })

                            # 재생성
                            if self.use_tools:
                                response = self.client.chat.completions.create(
                                    model=self.model_config['name'],
                                    messages=messages,
                                    tools=self.tools,
                                    tool_choice="auto",
                                    temperature=0
                                )
                            else:
                                response = self.client.chat.completions.create(
                                    model=self.model_config['name'],
                                    messages=messages,
                                    temperature=0
                                )

                            response_message = response.choices[0].message

                            # 새 응답에서 SQL 추출
                            new_sql = self._extract_sql_from_response(response_message.content)
                            if new_sql:
                                sql = new_sql
                                tool_call_log.append({
                                    "iteration": refine_iter + 1,
                                    "type": "final_response",
                                    "content": response_message.content
                                })
                            else:
                                break

                    # Forced refine 단계 (생성 후 항상 실행되는 검증)
                    if self.enable_forced_refine and sql and question and item:
                        refined_sql, refine_response = self._forced_refine(
                            sql, db_id, question, item, tool_call_log
                        )
                        if refined_sql != sql:
                            sql = refined_sql
                        # response를 refine 결과로 교체 (최종 SQL이 content에 들어가도록)
                        if refine_response:
                            response = refine_response

                    break

                # Tool call 실행
                messages.append(response_message)

                for tool_call in response_message.tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)

                    # Tool call 로깅
                    tool_call_log.append({
                        "iteration": iteration + 1,
                        "type": "tool_call",
                        "function": function_name,
                        "arguments": function_args
                    })

                    # Tool 실행
                    function_response = self._execute_tool_call(
                        function_name,
                        function_args,
                        db_id
                    )

                    # lookup_column_values 결과를 NoteTaker에 저장
                    if function_name == "lookup_column_values" and local_note_taker:
                        self._parse_and_store_lookup_result(function_args, function_response, local_note_taker)

                    # inspect_join_relationship 결과를 NoteTaker에 저장
                    if function_name == "inspect_join_relationship" and local_note_taker:
                        self._parse_and_store_join_result(function_args, function_response, local_note_taker)

                    # Tool 응답 로깅
                    tool_call_log.append({
                        "iteration": iteration + 1,
                        "type": "tool_response",
                        "function": function_name,
                        "response": function_response
                    })

                    # Tool 결과를 메시지에 추가
                    messages.append({
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": function_name,
                        "content": function_response
                    })

            # response 객체를 래퍼로 감싸서 tool_call_log 추가
            class ResponseWrapper:
                def __init__(self, response, tool_log):
                    self._response = response
                    self.tool_call_log = tool_log
                    # response의 모든 속성을 프록시
                    self.choices = response.choices
                    self.id = response.id
                    self.model = response.model
                    self.created = response.created

                def __getattr__(self, name):
                    return getattr(self._response, name)

            return ResponseWrapper(response, tool_call_log)

        except Exception as e:
            print(f"An error occurred while calling OpenAI API: {e}")
            import traceback
            traceback.print_exc()
            return None

    def format_tool_log(self, tool_call_log: List[Dict]) -> str:
        """Tool call 로그를 읽기 쉬운 형식으로 포맷팅"""
        if not tool_call_log:
            return "No tool calls were made."

        formatted = "\n" + "=" * 80 + "\n"
        formatted += "🔧 TOOL CALL LOG\n"
        formatted += "=" * 80 + "\n"

        for log_entry in tool_call_log:
            iteration = log_entry.get("iteration", "?")
            stage = log_entry.get("stage", None)
            log_type = log_entry.get("type")

            # Three-stage pipeline logs
            if log_type == "stage1_note":
                formatted += f"\n[Stage 1] 📋 NOTE PREPARATION:\n"
                formatted += "-" * 60 + "\n"
                content = log_entry.get('content', '')
                formatted += content + "\n"
                formatted += "-" * 60 + "\n"

            elif log_type == "stage2_note":
                formatted += f"\n[Stage 2] 🔧 ENHANCED NOTE (after tools):\n"
                formatted += "-" * 60 + "\n"
                content = log_entry.get('content', '')
                formatted += content + "\n"
                formatted += "-" * 60 + "\n"

            elif log_type == "tool_call":
                stage_prefix = f"Stage {stage}, " if stage else ""
                formatted += f"\n[{stage_prefix}Iteration {iteration}] 🤖 LLM Tool Call:\n"
                formatted += f"  Function: {log_entry['function']}\n"
                formatted += f"  Arguments: {json.dumps(log_entry['arguments'], indent=4)}\n"

            elif log_type == "tool_response":
                stage_prefix = f"Stage {stage}, " if stage else ""
                formatted += f"\n[{stage_prefix}Iteration {iteration}] 📊 Tool Response:\n"
                response = log_entry['response']
                # 응답을 들여쓰기
                formatted += "  " + response.replace("\n", "\n  ") + "\n"

            elif log_type == "final_response":
                stage_prefix = f"Stage {stage}" if stage else f"Iteration {iteration}"
                formatted += f"\n[{stage_prefix}] ✅ Final SQL Response:\n"
                formatted += f"{log_entry['content']}\n"

            elif log_type == "refine_trigger":
                formatted += f"\n[Refine {iteration}] 🔄 Refine Agent Triggered:\n"
                formatted += f"  Reason: {log_entry.get('reason', 'unknown')}\n"
                formatted += f"  Analysis:\n"
                analysis = log_entry.get('analysis', '')
                formatted += "  " + analysis.replace("\n", "\n  ") + "\n"

            elif log_type == "note_taking_iter":
                formatted += f"\n[Note {iteration}] 📝 Note-Taking Iteration:\n"
                formatted += f"  SQL: {log_entry.get('sql', '')[:100]}...\n"
                exec_result = log_entry.get('exec_result', {})
                formatted += f"  Exec Result: success={exec_result.get('success')}, rows={exec_result.get('row_count')}\n"
                formatted += f"  Schema Check:\n"
                schema_check = log_entry.get('schema_check', '')
                formatted += "    " + schema_check.replace("\n", "\n    ") + "\n"
                if log_entry.get('refine_feedback'):
                    formatted += f"  Refine Feedback: {log_entry.get('refine_feedback')}\n"
                if log_entry.get('rule_review'):
                    formatted += f"  Rule Review:\n"
                    rule_review = log_entry.get('rule_review', '')
                    formatted += "    " + rule_review.replace("\n", "\n    ") + "\n"

            elif log_type == "note_taking_final":
                formatted += f"\n[Note Final] 📋 Final Note:\n"
                final_note = log_entry.get('final_note', '')
                formatted += "  " + final_note.replace("\n", "\n  ") + "\n"

            elif log_type == "forced_refine":
                formatted += f"\n[Forced Refine] SQL Review:\n"
                formatted += "-" * 60 + "\n"
                formatted += f"  Original SQL: {log_entry.get('original_sql', '')[:200]}\n"
                formatted += f"  Exec Result Rows: {log_entry.get('exec_result_rows', 0)}\n"
                formatted += f"  Refine Response:\n"
                refine_resp = log_entry.get('refine_response', '')
                formatted += "    " + refine_resp.replace("\n", "\n    ")[:1000] + "\n"
                formatted += f"  Refined SQL: {log_entry.get('refined_sql', '')[:200]}\n"
                formatted += "-" * 60 + "\n"

        formatted += "=" * 80 + "\n"
        return formatted

    def _extract_sql_from_response(self, content: str) -> Optional[str]:
        """LLM 응답에서 SQL 추출"""
        if not content:
            return None

        # ```sql ... ``` 블록 추출
        sql_match = re.search(r'```sql\s*(.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()

        # ``` ... ``` 블록 추출 (sql 태그 없이)
        code_match = re.search(r'```\s*(SELECT.*?)\s*```', content, re.DOTALL | re.IGNORECASE)
        if code_match:
            return code_match.group(1).strip()

        # SELECT로 시작하는 문장 추출
        select_match = re.search(r'(SELECT\s+.*?;)', content, re.DOTALL | re.IGNORECASE)
        if select_match:
            return select_match.group(1).strip()

        # SELECT 문이 세미콜론 없이 끝나는 경우
        select_no_semi = re.search(r'(SELECT\s+.+?)(?:\n\n|$)', content, re.DOTALL | re.IGNORECASE)
        if select_no_semi:
            return select_no_semi.group(1).strip()

        return None

    def _execute_sql(self, sql: str, db_id: str, timeout_ms: int = 30000, max_rows: int = 5) -> Dict[str, Any]:
        """
        SQL 실행 및 결과 반환

        Returns:
            {
                "success": bool,
                "row_count": int,
                "error": str or None,
                "error_type": "syntax_error" | "empty_result" | "timeout" | None,
                "results": list (처음 max_rows 행)
            }
        """
        result = {
            "success": False,
            "row_count": 0,
            "error": None,
            "error_type": None,
            "results": []
        }

        try:
            conn = mysql.connector.connect(
                host=self.conn_info.get('host', '127.0.0.1'),
                port=self.conn_info.get('port', 3306),
                user=self.conn_info.get('user', 'root'),
                password=self.conn_info.get('password', ''),
                database=db_id
            )
            cursor = conn.cursor(dictionary=True)

            # Timeout 설정
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME = {timeout_ms}")

            # SQL 실행
            cursor.execute(sql)
            rows = cursor.fetchall()

            result["success"] = True
            result["row_count"] = len(rows)
            result["results"] = rows[:max_rows]

            # Empty result 체크
            if len(rows) == 0:
                result["error_type"] = "empty_result"

            cursor.close()
            conn.close()

        except mysql.connector.Error as e:
            error_msg = str(e)
            result["error"] = error_msg

            # Error 분류
            if "max_execution_time" in error_msg.lower() or "interrupted" in error_msg.lower():
                result["error_type"] = "timeout"
            else:
                result["error_type"] = "syntax_error"

        except Exception as e:
            result["error"] = str(e)
            result["error_type"] = "syntax_error"

        return result

    def _format_execute_sql_result(self, result: Dict[str, Any]) -> str:
        """execute_sql tool 결과를 LLM이 읽기 좋은 문자열로 포맷"""
        lines = []

        if result["success"]:
            lines.append(f"SUCCESS: {result['row_count']} rows returned")
            if result["row_count"] == 0:
                lines.append("WARNING: Query returned 0 rows. Check your WHERE conditions or JOIN logic.")
            elif result["results"]:
                # 컬럼 헤더
                cols = list(result["results"][0].keys())
                lines.append("| " + " | ".join(cols) + " |")
                lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
                # 데이터 행 (최대 5행)
                for row in result["results"]:
                    vals = [str(v) if v is not None else "NULL" for v in row.values()]
                    # 너무 긴 값은 잘라냄
                    vals = [v[:50] + "..." if len(v) > 50 else v for v in vals]
                    lines.append("| " + " | ".join(vals) + " |")
                if result["row_count"] > 5:
                    lines.append(f"... ({result['row_count'] - 5} more rows)")
        else:
            lines.append(f"ERROR ({result['error_type']}): {result['error']}")
            if result["error_type"] == "timeout":
                lines.append("HINT: Query timed out. Consider adding indexes, limiting results, or simplifying the query.")

        return "\n".join(lines)

    def _build_refine_hints(self, item: Dict[str, Any]) -> str:
        """item에서 evidence, mapping, join_keys를 추출하여 refine 힌트 문자열 생성"""
        parts = []
        evidence = item.get('evidence', '')
        if evidence:
            parts.append(f"Evidence: {evidence}")
        mapping = item.get('mapping', {})
        if mapping:
            mapping_texts = [f"- '{k}' → {', '.join(v)}" for k, v in mapping.items()]
            parts.append("Mappings:\n" + "\n".join(mapping_texts))
        join_keys = item.get('join_keys', [])
        if join_keys:
            join_str = ", ".join([f"({p[0]} = {p[1]})" for p in join_keys])
            parts.append(f"Join Keys: {join_str}")
        return "\n\n".join(parts)

    def _forced_refine(self, sql: str, db_id: str, question: str, item: Dict[str, Any], tool_call_log: List[Dict]) -> tuple:
        """
        강제 refine 단계: SQL 실행 결과 + NLQ + hints를 보고 재검토.
        생성 후 항상 실행되는 마지막 검증 단계.

        Returns:
            (refined_sql, response) 튜플
        """
        # 1. SQL 실행 (30행까지)
        exec_result = self._execute_sql(sql, db_id, max_rows=30)

        # 2. 결과 포맷팅
        result_str = self._format_execute_sql_result(exec_result)

        # 3. Hints 조립
        hints_str = self._build_refine_hints(item)

        # 4. 2단계: 먼저 PASS/FAIL 판정, FAIL일 때만 수정
        judge_system = """You judge whether a SQL query correctly answers the question. Reply with ONLY one word: PASS or FAIL.

PASS = The SQL reasonably answers the question. No critical errors.
FAIL = The SQL has a clear, concrete problem:
  - WHERE filter or condition the question never asked for (over-translation)
  - Hallucinated string values not in the Mappings/Evidence
  - Results obviously don't match the question's intent
  - COALESCE/IFNULL/FORMAT added without being requested

When in doubt, PASS. Most SQL is correct."""

        judge_user = f"""Question: {question}

{hints_str}

SQL:
```sql
{sql}
```

Execution Result ({exec_result['row_count']} rows):
{result_str}

Verdict (PASS or FAIL):"""

        # 5. 1단계: PASS/FAIL 판정
        try:
            judge_response = self.client.chat.completions.create(
                model=self.model_config['name'],
                messages=[
                    {"role": "system", "content": judge_system},
                    {"role": "user", "content": judge_user}
                ],
                temperature=0,
                max_completion_tokens=10
            )

            verdict = judge_response.choices[0].message.content.strip().upper()
            is_fail = "FAIL" in verdict

            if not is_fail:
                # PASS → 원본 SQL 그대로
                tool_call_log.append({
                    "type": "forced_refine",
                    "original_sql": sql,
                    "refined_sql": sql,
                    "verdict": "PASS",
                    "exec_result_rows": exec_result["row_count"],
                    "refine_response": f"PASS - no changes"
                })
                return sql, None

            # FAIL → 2단계: 수정 요청
            fix_system = """You are a SQL fixer. A SQL query was flagged as having a problem. Fix ONLY the specific issue. Keep everything else exactly the same.
Return the fixed SQL in ```sql ... ``` block."""

            fix_user = f"""Question: {question}

{hints_str}

Original SQL (flagged as problematic):
```sql
{sql}
```

Execution Result ({exec_result['row_count']} rows):
{result_str}

Fix only the concrete issue. Keep structure, JOINs, GROUP BY, aliases unchanged unless they are the problem."""

            response = self.client.chat.completions.create(
                model=self.model_config['name'],
                messages=[
                    {"role": "system", "content": fix_system},
                    {"role": "user", "content": fix_user}
                ],
                temperature=0
            )

            refine_content = response.choices[0].message.content
            refined_sql = self._extract_sql_from_response(refine_content)

            # 로깅
            tool_call_log.append({
                "type": "forced_refine",
                "original_sql": sql,
                "refined_sql": refined_sql or sql,
                "verdict": "FAIL",
                "exec_result_rows": exec_result["row_count"],
                "refine_response": refine_content
            })

            return (refined_sql if refined_sql else sql), response

        except Exception as e:
            print(f"Forced refine error: {e}")
            tool_call_log.append({
                "type": "forced_refine",
                "original_sql": sql,
                "refined_sql": sql,
                "exec_result_rows": exec_result.get("row_count", 0),
                "refine_response": f"Error: {str(e)}"
            })
            return sql, None

    def _parse_and_store_lookup_result(self, function_args: Dict, function_response: str, note_taker):
        """
        lookup_column_values 결과를 파싱하여 NoteTaker에 저장

        Args:
            function_args: tool call 인자 (table, column, search_term)
            function_response: tool 응답 문자열
            note_taker: ParsingNoteTaker 인스턴스
        """
        table = function_args.get('table', '')
        column = function_args.get('column', '')
        search_term = function_args.get('search_term', '')

        # 응답 파싱
        found = False
        similar_values = []

        if '✅ FOUND' in function_response:
            found = True
        elif '❌ NOT FOUND' in function_response:
            found = False
            # 유사값 추출: "→ 'value'" 형태 파싱
            import re
            # → 'value' (count rows) 패턴 매칭
            matches = re.findall(r"→ '([^']+)'", function_response)
            similar_values = matches[:5]

        note_taker.add_lookup_result(table, column, search_term, found, similar_values)

    def _parse_and_store_join_result(self, function_args: Dict, function_response: str, note_taker):
        """
        inspect_join_relationship 결과를 파싱하여 NoteTaker에 저장

        Args:
            function_args: tool call 인자 (table1, table2, join_key1, join_key2)
            function_response: tool 응답 문자열
            note_taker: ParsingNoteTaker 인스턴스
        """
        table1 = function_args.get('table1', '')
        table2 = function_args.get('table2', '')
        join_key1 = function_args.get('join_key1', '')
        join_key2 = function_args.get('join_key2', '')

        # 응답 파싱
        cardinality = "UNKNOWN"
        join_result_count = 0
        warning = None

        # Cardinality 추출: "Cardinality: X:X"
        import re
        card_match = re.search(r'Cardinality:\s*(\S+)', function_response)
        if card_match:
            cardinality = card_match.group(1)

        # JOIN result count 추출: "JOIN produces X rows"
        count_match = re.search(r'JOIN produces\s+([\d,]+)\s*rows', function_response)
        if count_match:
            join_result_count = int(count_match.group(1).replace(',', ''))

        # Warning 추출: "⚠️ WARNING:" 또는 "⚠️ BE CAREFUL"
        if '⚠️' in function_response:
            warning_match = re.search(r'⚠️[^:]*:\s*(.+?)(?:\n|$)', function_response)
            if warning_match:
                warning = warning_match.group(1).strip()
            elif 'M:N' in cardinality:
                warning = "M:N relationship - data multiplication risk"

        note_taker.add_join_analysis_result(
            table1, table2, join_key1, join_key2,
            cardinality, join_result_count, warning
        )

    def _run_refine_agent(self, sql: str, exec_result: Dict, db_id: str, question: str = None) -> Optional[str]:
        """
        Refine agent 실행 및 피드백 생성

        Returns:
            피드백 메시지 (LLM에게 전달) or None
        """
        error_type = exec_result.get("error_type")

        if error_type == "syntax_error" and self.enable_syntax_fixer:
            from src.refine_agent.syntax_fixer import analyze_sql_error, format_syntax_fix_advice
            analysis = analyze_sql_error(sql, exec_result.get("error", ""))
            return format_syntax_fix_advice(analysis)

        elif error_type == "empty_result" and self.enable_empty_handler:
            from src.refine_agent.empty_result_handler import analyze_empty_result, format_empty_result_advice
            analysis = analyze_empty_result(sql, self.conn_info, db_id, question)
            return format_empty_result_advice(analysis)

        return None

    def _get_llm_feedback(self, sql: str, question: str, item: Dict[str, Any], current_note: str = None) -> Optional[Tuple[str, int]]:
        """
        LLM에게 SQL에 대한 비판적 검토 요청

        Args:
            sql: 생성된 SQL
            question: 원본 NLQ
            item: 데이터셋 아이템 (mapping, join_keys 등 hints 포함)
            current_note: 현재까지의 NOTE (optional)

        Returns:
            (피드백 문자열, 확신도 1-5) 또는 None (문제 없음)
        """
        if not sql or not question or not item:
            return None

        # Hints 정보 추출
        mapping = item.get('mapping', {})
        join_keys = item.get('join_keys', [])
        evidence = item.get('evidence', '')

        # Hints를 읽기 쉬운 형식으로 변환
        hints_text = "Hints:\n"
        if evidence:
            hints_text += f"  [Evidence] {evidence}\n"
        if mapping:
            hints_text += "  [Mapping - 사용해야 하는 컬럼들]\n"
            for keyword, columns in mapping.items():
                hints_text += f"    '{keyword}' → {', '.join(columns)}\n"
        if join_keys:
            hints_text += "  [Join Keys - 사용해야 하는 조인 조건]\n"
            for pair in join_keys:
                if len(pair) == 2:
                    hints_text += f"    {pair[0]} = {pair[1]}\n"

        # 비판적 검토 프롬프트 (매우 보수적으로 - 명백한 오류만)
        review_prompt = f"""다음 SQL이 Question의 의도와 일치하는지 검토해주세요.

Question: {question}

Generated SQL:
```sql
{sql}
```

**검토 항목 (명백한 구조적 오류만):**
1. Question이 "가장 많은/최대/highest/most"를 요구하는데 ORDER BY DESC가 없거나 ASC인가?
2. Question이 "가장 적은/최소/lowest/least"를 요구하는데 ORDER BY ASC가 없거나 DESC인가?
3. Question이 "상위 N개/top N"을 요구하는데 LIMIT N이 없는가?

**절대 지적하지 말 것 (이미 별도 검증됨):**
- WHERE 절의 값 (예: 'History', 'Active', 'STREET') - lookup_val로 이미 검증됨
- 컬럼/테이블 누락 - Schema Check로 이미 검증됨
- JOIN 조건 - hints에서 제공됨
- 비즈니스 로직 해석 (예: "독립 활동"이 무엇인지)

**판단 기준:**
- SQL의 WHERE 값이 Question과 다르게 보여도 OK (lookup_val이 찾은 실제 DB 값일 수 있음)
- Question에 없는 조건이 WHERE에 있어도 OK (hints에서 온 것일 수 있음)
- 조금이라도 불확실하면 반드시 OK로 응답

응답 형식:
[확신도: N] 피드백

- 확신도 1-3: 문제없거나 불확실 → OK
- 확신도 4: 거의 확실히 틀림 (ORDER BY 방향 반대 등)
- 확신도 5: 100% 확실히 틀림

예시:
- "[확신도: 1] OK"
- "[확신도: 5] Question은 'highest'를 요구하는데 ORDER BY ASC임"
"""

        try:
            model_name = self.model_config['name'].lower()
            api_params = {
                "model": self.model_config['name'],
                "messages": [
                    {"role": "system", "content": "당신은 매우 보수적인 SQL 검토자입니다. ORDER BY 방향 오류 같은 명백한 구조적 오류만 지적합니다. WHERE 값이나 비즈니스 로직은 절대 판단하지 않습니다. 조금이라도 불확실하면 OK로 응답합니다."},
                    {"role": "user", "content": review_prompt}
                ],
                "temperature": 0
            }

            if any(x in model_name for x in ['gpt-5', 'o1', 'o3']):
                api_params["max_completion_tokens"] = 200
            else:
                api_params["max_tokens"] = 200

            response = self.client.chat.completions.create(**api_params)

            feedback = response.choices[0].message.content.strip()

            # 확신도 파싱
            import re
            confidence_match = re.search(r'\[확신도:\s*(\d)\]', feedback)
            if confidence_match:
                confidence = int(confidence_match.group(1))
                # 확신도 1-2는 문제없음으로 처리
                if confidence <= 2:
                    return None
                # 피드백 텍스트에서 확신도 태그 제거
                feedback_text = re.sub(r'\[확신도:\s*\d\]\s*', '', feedback).strip()
                if feedback_text.upper() == "OK":
                    return None
                return (feedback_text, confidence)
            else:
                # 파싱 실패 시 기존 방식으로 폴백
                if feedback.upper() == "OK" or "문제없" in feedback:
                    return None
                return (feedback, 3)  # 기본 확신도 3

        except Exception as e:
            print(f"LLM feedback 요청 중 오류: {e}")
            return None

    def generate_three_stage(
        self,
        schema: str,
        question: str,
        hints: str = "",
        db_id: str = "dw",
        item: Dict[str, Any] = None,
        gold_sql: str = None,
        max_tool_iterations: int = 10
    ):
        """
        Three-stage SQL generation pipeline.

        Stage 1: Note Preparation - analyze question and schema
        Stage 2: Plan with Tools - call tools to gather information
        Stage 3: SQL Generation - generate SQL using prepared notes

        Args:
            schema: Database schema
            question: Natural language question
            hints: Additional hints (evidence, mapping, etc.)
            db_id: Database ID
            item: Dataset item (for ask_granularity tool)
            gold_sql: Gold SQL (for User Agent mode)
            max_tool_iterations: Max iterations for Stage 2 tool calls

        Returns:
            response object with tool_call_log
        """
        from src.prompt_builder.three_stage_prompts import (
            build_stage1_prompt, build_stage2_prompt, build_stage3_prompt
        )

        # Thread-local storage for User Agent mode
        self._thread_local.gold_sql = gold_sql

        tool_call_log = []

        try:
            # ================================================================
            # STAGE 1: NOTE PREPARATION (No tools)
            # ================================================================
            stage1_prompts = build_stage1_prompt(schema, question, hints)

            stage1_messages = [
                {"role": "system", "content": stage1_prompts["system"]},
                {"role": "user", "content": stage1_prompts["user"]}
            ]

            stage1_response = self.client.chat.completions.create(
                model=self.model_config['name'],
                messages=stage1_messages,
                temperature=0
            )

            stage1_note = stage1_response.choices[0].message.content

            tool_call_log.append({
                "stage": 1,
                "type": "stage1_note",
                "content": stage1_note
            })

            # ================================================================
            # STAGE 2: PLAN WITH TOOLS
            # ================================================================
            stage2_prompts = build_stage2_prompt(schema, question, stage1_note, hints)

            # Build Stage 2 system message with tool descriptions
            stage2_system = stage2_prompts["system"]
            if self.use_tools:
                stage2_system += "\n\nAvailable tools:\n"
                if self.enable_lookup_column_values:
                    stage2_system += "- lookup_column_values: Verify if a value exists in a column\n"
                if self.enable_join_path_finder:
                    stage2_system += "- find_join_path: Find JOIN path between tables\n"
                if self.enable_join_inspector:
                    stage2_system += "- inspect_join_relationship: Check JOIN cardinality\n"
                if self.enable_ask_granularity:
                    stage2_system += "- ask_granularity: Clarify aggregation pattern with user\n"
                if self.enable_execute_sql:
                    stage2_system += "- execute_sql: Execute SQL against database to test and verify results\n"

            stage2_messages = [
                {"role": "system", "content": stage2_system},
                {"role": "user", "content": stage2_prompts["user"]}
            ]

            # Stage 2 loop with tool calls
            stage2_note = None
            for iteration in range(max_tool_iterations):
                if self.use_tools:
                    stage2_response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=stage2_messages,
                        tools=self.tools,
                        tool_choice="auto",
                        temperature=0
                    )
                else:
                    stage2_response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=stage2_messages,
                        temperature=0
                    )

                response_message = stage2_response.choices[0].message

                # If no tool calls, we have the enhanced note
                if not response_message.tool_calls:
                    stage2_note = response_message.content
                    tool_call_log.append({
                        "stage": 2,
                        "iteration": iteration + 1,
                        "type": "stage2_note",
                        "content": stage2_note
                    })
                    break

                # Process tool calls
                stage2_messages.append(response_message)

                for tool_call in response_message.tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)

                    tool_call_log.append({
                        "stage": 2,
                        "iteration": iteration + 1,
                        "type": "tool_call",
                        "function": function_name,
                        "arguments": function_args
                    })

                    # Execute tool
                    function_response = self._execute_tool_call(
                        function_name, function_args, db_id
                    )

                    tool_call_log.append({
                        "stage": 2,
                        "iteration": iteration + 1,
                        "type": "tool_response",
                        "function": function_name,
                        "response": function_response[:500] if len(function_response) > 500 else function_response
                    })

                    stage2_messages.append({
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": function_name,
                        "content": function_response
                    })

            # Fallback if no note was generated
            if not stage2_note:
                stage2_note = stage1_note + "\n\n[No additional verification performed]"

            # ================================================================
            # STAGE 3: SQL GENERATION (No tools)
            # ================================================================
            stage3_prompts = build_stage3_prompt(schema, question, stage2_note)

            stage3_messages = [
                {"role": "system", "content": stage3_prompts["system"]},
                {"role": "user", "content": stage3_prompts["user"]}
            ]

            stage3_response = self.client.chat.completions.create(
                model=self.model_config['name'],
                messages=stage3_messages,
                temperature=0
            )

            final_sql_content = stage3_response.choices[0].message.content

            tool_call_log.append({
                "stage": 3,
                "type": "final_response",
                "content": final_sql_content
            })

            # Create response wrapper
            class ResponseWrapper:
                def __init__(self, response, tool_log):
                    self._response = response
                    self.tool_call_log = tool_log
                    self.choices = response.choices
                    self.id = response.id
                    self.model = response.model
                    self.created = response.created

                def __getattr__(self, name):
                    return getattr(self._response, name)

            return ResponseWrapper(stage3_response, tool_call_log)

        except Exception as e:
            print(f"Three-stage generation error: {e}")
            import traceback
            traceback.print_exc()
            return None
