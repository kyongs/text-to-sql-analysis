# src/model/openai_model.py

import os
import re
import json
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

        # Refine agent 활성화 여부
        refine_agents = config.get('refine_agents', {})
        self.enable_syntax_fixer = refine_agents.get('syntax_fixer', False)
        self.enable_empty_handler = refine_agents.get('empty_handler', False)
        self.max_refine_iterations = refine_agents.get('max_iterations', 1)

        # Note-taking 활성화 여부 (실제 인스턴스는 generate()에서 스레드 로컬로 생성)
        self.enable_note_taking = config.get('note_taking', False)

        # LLM Feedback 활성화 여부 (note_taking과 함께 사용)
        self.enable_llm_feedback = config.get('llm_feedback', False)

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
        else:
            return f"Unknown tool: {tool_name}"

    def generate(self, prompt: str, db_id: str = "dw", max_iterations: int = 10, question: str = None, item: Dict[str, Any] = None):
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

        Returns:
            response 객체 (tool 사용 시 tool_call_log 포함)
        """
        # Note-taking 초기화 (각 호출마다 새로운 NoteTaker 생성 - 멀티스레드 안전)
        local_note_taker = None
        if self.enable_note_taking and item:
            import sys
            from pathlib import Path
            src_dir = Path(__file__).parent.parent
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            from note_taker import ParsingNoteTaker
            local_note_taker = ParsingNoteTaker(item)

        # Tool이 있으면 상세 시스템 메시지 (활성화된 tool에 따라 동적 생성)
        if self.use_tools:
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
                            llm_feedback = None
                            if self.enable_llm_feedback and question and item:
                                current_note_for_feedback = local_note_taker.get_current_note() if local_note_taker.iter_notes else None
                                llm_feedback = self._get_llm_feedback(sql, question, item, current_note_for_feedback)

                            # NOTE에 iter 기록 추가 (llm_feedback 포함)
                            local_note_taker.add_iter_note(note_iter, sql, exec_result, llm_feedback)

                            # 로깅
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
                                "llm_feedback": llm_feedback
                            })

                            # 성공이고 문제없으면 종료 (LLM Feedback도 없어야 함)
                            has_llm_issues = llm_feedback is not None
                            if exec_result["success"] and exec_result["row_count"] > 0 and not local_note_taker.has_issues() and not has_llm_issues:
                                break

                            # 마지막 iter면 종료
                            if note_iter >= max_note_iterations:
                                break

                            # 문제가 있으면 NOTE와 함께 refine 요청
                            issues_summary = local_note_taker.get_issues_summary()
                            current_note = local_note_taker.get_current_note()

                            # LLM Feedback이 있으면 issues에 추가
                            if llm_feedback:
                                if issues_summary:
                                    issues_summary += f"\n\n[LLM Review]\n{llm_feedback}"
                                else:
                                    issues_summary = f"[LLM Review]\n{llm_feedback}"

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
            log_type = log_entry.get("type")

            if log_type == "tool_call":
                formatted += f"\n[Iteration {iteration}] 🤖 LLM Tool Call:\n"
                formatted += f"  Function: {log_entry['function']}\n"
                formatted += f"  Arguments: {json.dumps(log_entry['arguments'], indent=4)}\n"

            elif log_type == "tool_response":
                formatted += f"\n[Iteration {iteration}] 📊 Tool Response:\n"
                response = log_entry['response']
                # 응답을 들여쓰기
                formatted += "  " + response.replace("\n", "\n  ") + "\n"

            elif log_type == "final_response":
                formatted += f"\n[Iteration {iteration}] ✅ Final SQL Response:\n"
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

            elif log_type == "note_taking_final":
                formatted += f"\n[Note Final] 📋 Final Note:\n"
                final_note = log_entry.get('final_note', '')
                formatted += "  " + final_note.replace("\n", "\n  ") + "\n"

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

    def _execute_sql(self, sql: str, db_id: str, timeout_ms: int = 30000) -> Dict[str, Any]:
        """
        SQL 실행 및 결과 반환

        Returns:
            {
                "success": bool,
                "row_count": int,
                "error": str or None,
                "error_type": "syntax_error" | "empty_result" | "timeout" | None,
                "results": list (처음 몇 행)
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
            result["results"] = rows[:5]  # 처음 5행만 저장

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

    def _get_llm_feedback(self, sql: str, question: str, item: Dict[str, Any], current_note: str = None) -> Optional[str]:
        """
        LLM에게 SQL에 대한 비판적 검토 요청

        Args:
            sql: 생성된 SQL
            question: 원본 NLQ
            item: 데이터셋 아이템 (mapping, join_keys 등 hints 포함)
            current_note: 현재까지의 NOTE (optional)

        Returns:
            LLM의 비판적 검토 피드백
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

        # 비판적 검토 프롬프트
        review_prompt = f"""다음 SQL을 비판적으로 검토해주세요.

Question: {question}

{hints_text}

Generated SQL:
```sql
{sql}
```
"""
        if current_note:
            review_prompt += f"""
현재 NOTE:
{current_note}
"""

        review_prompt += """
검토 항목 (확실한 문제만 지적):
1. Hints의 컬럼/조인이 SQL에서 누락되었는가? (NOTE의 Schema Check에서 ☐ 표시된 항목)
2. Question의 의도와 SQL의 결과가 일치하는가? (예: "list all"인데 LIMIT이 있거나, 집계가 필요한데 없는 경우)
3. 불필요한 DISTINCT나 GROUP BY가 있는가?

주의사항:
- WHERE 절 조건의 값(예: 'STREET', 'Active')은 컬럼명이나 Question에서 유추 가능하면 정상임. 단순히 Hints에 없다고 문제 삼지 말 것.
- 구체적인 값의 정확성(예: 'Computer Science' vs 'CS')은 판단하지 말 것 - 이는 검증 불가.
- 확실하지 않으면 OK로 응답.

확실한 문제가 있으면 간결하게 지적하고, 문제가 없거나 불확실하면 "OK"라고만 응답.
응답은 한국어로, 2문장 이내로."""

        try:
            # GPT-5.1+ uses max_completion_tokens, older models use max_tokens
            model_name = self.model_config['name'].lower()
            api_params = {
                "model": self.model_config['name'],
                "messages": [
                    {"role": "system", "content": "당신은 SQL 검토 전문가입니다. 주어진 SQL이 Question과 Hints에 부합하는지 비판적으로 검토합니다."},
                    {"role": "user", "content": review_prompt}
                ],
                "temperature": 0
            }

            # GPT-5, o1, o3 등 최신 모델은 max_completion_tokens 사용
            if any(x in model_name for x in ['gpt-5', 'o1', 'o3']):
                api_params["max_completion_tokens"] = 300
            else:
                api_params["max_tokens"] = 300

            response = self.client.chat.completions.create(**api_params)

            feedback = response.choices[0].message.content.strip()

            # "OK"만 반환하면 None으로 처리 (문제 없음)
            if feedback.upper() == "OK" or feedback == "문제없음" or feedback == "문제 없음":
                return None

            return feedback

        except Exception as e:
            print(f"LLM feedback 요청 중 오류: {e}")
            return None
