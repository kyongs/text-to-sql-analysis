# src/model/openai_model_with_tools.py

import os
import json
from openai import OpenAI
from typing import Dict, Any, List, Optional
from src.agent.join_inspector import inspect_join_relationship
from src.agent.join_path_finder import find_join_path
from src.agent.column_value_lookup import lookup_column_values, format_lookup_result
from src.agent.ask_granularity import ask_granularity


class OpenAIModelWithTools:
    """
    OpenAI 모델에 tool calling 기능을 추가한 클래스
    LLM이 JOIN 관계를 분석할 수 있도록 함
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
        self.enable_ask_granularity = enabled_tools.get('ask_granularity', False)

        # User Agent 설정 (ask_granularity에서 LLM2가 자동 선택)
        user_agent_config = config.get('user_agent', {})
        self.use_user_agent = user_agent_config.get('enabled', False)
        self.user_agent_model = user_agent_config.get('model', 'gpt-4o-mini')

        # 현재 처리 중인 gold_sql (generate 호출 시 설정)
        self._current_gold_sql = None

        # Tool 정의 (활성화된 tool만)
        self.tools = self._initialize_tools()
    
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
                    "description": "Look up actual distinct values in a database column. **MUST USE this tool when:** (1) The question mentions a category/role/department/status but doesn't give the EXACT database value, (2) You're about to write WHERE column = 'some string' without seeing that exact string in the schema examples, (3) The column stores categorical data like names, types, or statuses. **Common mistakes this prevents:** 'Supervisor' vs actual 'Activity leader', 'Computer Science' vs actual 'Electrical Eng & Computer Sci'. Even if you see example values in the schema, they may be incomplete - use this tool to confirm.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table": {
                                "type": "string",
                                "description": "The table name to query"
                            },
                            "column": {
                                "type": "string",
                                "description": "The column name to get distinct values from"
                            }
                        },
                        "required": ["table", "column"]
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

        return tools
    
    def _execute_tool_call(self, tool_name: str, arguments: Dict[str, Any], db_id: str) -> str:
        """Tool call 실행"""
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
                db_id=db_id
            )
            return format_lookup_result(result)
        elif tool_name == "ask_granularity":
            return ask_granularity(
                question=arguments["question"],
                options=arguments["options"],
                reason=arguments.get("reason", ""),
                gold_sql=self._current_gold_sql,
                use_user_agent=self.use_user_agent,
                user_agent_model=self.user_agent_model
            )
        else:
            return f"Unknown tool: {tool_name}"
    
    def generate(self, prompt: str, db_id: str = "dw", max_iterations: int = 10, gold_sql: str = None):
        """
        OpenAI API를 호출하고 필요시 tool calling 수행

        Args:
            prompt: 사용자 프롬프트
            db_id: 데이터베이스 ID
            max_iterations: 최대 tool call 반복 횟수
            gold_sql: 정답 SQL (User Agent 모드에서 ask_granularity tool이 사용)

        Returns:
            response 객체와 tool call 로그
        """
        # User Agent 모드에서 사용할 gold_sql 저장
        self._current_gold_sql = gold_sql

        system_message = """You are a MySQL SQL expert. Your job is to write a MySQL SQL query to answer the user's question.

You have access to tools that help you write better SQL:

1. **find_join_path**: Find the optimal JOIN path between two tables
   - **USE THIS FIRST** when you need to join tables that might not be directly related
   - Returns the shortest path including any necessary intermediate (bridge) tables
   - **CRITICAL**: Do NOT skip intermediate tables - each hop is required for data integrity

2. **inspect_join_relationship**: Analyze JOIN relationships between tables
   - Check cardinality (1:1, 1:N, M:N) before writing JOIN queries
   - Identify potential data multiplication issues

3. **ask_granularity**: Clarify aggregation granularity with the user
   - Use when the question involves COUNT/SUM/AVG and the desired result level is ambiguous
   - The tool will present options to the user and return their choice
   - Wait for user selection before writing the final SQL

When writing SQL queries:
- **Multi-hop JOINs**: If find_join_path shows intermediate tables, you MUST include ALL of them in your query
- **DISTINCT usage**: If the tool shows M:N (many-to-many) cardinality, consider using SELECT DISTINCT or COUNT(DISTINCT ...) to avoid duplicate rows
- **JOIN type selection**: Logically determine whether to use INNER JOIN or LEFT JOIN based on:
  * Whether you need all rows from the left table (LEFT JOIN) or only matching rows (INNER JOIN)
  * The cardinality information from the tool
  * The business logic of the question
- **GROUP BY optimization**: For M:N relationships, use GROUP BY with appropriate aggregate functions (COUNT DISTINCT, MAX, MIN, etc.)
"""
        
        # Tool이 없으면 간단한 시스템 메시지
        if len(self.tools) == 0:
            system_message = """You are a MySQL SQL expert. Your job is to write a MySQL SQL query to answer the user's question."""
        
        if self.db_type == 'sqlite':
            system_message = system_message.replace("MySQL", "SQLite")
        
        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt}
        ]
        
        tool_call_log = []  # Tool call 중간 과정 로깅
        
        try:
            for iteration in range(max_iterations):
                # API 호출 - tools 리스트가 비어있지 않으면 tool calling 활성화
                if len(self.tools) > 0:
                    response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=messages,
                        tools=self.tools,
                        tool_choice="auto"
                    )
                else:
                    response = self.client.chat.completions.create(
                        model=self.model_config['name'],
                        messages=messages
                    )
                
                response_message = response.choices[0].message
                
                # Tool call이 없으면 종료
                if not response_message.tool_calls:
                    # 최종 응답 로깅
                    tool_call_log.append({
                        "iteration": iteration + 1,
                        "type": "final_response",
                        "content": response_message.content
                    })
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
        
        formatted = "\n" + "="*80 + "\n"
        formatted += "🔧 TOOL CALL LOG\n"
        formatted += "="*80 + "\n"
        
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
        
        formatted += "="*80 + "\n"
        return formatted
