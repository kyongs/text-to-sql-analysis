# src/db_utils/logger.py

import threading
from datetime import datetime

class TxtLogger:
    """
    Handles writing detailed, human-readable logs to a .txt file in a thread-safe manner.
    """
    def __init__(self, log_path: str, total_items: int):
        self.log_path = log_path
        self.total_items = total_items
        self.processed_count = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.lock = threading.Lock() 
        with open(self.log_path, 'w', encoding='utf-8') as f:
            f.write(f"--- Log Session Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")


    def format_and_log(self, result_data: dict):
        """
        Formats a single result into a text block and appends it to the log file.
        """
        prompt = result_data.get('prompt', 'N/A')
        model_response = result_data.get('model_response')
        predicted_sql = result_data.get('predicted_sql', 'N/A')
        tool_call_log = result_data.get('tool_call_log')

        final_prompt_str = f"***** FINAL PROMPT *****\n{prompt}\n\n"
        
        # Tool call 로그 추가
        tool_log_str = ""
        if tool_call_log:
            tool_log_str = "***** TOOL CALL LOG *****\n"
            for log_entry in tool_call_log:
                iteration = log_entry.get("iteration", "?")
                log_type = log_entry.get("type")
                
                if log_type == "tool_call":
                    tool_log_str += f"\n[Iteration {iteration}] 🤖 LLM Tool Call:\n"
                    tool_log_str += f"  Function: {log_entry['function']}\n"
                    import json
                    tool_log_str += f"  Arguments: {json.dumps(log_entry['arguments'], indent=4)}\n"
                
                elif log_type == "tool_response":
                    tool_log_str += f"\n[Iteration {iteration}] 📊 Tool Response:\n"
                    response = log_entry['response']
                    # 응답을 들여쓰기 (간략화)
                    lines = response.split('\n')[:20]  # 처음 20줄만
                    tool_log_str += "  " + "\n  ".join(lines) + "\n"
                    if len(response.split('\n')) > 20:
                        tool_log_str += "  ... (truncated)\n"
                
                elif log_type == "final_response":
                    tool_log_str += f"\n[Iteration {iteration}] ✅ Final SQL Response:\n"
                    tool_log_str += f"  {log_entry['content']}\n"

                elif log_type == "refine_trigger":
                    tool_log_str += f"\n[Refine {iteration}] 🔄 Refine Agent Triggered:\n"
                    tool_log_str += f"  Reason: {log_entry.get('reason', 'unknown')}\n"
                    analysis = log_entry.get('analysis', '')
                    if analysis:
                        tool_log_str += f"  Analysis:\n"
                        # 분석 내용 들여쓰기
                        for line in analysis.split('\n')[:30]:  # 처음 30줄만
                            tool_log_str += f"    {line}\n"
                        if len(analysis.split('\n')) > 30:
                            tool_log_str += "    ... (truncated)\n"

            tool_log_str += "\n"
        
        response_str = f"***** RESPONSE *****\n{model_response}\n\n"

        usage = model_response.usage if model_response else None
        token_info_str = f"***** TOKEN INFO *****\n{usage}\n\n"
        
        final_sql_str = f"***** FINAL SQL QUERY *****\n{predicted_sql}\n\n"

        token_monitoring_str = ""
        if usage:
            with self.lock:
                self.processed_count += 1
                self.total_prompt_tokens += usage.prompt_tokens
                self.total_completion_tokens += usage.completion_tokens
                
                avg_prompt = self.total_prompt_tokens / self.processed_count
                avg_completion = self.total_completion_tokens / self.processed_count
                total_tokens = self.total_prompt_tokens + self.total_completion_tokens
                
                token_monitoring_str = (
                    "***** TOKEN MONITORING *****\n"
                    f"TOKEN PER ITERATION    : {usage.prompt_tokens}, {usage.completion_tokens}\n"
                    f"TOTAL                  : {total_tokens}, {self.total_prompt_tokens}, {self.total_completion_tokens}\n"
                    f"AVG                    : {avg_prompt + avg_completion:.2f}, {avg_prompt:.2f}, {avg_completion:.2f}\n"
                )

        log_entry = (
            final_prompt_str +
            tool_log_str +
            response_str +
            token_info_str +
            final_sql_str +
            token_monitoring_str +
            "*" * 150 + "\n" +
            "*" * 150 + "\n\n"
        )

        with self.lock:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(log_entry)
