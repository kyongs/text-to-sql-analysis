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
        # 문항 정보 추출
        original_index = result_data.get('original_index', '?')
        question = result_data.get('question', 'N/A')
        db_id = result_data.get('db_id', 'N/A')

        prompt = result_data.get('prompt', 'N/A')
        model_response = result_data.get('model_response')
        predicted_sql = result_data.get('predicted_sql', 'N/A')
        tool_call_log = result_data.get('tool_call_log')

        # 문항 헤더 추가
        header_str = f"{'='*150}\n"
        header_str += f"[Question #{original_index}] {question}\n"
        header_str += f"DB: {db_id}\n"
        header_str += f"{'='*150}\n\n"

        final_prompt_str = f"***** FINAL PROMPT *****\n{prompt}\n\n"
        
        # Tool call 로그 추가
        tool_log_str = ""
        if tool_call_log:
            tool_log_str = "***** TOOL CALL LOG *****\n"
            import json
            for log_entry in tool_call_log:
                iteration = log_entry.get("iteration", "?")
                stage = log_entry.get("stage", None)
                log_type = log_entry.get("type")

                # Three-stage pipeline logs
                if log_type == "stage1_note":
                    tool_log_str += f"\n[Stage 1] 📋 NOTE PREPARATION:\n"
                    tool_log_str += "-" * 60 + "\n"
                    content = log_entry.get('content', '')
                    tool_log_str += content + "\n"
                    tool_log_str += "-" * 60 + "\n"

                elif log_type == "stage2_note":
                    tool_log_str += f"\n[Stage 2] 🔧 ENHANCED NOTE (after tools):\n"
                    tool_log_str += "-" * 60 + "\n"
                    content = log_entry.get('content', '')
                    tool_log_str += content + "\n"
                    tool_log_str += "-" * 60 + "\n"

                elif log_type == "tool_call":
                    stage_prefix = f"Stage {stage}, " if stage else ""
                    tool_log_str += f"\n[{stage_prefix}Iteration {iteration}] 🤖 LLM Tool Call:\n"
                    tool_log_str += f"  Function: {log_entry['function']}\n"
                    tool_log_str += f"  Arguments: {json.dumps(log_entry['arguments'], indent=4)}\n"

                elif log_type == "tool_response":
                    stage_prefix = f"Stage {stage}, " if stage else ""
                    tool_log_str += f"\n[{stage_prefix}Iteration {iteration}] 📊 Tool Response:\n"
                    response = log_entry['response']
                    # 응답을 들여쓰기 (간략화)
                    lines = response.split('\n')[:20]  # 처음 20줄만
                    tool_log_str += "  " + "\n  ".join(lines) + "\n"
                    if len(response.split('\n')) > 20:
                        tool_log_str += "  ... (truncated)\n"

                elif log_type == "final_response":
                    stage_prefix = f"Stage {stage}" if stage else f"Iteration {iteration}"
                    tool_log_str += f"\n[{stage_prefix}] ✅ Final SQL Response:\n"
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

                elif log_type == "note_taking_iter":
                    tool_log_str += f"\n[Note {iteration}] 📝 Note-Taking Iteration:\n"
                    sql_preview = log_entry.get('sql', '')[:100]
                    tool_log_str += f"  SQL: {sql_preview}...\n"
                    exec_result = log_entry.get('exec_result', {})
                    tool_log_str += f"  Exec Result: success={exec_result.get('success')}, rows={exec_result.get('row_count')}\n"
                    schema_check = log_entry.get('schema_check', '')
                    tool_log_str += f"  Schema Check:\n"
                    for line in schema_check.split('\n')[:10]:
                        tool_log_str += f"    {line}\n"
                    if log_entry.get('refine_feedback'):
                        tool_log_str += f"  Refine Feedback: {log_entry.get('refine_feedback')}\n"
                    if log_entry.get('rule_review'):
                        tool_log_str += f"  Rule Review:\n"
                        rule_review = log_entry.get('rule_review', '')
                        for line in rule_review.split('\n')[:10]:
                            tool_log_str += f"    {line}\n"
                    if log_entry.get('llm_feedback'):
                        tool_log_str += f"  LLM Feedback: {log_entry.get('llm_feedback')}\n"

                elif log_type == "note_taking_final":
                    tool_log_str += f"\n[Note Final] 📋 Final Note:\n"
                    final_note = log_entry.get('final_note', '')
                    for line in final_note.split('\n')[:50]:
                        tool_log_str += f"  {line}\n"
                    if len(final_note.split('\n')) > 50:
                        tool_log_str += "  ... (truncated)\n"

                elif log_type == "forced_refine":
                    tool_log_str += f"\n[Forced Refine] SQL Review:\n"
                    tool_log_str += "-" * 60 + "\n"
                    tool_log_str += f"  Original SQL: {log_entry.get('original_sql', '')[:200]}\n"
                    tool_log_str += f"  Exec Result Rows: {log_entry.get('exec_result_rows', 0)}\n"
                    tool_log_str += f"  Refine Response:\n"
                    refine_resp = log_entry.get('refine_response', '')
                    for line in refine_resp.split('\n')[:40]:
                        tool_log_str += f"    {line}\n"
                    if len(refine_resp.split('\n')) > 40:
                        tool_log_str += "    ... (truncated)\n"
                    tool_log_str += f"  Refined SQL: {log_entry.get('refined_sql', '')[:200]}\n"
                    tool_log_str += "-" * 60 + "\n"

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
            header_str +
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
