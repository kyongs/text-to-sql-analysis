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

        final_prompt_str = f"***** FINAL PROMPT *****\n{prompt}\n\n"
        
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
