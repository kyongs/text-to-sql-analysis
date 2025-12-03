# src/model/openai_model.py

import os
from openai import OpenAI
from typing import Dict, Any

class OpenAIModel:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_config = config['model']
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable not set.")
        self.client = OpenAI(api_key=api_key)

    def generate(self, prompt: str):
        """
        OpenAI API를 호출하고 전체 응답 객체를 반환합니다.
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model_config['name'],
                messages=[
                    {"role": "system", "content": "You are a SQLite SQL expert. Your job is to write a SQLite SQL query to answer the user's question."},
                    {"role": "user", "content": prompt}
                ],
                # temperature=0,
            )
            return response
        except Exception as e:
            print(f"An error occurred while calling OpenAI API: {e}")
            return None