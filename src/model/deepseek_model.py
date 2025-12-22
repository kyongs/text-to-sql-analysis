import os
from openai import OpenAI
from typing import Dict, Any

class DeepSeekModel:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_config = config['model']
        
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable not set.")
        
        # [수정 1] config에서 base_url을 가져오도록 변경 (Special 모델 대응)
        # config에 없으면 기본 주소 사용
        base_url = self.model_config.get('base_url', "https://api.deepseek.com")
        print(f"DeepSeek API URL: {base_url}")
        
        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url
        )

    def generate(self, prompt: str):
        try:
            # 기본 인자 설정
            kwargs = {
                "model": self.model_config['name'],
                "messages": [
                    {"role": "system", "content": "You are a SQLite SQL expert. Output ONLY the SQLite SQL query. Do not explain."},
                    {"role": "user", "content": prompt}
                ],
                "stream": False
            }

            # [수정 2] Reasoner(추론) 모델이나 Special 모델은 temperature를 지원하지 않음
            # 모델명에 'reasoner'가 없고, URL에 'speciale'이 없을 때만 temperature=0 설정
            is_reasoning_model = "reasoner" in self.model_config['name']
            is_speciale_url = "speciale" in str(self.client.base_url)

            if not (is_reasoning_model or is_speciale_url):
                kwargs["temperature"] = 0

            response = self.client.chat.completions.create(**kwargs)
            return response
            
        except Exception as e:
            print(f"An error occurred while calling DeepSeek API: {e}")
            return None