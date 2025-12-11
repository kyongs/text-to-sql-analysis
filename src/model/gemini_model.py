import os
from typing import Dict, Any
import google.generativeai as genai

# OpenAI의 Usage 객체 구조를 흉내내는 클래스 추가
class MockUsage:
    def __init__(self, prompt_tokens=0, completion_tokens=0, total_tokens=0):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens

class MockMessage:
    def __init__(self, content):
        self.content = content

class MockChoice:
    def __init__(self, content):
        self.message = MockMessage(content)

class MockResponse:
    # usage 인자를 받을 수 있도록 수정
    def __init__(self, content, usage=None):
        self.choices = [MockChoice(content)]
        # usage가 없으면 0으로 채워진 빈 객체를 넣음 (에러 방지)
        self.usage = usage if usage else MockUsage()

class GeminiModel:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.model_config = config['model']

        api_key = os.getenv("GEMINI_API_KEY") 
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set.")

        genai.configure(api_key=api_key)
        self.client = genai.GenerativeModel(self.model_config['name'])

    def generate(self, prompt: str):
        system_prompt = self.model_config.get("system_prompt", "")
        full_prompt = f"{system_prompt}\n\nUser: {prompt}"

        try:
            response = self.client.generate_content(full_prompt)
            
            usage_data = None
            final_text = ""

            if not response.parts:
                final_text = "Error: Blocked or Empty Response"
                # 에러 시 사용량 0
                usage_data = MockUsage()
            else:
                final_text = response.text
                
                # Gemini의 토큰 사용량을 OpenAI 포맷으로 변환
                # (usage_metadata가 존재할 경우)
                if hasattr(response, 'usage_metadata'):
                    usage_data = MockUsage(
                        prompt_tokens=response.usage_metadata.prompt_token_count,
                        completion_tokens=response.usage_metadata.candidates_token_count,
                        total_tokens=response.usage_metadata.total_token_count
                    )
                else:
                    usage_data = MockUsage()

            # usage 데이터를 포함하여 반환
            return MockResponse(final_text, usage=usage_data)

        except Exception as e:
            print(f"Gemini Error: {e}")
            return None