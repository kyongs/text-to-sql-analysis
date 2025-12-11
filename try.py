import os
import sys
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 경로 설정 (src 폴더를 찾기 위해)
sys.path.append(os.getcwd())

try:
    from src.model.gemini_model import GeminiModel
except ImportError:
    print("❌ [Import Error] src/model/gemini_model.py를 찾을 수 없습니다.")
    print("   이 파일(test_gemini.py)이 main.py와 같은 위치에 있는지 확인해주세요.")
    sys.exit(1)

def test_gemini():
    print("----------- 테스트 시작 -----------")

    # 1. API 키 확인
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("❌ [Error] .env 파일에서 GEMINI_API_KEY를 찾을 수 없습니다.")
        return
    print(f"✅ API Key 확인됨: {api_key[:5]}**********")

    # 2. 가짜 Config 생성 (main.py에서 넘어오는 것과 동일한 구조)
    dummy_config = {
        'model': {
            'name': 'gemini-3-pro-preview',  # 혹은 'gemini-pro'
            'provider': 'google',
            'system_prompt': 'You are a SQL expert.'
        }
    }

    # 3. 모델 초기화 테스트
    try:
        print("🔄 모델 초기화 중...")
        model = GeminiModel(dummy_config)
        print("✅ 모델 초기화 성공")
    except Exception as e:
        print(f"❌ [Init Error] 모델 생성 실패: {e}")
        return

    # 4. 생성 테스트
    prompt = "Say 'Hello SQL' only."
    print(f"📤 질문 전송: {prompt}")

    try:
        response = model.generate(prompt)
        
    # ... (앞부분 동일) ...
        
        # 5. 결과 검증
        if response is None:
            print("❌ [Error] 응답이 None입니다.")
        else:
            print("📥 응답 객체 수신됨")
            
            # 1) 내용(Content) 확인
            try:
                content = response.choices[0].message.content
                print(f"✅ [Success] 내용 추출 성공: {content}")
            except AttributeError as e:
                print(f"❌ [Structure Error] Content 구조 에러: {e}")

            # 2) 사용량(Usage) 확인 (이게 아까 에러난 부분!!)
            try:
                # usage가 있는지, 그리고 total_tokens에 접근 가능한지 테스트
                tokens = response.usage.total_tokens
                print(f"✅ [Success] Usage 정보 확인됨: {tokens} tokens")
            except AttributeError as e:
                print(f"❌ [Risk] Usage 정보가 없습니다! main.py에서 에러 날 수 있음: {e}")
                print(f"   현재 객체 속성: {response.__dict__}")

    except Exception as e:
        print(f"❌ [Generate Error] 생성 중 에러 발생: {e}")


if __name__ == "__main__":
    test_gemini()