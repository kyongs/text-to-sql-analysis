import pandas as pd
import json, os

# 1. 만약 데이터가 'data.json'이라는 파일에 저장되어 있다면 아래 주석을 해제하여 사용하세요.

base_dir = r"C:\Users\domir\Desktop\code\text-to-sql-analysis\outputs"
sub_path = r"20251216_beaver_dw_deepseek_3.2\predictions.json"
full_path = os.path.join(base_dir, sub_path)

with open(full_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# 2. DataFrame으로 변환
df = pd.DataFrame(data)

# 3. CSV 파일로 저장
# encoding='utf-8-sig'는 엑셀에서 열었을 때 한글이나 특수문자 깨짐을 방지합니다.
df.to_csv('output_pandas.csv', index=False, encoding='utf-8-sig')

print("변환 완료: output_pandas.csv")