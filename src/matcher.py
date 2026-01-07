import os
import json
from PIL import Image, ImageDraw

# 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_PATH = os.path.join(BASE_DIR, '02_output', 'ocr_debug.json')
IMAGE_PATH = os.path.join(BASE_DIR, '01_input', 'test.jpg')
OUTPUT_IMAGE_PATH = os.path.join(BASE_DIR, '02_output', 'marked_result.png')

# 임시 금칙어 리스트 (나중에는 엑셀에서 불러옵니다)
# 결과에 포함된 단어로 테스트해 보세요.
PROHIBITED_WORDS = ['사과', '농산물', '원재료'] 

def run_matching_and_marking():
    # 1. JSON 데이터 로드
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    annotations = data.get('textAnnotations', [])
    if not annotations:
        print("검색된 텍스트가 없습니다.")
        return

    # 2. 이미지 불러오기
    img = Image.open(IMAGE_PATH)
    draw = ImageDraw.Draw(img)
    
    found_count = 0
    print(f"🔍 금칙어 검색 시작: {PROHIBITED_WORDS}")

    # 3. 개별 단어 대조 (index 1번부터가 개별 단어입니다)
    for item in annotations[1:]:
        word = item.get('description', '')
        
        # 금칙어가 포함되어 있는지 확인
        if any(p_word in word for p_word in PROHIBITED_WORDS):
            print(f"🚩 위반 단어 발견: {word}")
            
            # 좌표 추출 (4개 꼭짓점)
            vertices = item.get('boundingPoly', {}).get('vertices', [])
            if len(vertices) == 4:
                # 박스 그리기용 좌표 정리 (좌상단 x,y / 우하단 x,y)
                x_points = [v.get('x', 0) for v in vertices]
                y_points = [v.get('y', 0) for v in vertices]
                shape = [min(x_points), min(y_points), max(x_points), max(y_points)]
                
                # 빨간색 사각형 그리기 (두께 3)
                draw.rectangle(shape, outline="red", width=3)
                found_count += 1

    # 4. 결과 저장
    if found_count > 0:
        img.save(OUTPUT_IMAGE_PATH)
        print(f"✅ 마킹 완료! {found_count}개의 위반 사항을 표시했습니다.")
        print(f"결과 이미지 확인: {OUTPUT_IMAGE_PATH}")
    else:
        print("검출된 위반 사항이 없습니다.")

if __name__ == "__main__":
    run_matching_and_marking()