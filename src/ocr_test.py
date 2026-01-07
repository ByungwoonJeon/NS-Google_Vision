import os
import json
from google.cloud import vision
from google.protobuf.json_format import MessageToDict

# [보안 및 경로 설정]
# os.getcwd() 대신 파일의 실제 위치를 기준으로 경로를 잡습니다.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KEY_PATH = os.path.join(BASE_DIR, 'auth', 'vision_key.json')

# 환경 변수 설정
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = KEY_PATH

def save_ocr_result_to_json_korean():
    print(f"🔍 인증 키 경로 확인: {KEY_PATH}")
    
    # 1. 키 파일이 진짜 그 자리에 있는지 먼저 확인
    if not os.path.exists(KEY_PATH):
        print(f"❌ 에러: 인증 키 파일을 찾을 수 없습니다! 위치를 확인하세요: {KEY_PATH}")
        return

    try:
        print("🚀 구글 비전 API 클라이언트 접속 시도...")
        client = vision.ImageAnnotatorClient()
        
        # 이미지 경로 설정
        image_path = os.path.join(BASE_DIR, '01_input', 'test.jpg')
        
        if not os.path.exists(image_path):
            print(f"❌ 에러: 이미지 파일이 없습니다! 위치: {image_path}")
            return

        with open(image_path, 'rb') as image_file:
            content = image_file.read()
        
        image = vision.Image(content=content)
        print("📡 이미지 분석 중...")
        
        response = client.text_detection(image=image)
        
        # 결과 변환 및 저장
        result_dict = MessageToDict(response._pb)
        output_path = os.path.join(BASE_DIR, '02_output', 'ocr_debug.json')
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, ensure_ascii=False, indent=4)
        
        print(f"✅ 한글 변환 완료! 결과물: {output_path}")

    except Exception as e:
        print(f"❌ 상세 에러 발생: {e}")

if __name__ == "__main__":
    save_ocr_result_to_json_korean()