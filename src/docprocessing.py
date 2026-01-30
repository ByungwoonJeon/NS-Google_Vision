import os
import io
import time
from google.cloud import vision

class DocProcessor:
    def __init__(self, key_path, logger):
        self.logger = logger
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
            self.client = vision.ImageAnnotatorClient()
            self.logger.info("[OCR] Google Vision API 클라이언트 인증 성공")
        except Exception as e:
            self.logger.error(f"[OCR] API 인증 실패: {e}")
            raise e

    def run(self, image_path):
        file_name = os.path.basename(image_path)
        self.logger.info(f"[OCR] >>> 요청: '{file_name}' 분석 시작")
        start_time = time.time()

        try:
            with io.open(image_path, 'rb') as image_file:
                content = image_file.read()

            image = vision.Image(content=content)
            response = self.client.text_detection(image=image)
            
            if response.error.message:
                raise Exception(f"Google API 반환 에러: {response.error.message}")

            elapsed = time.time() - start_time
            self.logger.info(f"[OCR] <<< 완료: '{file_name}' ({elapsed:.2f}초 소요)")
            
            return response.text_annotations

        except Exception as e:
            self.logger.error(f"[OCR] 처리 실패 ('{file_name}'): {e}")
            raise e