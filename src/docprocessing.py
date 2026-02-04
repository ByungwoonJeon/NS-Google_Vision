import os
import io
import time
import json
from google.cloud import vision
from google.protobuf.json_format import MessageToDict

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

    def run(self, image_path, save_json_path=None):
        """
        이미지를 받아 문서 특화 OCR(Document Text Detection)을 수행합니다.
        """
        file_name = os.path.basename(image_path)
        start_time = time.time()

        try:
            with io.open(image_path, 'rb') as image_file:
                content = image_file.read()

            image = vision.Image(content=content)
            
            # [핵심 수정] text_detection -> document_text_detection 으로 변경
            # 문서나 빽빽한 텍스트 인식률이 훨씬 좋습니다.
            response = self.client.document_text_detection(image=image)
            
            if response.error.message:
                raise Exception(f"Google API 반환 에러: {response.error.message}")

            # JSON 파일 저장 (한글 보존 + 들여쓰기)
            if save_json_path:
                try:
                    response_dict = MessageToDict(response._pb) if hasattr(response, '_pb') else MessageToDict(response)
                    with open(save_json_path, "w", encoding="utf-8") as f:
                        json.dump(response_dict, f, ensure_ascii=False, indent=4)
                except Exception as json_e:
                    self.logger.warning(f"[OCR] JSON 저장 실패 ({file_name}): {json_e}")

            elapsed = time.time() - start_time
            
            # document_text_detection도 결과 포맷(text_annotations)은 호환되므로
            # 기존 postprocessing.py를 수정할 필요 없이 그대로 쓸 수 있습니다.
            return response.text_annotations

        except Exception as e:
            self.logger.error(f"[OCR] 처리 실패 ('{file_name}'): {e}")
            raise e