import os
from PIL import Image

class ImageHandler:
    def __init__(self, logger):
        self.logger = logger

    def merge_and_save(self, image_paths, output_path):
        if not image_paths:
            self.logger.warning("[이미지병합] 병합할 이미지가 없습니다.")
            return

        try:
            self.logger.info(f"[이미지병합] 총 {len(image_paths)}장의 이미지를 병합합니다.")
            
            images = []
            for p in image_paths:
                try:
                    images.append(Image.open(p))
                except Exception as img_e:
                    self.logger.error(f"[이미지병합] 이미지 열기 실패 ({p}): {img_e}")

            if not images:
                return

            max_width = max(img.width for img in images)
            total_height = sum(img.height for img in images)
            
            merged_img = Image.new('RGB', (max_width, total_height), (255, 255, 255))
            y_offset = 0
            for img in images:
                merged_img.paste(img, (0, y_offset))
                y_offset += img.height
                img.close()
            
            self._save_optimized(merged_img, output_path)

        except Exception as e:
            self.logger.error(f"[이미지병합] 병합 중 치명적 오류: {e}")

    def _save_optimized(self, image_obj, save_path, max_mb=30):
        target_size = max_mb * 1024 * 1024
        quality = 95
        resize_ratio = 0.9
        
        try:
            image_obj.save(save_path, quality=quality)
            current_size = os.path.getsize(save_path)
            self.logger.info(f"[결과저장] 1차 저장 완료. 크기: {current_size/1024/1024:.2f}MB")

            while current_size > target_size:
                self.logger.info(f"[용량최적화] 제한({max_mb}MB) 초과 -> 압축 진행 중...")
                
                if quality > 80:
                    quality -= 5
                    image_obj.save(save_path, quality=quality, optimize=True)
                else:
                    w, h = image_obj.size
                    new_w, new_h = int(w * resize_ratio), int(h * resize_ratio)
                    self.logger.info(f"[용량최적화]   -> 해상도 리사이징 ({w}x{h} -> {new_w}x{new_h})")
                    image_obj = image_obj.resize((new_w, new_h), Image.Resampling.LANCZOS)
                    image_obj.save(save_path, quality=quality, optimize=True)
                
                current_size = os.path.getsize(save_path)
            
            self.logger.info(f"[결과저장] 최종 저장 완료 ({current_size/1024/1024:.2f}MB): {os.path.basename(save_path)}")
            
        except Exception as e:
            self.logger.error(f"[결과저장] 저장/압축 중 에러: {e}")