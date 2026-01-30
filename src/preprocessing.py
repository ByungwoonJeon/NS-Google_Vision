import os
import glob

class PreProcessor:
    def __init__(self, logger):
        self.logger = logger

    def create_idp_items(self, task, config):
        p_code = task['product_code']
        input_dir = task['image_folder_path']
        base_output_dir = config['base_output_dir']

        self.logger.info(f"[전처리] 폴더 스캔 시작: {input_dir}")

        all_files = sorted(glob.glob(os.path.join(input_dir, "*")))
        image_files = [f for f in all_files if f.lower().endswith(('.jpg', '.jpeg', '.png'))]

        if not image_files:
            self.logger.warning(f"[전처리] 해당 폴더에 이미지 파일이 없습니다.")
            return {}

        self.logger.info(f"[전처리] 발견된 이미지 파일: {len(image_files)}개")

        product_output_dir = os.path.join(base_output_dir, p_code)
        os.makedirs(product_output_dir, exist_ok=True)
        self.logger.info(f"[전처리] 결과 저장 폴더 생성: {product_output_dir}")

        idp_items = {}
        for idx, file_path in enumerate(image_files):
            file_name = os.path.basename(file_path)
            item_id = f"{p_code}_{idx}"
            
            item = {
                "id": item_id,
                "index": idx,
                "product_code": p_code,
                "category": task['category'],
                "file_name": file_name,
                "input_path": file_path,
                "temp_path": os.path.join(product_output_dir, f"temp_{idx}_{file_name}"),
                "output_dir": product_output_dir,
                "ocr_data": None,
                "issues": [],
                "log_prefix": f"[{idx+1}/{len(image_files)}][{file_name}]"
            }
            idp_items[item_id] = item
        
        self.logger.info(f"[전처리] 작업 객체(Item) {len(idp_items)}개 생성 완료.")
        return idp_items