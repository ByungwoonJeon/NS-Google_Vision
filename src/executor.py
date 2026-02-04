import sys
import json
import os
import time
import logging
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from preprocessing import PreProcessor
from docprocessing import DocProcessor
from postprocessing import PostProcessor
from image_handler import ImageHandler

MASTER_PATHS = {
    'general_ban': r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input\공산품_금칙어.xlsx",
    'general_ftc': r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input\공산품_공정위.xlsx",
    'general_except': r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input\예외사항문구.xlsx",
    'food_ban': r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input\식품_금칙어.xlsx",
    'food_ftc': r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input\식품_공정위.xlsx"
}

def setup_logger(base_log_dir):
    try:
        now = datetime.now()
        folder_name = now.strftime("%Y%m%d_%H%M")
        log_dir = os.path.join(base_log_dir, folder_name)
        os.makedirs(log_dir, exist_ok=True)
        log_file_path = os.path.join(log_dir, "process_log.txt")
        
        logger = logging.getLogger("NS_OCR_Main")
        logger.setLevel(logging.INFO)
        if logger.hasHandlers(): logger.handlers.clear()

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        formatter = logging.Formatter('[%(asctime)s] [%(threadName)s] %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        
        return logger, log_dir
    except Exception as e:
        print(f"FATAL ERROR: 로그 폴더 생성 실패 - {e}")
        return None, None

def setup_product_logger(product_code, output_dir):
    logger = logging.getLogger(f"Log_{product_code}")
    logger.setLevel(logging.INFO)
    if logger.hasHandlers(): logger.handlers.clear()

    log_path = os.path.join(output_dir, f"{product_code}_process.log")
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    formatter = logging.Formatter(f'[%(asctime)s] [%(threadName)s] [%(levelname)s] [{product_code}] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

def process_single_product(product_folder_path, category, config, modules, main_logger):
    product_code = os.path.basename(product_folder_path)
    # [중요] 결과 파일들이 저장될 경로
    product_output_dir = os.path.join(config['base_output_dir'], product_code)
    os.makedirs(product_output_dir, exist_ok=True)

    logger = setup_product_logger(product_code, product_output_dir)
    logger.info(f">>> [처리 시작] 경로: {product_folder_path}")

    try:
        pre_proc, doc_proc, post_proc, img_handler = modules
        task = {"product_code": product_code, "category": category, "image_folder_path": product_folder_path}

        idp_items = pre_proc.create_idp_items(task, config)
        if not idp_items:
            logger.warning("이미지 없음.")
            return {"code": product_code, "status": "SKIP"}

        processed_img_paths, all_issues = [], []
        next_start_index = 1 

        for item_id, item in idp_items.items():
            try:
                # [수정됨] JSON 파일도 결과 폴더에 저장되도록 경로 설정
                json_filename = f"{item['file_name']}_ocr.json"
                json_save_path = os.path.join(product_output_dir, json_filename)

                # OCR 수행 (JSON 경로 전달)
                item['ocr_data'] = doc_proc.run(item['input_path'], save_json_path=json_save_path)
                
                # 후처리 수행
                issues, saved_path = post_proc.process_one_image(item, start_index=next_start_index, logger=logger)
                
                next_start_index += len(issues)
                all_issues.extend(issues)
                processed_img_paths.append(saved_path)
            except Exception as e:
                logger.error(f"이미지 에러 ({item['file_name']}): {e}")

        if processed_img_paths:
            final_img_path = os.path.join(product_output_dir, f"{product_code}_Result_merged.png")
            img_handler.merge_and_save(processed_img_paths, final_img_path)
            post_proc.save_excel(all_issues, product_output_dir, product_code, logger=logger)

            for p in processed_img_paths:
                if os.path.exists(p) and "temp_" in os.path.basename(p):
                    try: os.remove(p)
                    except: pass
        
        logger.info(f"<<< [완료] 적발: {len(all_issues)}건")
        return {"code": product_code, "status": "SUCCESS"}
    except Exception as e:
        logger.error(f"치명적 오류: {e}")
        return {"code": product_code, "status": "FAIL"}

def run_rpa_process(args):
    try:
        if isinstance(args, str): args = json.loads(args)
        log_base_path = args.get('strLogPath', r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\log")
        
        main_logger, current_log_dir = setup_logger(log_base_path)
        if not main_logger: return json.dumps({"status": "FAIL"}, ensure_ascii=False)

        main_logger.info("=== RPA START ===")
        input_root = args.get('strInput')
        output_root = args.get('strIOutput')
        api_key_path = args.get('strOcrKey')
        category_raw = args.get('strCategory', 'GENERAL')

        product_folders = [os.path.join(input_root, d) for d in os.listdir(input_root) if os.path.isdir(os.path.join(input_root, d))]
        category = 'FOOD' if '식품' in category_raw or 'FOOD' in category_raw.upper() else 'GENERAL'

        pre_proc = PreProcessor(main_logger)
        doc_proc = DocProcessor(api_key_path, main_logger)
        post_proc = PostProcessor(MASTER_PATHS, main_logger)
        img_handler = ImageHandler(main_logger)
        modules = (pre_proc, doc_proc, post_proc, img_handler)
        config = {"base_output_dir": output_root, "google_key_file": api_key_path, "master_paths": MASTER_PATHS}

        results = []
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="OCR_Worker") as executor:
            futures = [executor.submit(process_single_product, p, category, config, modules, main_logger) for p in product_folders]
            for f in futures: results.append(f.result())

        success_cnt = sum(1 for r in results if r['status'] == 'SUCCESS')
        main_logger.info(f"=== RPA END (성공:{success_cnt}) ===")
        
        return json.dumps({"status": "SUCCESS", "log_path": current_log_dir}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"status": "FAIL", "error": str(e)}, ensure_ascii=False)

if __name__ == "__main__":
    test_args = {
        "strOcrKey": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\auth\vision_key.json",
        "strInput": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input",
        "strIOutput": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\02_output",
        "strCategory": "general",
        "strLogPath": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\log",
        "strReviewType": "사전심의" 
    }
    print(run_rpa_process(test_args))