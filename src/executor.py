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
        
        logger = logging.getLogger("NS_OCR_Logger")
        logger.setLevel(logging.INFO)
        if logger.hasHandlers():
            logger.handlers.clear()

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        
        return logger, log_dir
    except Exception as e:
        print(f"FATAL ERROR: 로그 폴더 생성 실패 - {e}")
        return None, None

def process_single_product(product_folder_path, category, config, modules, logger):
    product_code = os.path.basename(product_folder_path)
    logger.info(f"[상품처리] >>> 코드: {product_code} (경로: {product_folder_path})")

    try:
        pre_proc, doc_proc, post_proc, img_handler = modules
        
        task = {
            "product_code": product_code,
            "category": category,
            "image_folder_path": product_folder_path
        }

        idp_items = pre_proc.create_idp_items(task, config)
        if not idp_items:
            return {"code": product_code, "status": "SKIP", "msg": "이미지 없음"}

        processed_img_paths = []
        all_issues = []
        
        next_start_index = 1 

        for item_id, item in idp_items.items():
            try:
                item['ocr_data'] = doc_proc.run(item['input_path'])
                
                issues, saved_path = post_proc.process_one_image(item, start_index=next_start_index)
                
                next_start_index += len(issues)
                
                item['issues'] = issues
                all_issues.extend(issues)
                processed_img_paths.append(saved_path)
                
            except Exception as e:
                logger.error(f"[상품처리] 이미지 처리 에러 ({item['file_name']}): {e}")

        if processed_img_paths:
            final_img_path = os.path.join(config['base_output_dir'], product_code, f"{product_code}_Result_merged.png")
            img_handler.merge_and_save(processed_img_paths, final_img_path)

            post_proc.save_excel(all_issues, os.path.join(config['base_output_dir'], product_code), product_code)

            for p in processed_img_paths:
                if os.path.exists(p) and "temp_" in os.path.basename(p):
                    try:
                        os.remove(p)
                    except:
                        pass
        
        logger.info(f"[상품처리] <<< 완료 (코드: {product_code}, 적발: {len(all_issues)}건)")
        return {"code": product_code, "status": "SUCCESS", "issues": len(all_issues)}

    except Exception as e:
        logger.error(f"[상품처리] 치명적 에러 ({product_code}): {e}")
        logger.error(traceback.format_exc())
        return {"code": product_code, "status": "FAIL", "msg": str(e)}


def run_rpa_process(args):
    logger = None
    try:
        if isinstance(args, str):
            args = json.loads(args)
            
        log_base_path = args.get('strLogPath', r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\Logs")
        
        logger, current_log_dir = setup_logger(log_base_path)
        if not logger:
            return json.dumps({"status": "FAIL", "msg": "로그 폴더 생성 실패"}, ensure_ascii=False)

        logger.info("===================================================")
        logger.info("[전체프로세스] >>> RPA 요청 수신 및 시작")
        
        input_root = args.get('strInput')
        output_root = args.get('strIOutput')
        api_key_path = args.get('strOcrKey')
        category_raw = args.get('strCategory', 'GENERAL')

        if not input_root or not os.path.exists(input_root):
            logger.error(f"[전체설정] 입력 경로가 존재하지 않습니다: {input_root}")
            return json.dumps({"status": "FAIL", "msg": "Input Path Error"}, ensure_ascii=False)

        product_folders = [
            os.path.join(input_root, d) 
            for d in os.listdir(input_root) 
            if os.path.isdir(os.path.join(input_root, d))
        ]
        
        logger.info(f"[전체설정] 작업 대상 루트 폴더: {input_root}")
        logger.info(f"[전체설정] 발견된 상품 폴더 개수: {len(product_folders)}개")
        
        if not product_folders:
            logger.warning("[전체설정] 처리할 하위 상품 폴더가 없습니다.")
            return json.dumps({"status": "SKIP", "msg": "하위 폴더 없음"}, ensure_ascii=False)

        if '식품' in category_raw or 'FOOD' in category_raw.upper():
            category = 'FOOD'
        else:
            category = 'GENERAL'

        logger.info("[모듈초기화] 전처리, OCR, 후처리 모듈 생성 중...")
        pre_proc = PreProcessor(logger)
        doc_proc = DocProcessor(api_key_path, logger)
        post_proc = PostProcessor(MASTER_PATHS, logger)
        img_handler = ImageHandler(logger)
        
        modules = (pre_proc, doc_proc, post_proc, img_handler)
        
        config = {
            "base_output_dir": output_root,
            "google_key_file": api_key_path,
            "master_paths": MASTER_PATHS
        }

        logger.info(f"[병렬처리] 스레드 풀 가동 (대상: {len(product_folders)}건)")
        
        results = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(process_single_product, p_folder, category, config, modules, logger)
                for p_folder in product_folders
            ]
            
            for future in futures:
                results.append(future.result())

        success_cnt = sum(1 for r in results if r['status'] == 'SUCCESS')
        fail_cnt = sum(1 for r in results if r['status'] == 'FAIL')
        
        logger.info(f"[전체프로세스] <<< 완료 (성공: {success_cnt}, 실패: {fail_cnt}, 전체: {len(results)})")
        
        return json.dumps({
            "status": "SUCCESS",
            "log_path": current_log_dir,
            "summary": {
                "total": len(results),
                "success": success_cnt,
                "fail": fail_cnt
            }
        }, ensure_ascii=False)

    except Exception as e:
        err_msg = f"시스템 전체 치명적 오류: {str(e)}"
        if logger:
            logger.critical(f"[시스템에러] {err_msg}")
            logger.critical(traceback.format_exc())
        return json.dumps({"status": "FAIL", "error": str(e)}, ensure_ascii=False)

if __name__ == "__main__":
    test_args = {
        "strOcrKey": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\auth\vision_key.json",
        "strInput": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\01_input",
        "strIOutput": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\02_output",
        "strCategory": "general",
        "strLogPath": r"C:\Archive\01.Work\03.SmartIDP\NS-Google_Vision\log"
    }
    print(run_rpa_process(test_args))