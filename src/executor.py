import sys
import json
import os
import time
import logging
import traceback
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from PIL import Image  # [추가] PNG -> JPG 안전한 변환 및 저장을 위해 추가

from preprocessing import PreProcessor
from docprocessing import DocProcessor
from postprocessing import PostProcessor
from image_handler import ImageHandler

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

# [Main Processor] 4단계 파이프라인 총괄 실행 로직
def process_single_product(product_folder_path, category, review_type, config, modules, main_logger):
    product_code = os.path.basename(product_folder_path)
    product_output_dir = os.path.join(config['base_output_dir'], product_code)
    os.makedirs(product_output_dir, exist_ok=True)

    logger = setup_product_logger(product_code, product_output_dir)
    logger.info(f">>> [처리 시작] 경로: {product_folder_path}")

    try:
        pre_proc, doc_proc, post_proc, img_handler = modules
        task = {"product_code": product_code, "category": category, "image_folder_path": product_folder_path}

        # [Step 1: Pre-processing] 데이터 및 폴더 스캔
        idp_items = pre_proc.create_idp_items(task, config)
        if not idp_items:
            logger.warning("이미지 없음.")
            return {"code": product_code, "status": "SKIP"}

        processed_img_paths, all_issues = [], []
        next_start_index = 1 

        for item_id, item in idp_items.items():
            try:
                json_filename = f"{item['file_name']}_ocr.json"
                json_save_path = os.path.join(product_output_dir, json_filename)

                # [Step 2: Document Processing] Google Vision API 호출
                item['ocr_data'] = doc_proc.run(item['input_path'], save_json_path=json_save_path)
                
                # [Step 3: Post-processing] 텍스트 매칭 및 바운딩 박스 드로잉
                issues, saved_path = post_proc.process_one_image(item, start_index=next_start_index, logger=logger)
                
                next_start_index += len(issues)
                all_issues.extend(issues)
                processed_img_paths.append(saved_path)
            except Exception as e:
                logger.error(f"이미지 에러 ({item['file_name']}): {e}")

        # [Step 4: Data Export] 병합 이미지 및 엑셀 리포트 추출
        if processed_img_paths:
            active_t_name = config.get("active_t_name", "검사")
            
            if review_type == "사후":
                target_dir = os.path.join(product_output_dir, active_t_name)
                os.makedirs(target_dir, exist_ok=True)
                img_filename = f"{category}_{review_type}_{active_t_name}리스트_Result_Image_BOX.png"
            else:
                target_dir = product_output_dir
                img_filename = f"{category}_{review_type}_{active_t_name} 리스트_Result_Image_BOX.png"
                
            final_img_path = os.path.join(target_dir, img_filename)
            
            # 지정된 경로와 이름으로 이미지 병합 및 저장
            img_handler.merge_and_save(processed_img_paths, final_img_path)

            # ---------------------------------------------------------
            # [추가된 로직] 사후 심의 & 금칙어인 경우 대표 이미지(Result_상품번호.jpg) 추출
            # ---------------------------------------------------------
            if review_type == "사후" and active_t_name == "금칙어":
                root_img_filename = f"Result_{product_code}.jpg"
                root_img_path = os.path.join(product_output_dir, root_img_filename)
                try:
                    # 원본이 png일 수 있으므로 확장자 불일치를 막기 위해 PIL로 변환 후 저장
                    with Image.open(final_img_path) as img:
                        img.convert('RGB').save(root_img_path, quality=95)
                    logger.info(f"[Data Export] 대표 이미지 복사 완료 (루트 경로): {root_img_filename}")
                except Exception as img_e:
                    logger.error(f"[Data Export] 대표 이미지 복사 실패: {img_e}")
            # ---------------------------------------------------------
            
            # 엑셀 저장
            post_proc.save_excel(all_issues, product_output_dir, product_code, category=category, review_type=review_type, logger=logger)

            # 임시 파일 정리
            for p in processed_img_paths:
                if os.path.exists(p) and "temp_" in os.path.basename(p):
                    try: os.remove(p)
                    except: pass
        
        logger.info(f"<<< [완료] 적발: {len(all_issues)}건")
        return {"code": product_code, "status": "SUCCESS"}
    except Exception as e:
        logger.error(f"치명적 오류: {e}")
        return {"code": product_code, "status": "FAIL"}

def parse_custom_rpa_string(input_str):
    if not input_str: return {}
    pattern = re.compile(r"\{([^,]+),([^}]*)\}")
    matches = pattern.findall(input_str)
    parsed_dict = {}
    for key, value in matches:
        parsed_dict[key.strip()] = value.strip()
    return parsed_dict

def get_single_master_path_from_args(args, logger):
    classified = {'ban': '', 'ftc': '', 'except': ''}
    found_path = None
    
    FIXED_EXCEPTION_PATH = r"D:\NS-Google_Vision\예외사항문구.xlsx" 
    
    for key, val in args.items():
        if isinstance(val, str) and len(val) > 4:
            clean_val = val.strip().replace('\\', '/')
            if clean_val.lower().endswith(('.xlsx', '.xls')) and '/' in clean_val:
                found_path = clean_val
                break 
    
    if not found_path:
        logger.warning("[설정] 입력값에서 마스터 엑셀 파일을 찾을 수 없습니다.")
        return classified

    filename = os.path.basename(found_path)
    logger.info(f"[설정] 감지된 마스터 파일: {filename}")

    if os.path.exists(FIXED_EXCEPTION_PATH):
        classified['except'] = FIXED_EXCEPTION_PATH

    if "금칙어" in filename:
        classified['ban'] = found_path
        logger.info(f"   -> [금칙어] 검사 모드 (예외어 강제 포함)")
    elif "공정위" in filename:
        classified['ftc'] = found_path
        logger.info(f"   -> [공정위] 검사 모드 (예외어 강제 포함)")
    elif "예외" in filename:
        classified['except'] = found_path
        logger.info(f"   -> [예외어] 단독 검사 모드")
    else:
        logger.warning(f"   -> 파일명에서 검사 유형을 식별할 수 없습니다.")

    return classified

def run_rpa_process(args):
    try:
        if not args:
            return json.dumps({"status": "FAIL", "error": "입력 인자 없음"}, ensure_ascii=False)

        if isinstance(args, str):
            if args.strip().startswith("{") and "," in args:
                args = parse_custom_rpa_string(args)
            else:
                try: args = json.loads(args)
                except: pass
        
        if not isinstance(args, dict):
             return json.dumps({"status": "FAIL", "error": "입력값 형식 오류"}, ensure_ascii=False)

        log_base_path = args.get('strLogPath', r"D:\NS-Google_Vision\log")
        main_logger, current_log_dir = setup_logger(log_base_path)
        
        if not main_logger: 
            return json.dumps({"status": "FAIL", "error": "로그 폴더 생성 실패"}, ensure_ascii=False)

        main_logger.info("=== RPA START ===")
        main_logger.info(f"[입력값 확인] {args}")

        input_root = args.get('strInput')
        output_root = args.get('strOutput')
        api_key_path = args.get('strOcrKey')

        if not all([input_root, output_root, api_key_path]):
            return json.dumps({"status": "FAIL", "error": "필수 인자(Input/Output/Key) 누락"}, ensure_ascii=False)

        if not os.path.exists(input_root):
             return json.dumps({"status": "FAIL", "error": "입력 폴더 없음"}, ensure_ascii=False)

        raw_cat = args.get('strCategory', '공산품')
        if 'food' in raw_cat.lower():
            category = '식품'
        elif 'general' in raw_cat.lower():
            category = '공산품'
        else:
            category = raw_cat
            
        review_type = args.get('strReviewType', '사전')
        
        main_logger.info(f"[설정] 카테고리 확정: {category}, 심의타입: {review_type} (원본 입력: {raw_cat})")

        master_paths = get_single_master_path_from_args(args, main_logger)
        
        active_t_name = "검사"
        if master_paths.get('ban'): active_t_name = "금칙어"
        elif master_paths.get('ftc'): active_t_name = "공정위"
        elif master_paths.get('except'): active_t_name = "예외어"
        
        pre_proc = PreProcessor(main_logger)
        doc_proc = DocProcessor(api_key_path, main_logger)
        post_proc = PostProcessor(master_paths, main_logger)
        img_handler = ImageHandler(main_logger)
        
        modules = (pre_proc, doc_proc, post_proc, img_handler)
        config = {"base_output_dir": output_root, "google_key_file": api_key_path, "active_t_name": active_t_name}

        product_folders = []
        sub_dirs = [os.path.join(input_root, d) for d in os.listdir(input_root) if os.path.isdir(os.path.join(input_root, d))]
        
        if sub_dirs:
            product_folders = sub_dirs
            main_logger.info(f"[설정] 다중 상품 모드: {len(product_folders)}개 상품 폴더 발견")
        else:
            image_extensions = ('.jpg', '.jpeg', '.png', '.bmp')
            has_images = any(f.lower().endswith(image_extensions) for f in os.listdir(input_root))
            
            if has_images:
                product_folders = [input_root]
                main_logger.info(f"[설정] 단일 상품 모드: 입력 경로 자체({os.path.basename(input_root)})를 작업 대상으로 설정합니다.")
            else:
                main_logger.warning(f"[설정] 처리할 폴더나 이미지 파일을 찾을 수 없습니다: {input_root}")

        results = []
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="OCR_Worker") as executor:
            futures = [executor.submit(process_single_product, p, category, review_type, config, modules, main_logger) for p in product_folders]
            for f in futures: results.append(f.result())

        success_cnt = sum(1 for r in results if r['status'] == 'SUCCESS')
        main_logger.info(f"=== RPA END (성공:{success_cnt}) ===")
        
        return json.dumps({"status": "SUCCESS", "log_path": current_log_dir}, ensure_ascii=False)
        
    except Exception as e:
        err_msg = str(e)
        if 'main_logger' in locals() and main_logger:
            main_logger.error(f"치명적 오류 발생: {traceback.format_exc()}")
        return json.dumps({"status": "FAIL", "error": err_msg}, ensure_ascii=False)

def test_rpa(args):
    return json.dumps({"status": "SUCCESS", "message": "OK"}, ensure_ascii=False)

if __name__ == "__main__":
    KEY_PATH = r"D:\NS-Google_Vision\auth\vision_key.json"
    INPUT_DIR = r"D:\NS-Google_Vision\01_input"
    OUTPUT_DIR = r"D:\NS-Google_Vision\02_output"
    LOG_DIR = r"D:\NS-Google_Vision\log"
    SINGLE_MASTER_FILE = r"D:\NS-Google_Vision\01_input\공산품_사전_공정위 리스트.xlsx"
    
    rpa_input_str = (
        f"{{strOcrKey,{KEY_PATH}}},"
        f"{{strInput,{INPUT_DIR}}},"
        f"{{strOutput,{OUTPUT_DIR}}},"
        f"{{strLogPath,{LOG_DIR}}},"
        f"{{strCategory,공산품}},"
        f"{{strReviewType,사전}}," 
        f"{{strMasterPath,{SINGLE_MASTER_FILE}}}" 
    )

    print(f">>> [테스트] 단일 파일 입력: {os.path.basename(SINGLE_MASTER_FILE)}")
    result = run_rpa_process(rpa_input_str)
    print(f">>> [결과] {result}")