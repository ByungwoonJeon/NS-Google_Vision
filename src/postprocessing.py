import os
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

class PostProcessor:
    def __init__(self, master_paths, logger):
        self.logger = logger
        self.logger.info("[마스터로드] 데이터(금칙어 엑셀) 로드 시작...")
        self.master_data = self._load_master_data(master_paths)

    def _load_master_data(self, paths):
        data = {"general": {}, "food": {}}
        try:
            data['general']['ban'] = self._read_file(paths['general_ban'], "공산품_금칙어")
            data['general']['ftc'] = self._read_file(paths['general_ftc'], "공산품_공정위")
            data['general']['except'] = self._read_file(paths['general_except'], "공산품_예외어")
            
            data['food']['ban'] = self._read_file(paths['food_ban'], "식품_금칙어")
            data['food']['ftc'] = self._read_file(paths['food_ftc'], "식품_공정위")
            
            self.logger.info("[마스터로드] 모든 마스터 데이터 로드 완료.")
            return data
        except Exception as e:
            self.logger.error(f"[마스터로드] 로드 실패: {e}")
            raise e

    def _read_file(self, path, name):
        if not path or not os.path.exists(path):
            self.logger.warning(f"[마스터로드] 파일 없음: {name} ({path})")
            return set()
        try:
            if path.endswith('.csv'):
                df = pd.read_csv(path)
            else:
                df = pd.read_excel(path)
            
            keywords = {str(x).replace(" ", "") for x in df.iloc[:, 0].dropna() if str(x).strip()}
            self.logger.info(f"[마스터로드] - [{name}] 로드 완료 ({len(keywords)}건)")
            return keywords
        except Exception as e:
            self.logger.error(f"[마스터로드] - [{name}] 읽기 에러: {e}")
            return set()

    def process_one_image(self, item, start_index=1):
        file_name = item['file_name']
        log_head = item['log_prefix'] # 예: [1/2][파일명]
        
        ocr_texts = item['ocr_data']
        if not ocr_texts:
            self.logger.warning(f"[상세분석] {log_head} OCR 결과 없음")
            return [], item['input_path']

        category = item['category']
        self.logger.info(f"[상세분석] {log_head} 분석 시작 (시작 번호: {start_index})")
        
        cat_key = 'food' if category == 'FOOD' else 'general'
        ref_data = self.master_data[cat_key]
        
        current_issues = []
        issue_counter = start_index - 1 

        try:
            with Image.open(item['input_path']) as img:
                draw = ImageDraw.Draw(img)
                try:
                    font = ImageFont.truetype("malgun.ttf", 15)
                except:
                    font = ImageFont.load_default()

                for text in ocr_texts[1:]:
                    word_raw = text.description
                    word_clean = word_raw.replace(" ", "")
                    issue_type = None
                    
                    if word_clean in ref_data['ban']:
                        issue_type = "금칙어"
                    elif word_clean in ref_data['ftc']:
                        issue_type = "공정위"
                    
                    if issue_type and category == 'GENERAL':
                        if word_clean in ref_data['except']:
                            issue_type = None

                    if issue_type:
                        issue_counter += 1
                        
                        xs = [v.x for v in text.bounding_poly.vertices]
                        ys = [v.y for v in text.bounding_poly.vertices]
                        pad = 3
                        min_x, max_x = min(xs) - pad, max(xs) + pad
                        min_y, max_y = min(ys) - pad, max(ys) + pad

                        draw.rectangle([min_x, min_y, max_x, max_y], outline="red", width=2)
                        
                        index_str = str(issue_counter)
                        tag_w = len(index_str) * 10 + 10
                        tag_h = 20
                        
                        draw.rectangle([min_x, min_y - tag_h, min_x + tag_w, min_y], fill="red", outline="red")
                        draw.text((min_x + 5, min_y - tag_h + 2), index_str, fill="white", font=font)

                        current_issues.append({
                            "No": issue_counter,
                            "파일명": file_name,
                            "구분": issue_type,
                            "검출단어": word_raw
                        })
                        self.logger.debug(f"[상세분석]   -> [{issue_counter}] 검출: {word_raw}")
                
                img.save(item['temp_path'])
            
            return current_issues, item['temp_path']

        except Exception as e:
            self.logger.error(f"[상세분석] {log_head} 에러: {e}")
            raise e

    def save_excel(self, all_issues, output_dir, p_code):
        try:
            save_path = os.path.join(output_dir, f"{p_code}_Result_final.xlsx")
            columns = ['No', '파일명', '구분', '검출단어']
            df = pd.DataFrame(all_issues if all_issues else [], columns=columns)
            df.to_excel(save_path, index=False)
            self.logger.info(f"[결과저장] 최종 엑셀 저장 완료: {save_path}")
        except Exception as e:
            self.logger.error(f"[결과저장] 엑셀 저장 실패: {e}")