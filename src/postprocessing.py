import os
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

class PostProcessor:
    def __init__(self, master_paths, logger):
        self.logger = logger
        self.logger.info("[마스터로드] 데이터(금칙어/공정위/예외어) 로드 시작...")
        self.master_data = self._load_master_data(master_paths)

    def _load_master_data(self, paths):
        data = {"general": {}, "food": {}}
        try:
            # 공산품(general)
            data['general']['ban'] = self._read_file(paths['general_ban'], "공산품_금칙어")
            data['general']['ftc'] = self._read_file(paths['general_ftc'], "공산품_공정위")
            data['general']['except'] = self._read_file(paths['general_except'], "공산품_예외어")
            
            # 식품(food)
            data['food']['ban'] = self._read_file(paths['food_ban'], "식품_금칙어")
            data['food']['ftc'] = self._read_file(paths['food_ftc'], "식품_공정위")
            
            # 식품 예외어 처리가 필요하다면 여기에 추가 (현재는 공산품 예외어만 있는 것으로 간주)
            data['food']['except'] = set() 

            self.logger.info("[마스터로드] 모든 마스터 데이터 로드 완료.")
            return data
        except Exception as e:
            self.logger.error(f"[마스터로드] 로드 실패: {e}")
            raise e

    def _read_file(self, path, name):
        if not path or not os.path.exists(path):
            self.logger.warning(f"[마스터로드] 파일 없음 또는 경로 미지정: {name} ({path})")
            return set()
        try:
            if path.endswith('.csv'):
                df = pd.read_csv(path)
            else:
                df = pd.read_excel(path)
            # 공백 제거한 키워드 셋 생성
            keywords = {str(x).replace(" ", "") for x in df.iloc[:, 0].dropna() if str(x).strip()}
            self.logger.info(f"[마스터로드] {name}: {len(keywords)}개 로드 완료")
            return keywords
        except Exception as e:
            self.logger.error(f"[마스터로드] {name} 읽기 에러: {e}")
            return set()

    def process_one_image(self, item, start_index=1):
        """
        executor.py에서 호출하는 메인 함수
        이미지 1장을 처리하여 금칙어/공정위/예외어를 마킹하고 결과를 반환합니다.
        """
        file_name = item['file_name']
        temp_path = item['temp_path']
        page_number = item['index'] + 1
        
        # item 딕셔너리에서 필요한 데이터 추출
        ocr_result = item.get('ocr_data', [])
        # 카테고리는 'FOOD' 또는 'GENERAL'로 들어오므로 소문자로 변환
        category_raw = item.get('category', 'general')
        category = 'food' if str(category_raw).lower() == 'food' else 'general'

        current_issues = []
        issue_counter = start_index

        try:
            # OCR 결과가 없거나 실패했을 경우 처리
            if not ocr_result:
                # 원본 이미지를 그대로 복사해두고 빈 결과 반환
                if os.path.exists(item['input_path']):
                    Image.open(item['input_path']).save(temp_path)
                return [], temp_path

            with Image.open(item['input_path']) as img:
                draw = ImageDraw.Draw(img)
                try:
                    font = ImageFont.truetype("malgun.ttf", 15)
                except:
                    font = ImageFont.load_default()

                # 카테고리(general/food) 선택
                target_sets = self.master_data.get(category, {})
                
                ban_set = target_sets.get('ban', set())
                ftc_set = target_sets.get('ftc', set())
                except_set = target_sets.get('except', set())

                # OCR 결과 순회 (첫 번째 요소는 전체 텍스트이므로 건너뜀)
                for text in ocr_result[1:]:
                    word_raw = text.description
                    word_clean = word_raw.replace(" ", "")
                    
                    if not word_clean:
                        continue

                    found_type = None # 'ban', 'ftc', 'except'
                    matched_dict_word = word_clean

                    # 체크 우선순위: 예외어 -> 금칙어 -> 공정위
                    if word_clean in except_set:
                        found_type = 'except'
                    elif word_clean in ban_set:
                        found_type = 'ban'
                    elif word_clean in ftc_set:
                        found_type = 'ftc'

                    if found_type:
                        # 좌표 계산 및 박스 그리기
                        vertices = text.bounding_poly.vertices
                        min_x = min(v.x for v in vertices)
                        min_y = min(v.y for v in vertices)
                        max_x = max(v.x for v in vertices)
                        max_y = max(v.y for v in vertices)

                        color = "red" 
                        draw.rectangle([min_x, min_y, max_x, max_y], outline=color, width=3)
                        
                        index_str = str(issue_counter)
                        tag_w = len(index_str) * 10 + 10
                        tag_h = 20
                        
                        draw.rectangle([min_x, min_y - tag_h, min_x + tag_w, min_y], fill=color, outline=color)
                        draw.text((min_x + 5, min_y - tag_h + 2), index_str, fill="white", font=font)

                        # 결과 데이터 생성
                        current_issues.append({
                            "type": found_type,        
                            "category": category,      
                            "data": {
                                "단어": word_raw,
                                "실증자료여부 표시": "",
                                "페이지 번호": page_number,
                                "금지어 또는 한정표현 사전 단어": matched_dict_word
                            }
                        })
                        
                        self.logger.debug(f"[상세분석]   -> [{issue_counter}] 유형:{found_type} 검출: {word_raw}")
                        issue_counter += 1
                
                img.save(temp_path)
            
            return current_issues, temp_path

        except Exception as e:
            self.logger.error(f"[상세분석] {file_name} 처리 중 에러: {e}")
            try:
                if os.path.exists(item['input_path']):
                    Image.open(item['input_path']).save(temp_path)
            except:
                pass
            return [], temp_path

    def save_excel(self, all_issues, output_dir, p_code):
        """
        최종 결과 엑셀 저장
        검출된 issue들의 type('ban', 'ftc', 'except')에 따라 파일을 나누어 저장합니다.
        """
        try:
            # 데이터 분류용 딕셔너리 초기화
            grouped_issues = {'ban': [], 'ftc': [], 'except': []}
            detected_category = "general" 

            if all_issues:
                # 첫 번째 이슈에서 카테고리 정보를 가져옴 (모두 동일한 상품)
                detected_category = all_issues[0].get('category', 'general')
                
                for issue in all_issues:
                    i_type = issue.get('type')
                    i_data = issue.get('data')
                    if i_type in grouped_issues:
                        grouped_issues[i_type].append(i_data)
            else:
                # 이슈가 하나도 없을 때도 카테고리를 추정해야 한다면 기본값 사용하거나
                # p_code 등을 통해 알 수 있지만, 여기선 'general' 기본값 사용
                pass

            # 파일명 접두사 (general -> 공산품_사전, food -> 식품_사전)
            prefix_map = {
                "general": "공산품_사전",
                "food": "식품_사전"
            }
            file_prefix = prefix_map.get(detected_category, "공산품_사전")

            # 타입별 파일명 매핑
            type_name_map = {
                "ban": "금칙어",
                "ftc": "공정위",
                "except": "예외어"
            }

            columns = ['단어', '실증자료여부 표시', '페이지 번호', '금지어 또는 한정표현 사전 단어']

            # 3가지 타입(금칙어, 공정위, 예외어)에 대해 각각 파일 생성 시도
            for t_key, t_name in type_name_map.items():
                data_list = grouped_issues[t_key]
                
                # 데이터가 있을 때만 파일 저장
                if data_list:
                    full_file_name = f"{file_prefix}_{t_name} 리스트_Result_final.xlsx"
                    save_path = os.path.join(output_dir, full_file_name)
                    
                    df = pd.DataFrame(data_list, columns=columns)
                    df.to_excel(save_path, index=False)
                    self.logger.info(f"[결과저장] {t_name} 리스트 저장 완료: {full_file_name}")
                else:
                    # 데이터가 없으면 파일을 생성하지 않습니다. 
                    # (만약 빈 파일이라도 생성이 필요하면 여기에 빈 DataFrame 저장 로직 추가)
                    pass

        except Exception as e:
            self.logger.error(f"[결과저장] 엑셀 저장 실패: {e}")