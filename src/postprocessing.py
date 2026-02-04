import os
import re
import pandas as pd
import unicodedata
from PIL import Image, ImageDraw, ImageFont

class PostProcessor:
    def __init__(self, master_paths, logger):
        self.main_logger = logger
        self.main_logger.info("[마스터로드] 데이터 로드 및 한글 표준화(NFC) 시작...")
        self.master_data = self._load_master_data(master_paths)
        # 패턴 생성 시 master_data가 비어있으면 패턴도 생성되지 않음
        self.patterns = {
            'general': self._prepare_patterns('general'),
            'food': self._prepare_patterns('food')
        }
        self.main_logger.info("[마스터로드] 정규식 엔진 및 라인 클러스터링 준비 완료.")

    def _normalize(self, text):
        if not isinstance(text, str): text = str(text)
        return unicodedata.normalize('NFC', text)

    def _load_master_data(self, paths):
        data = {"general": {}, "food": {}}
        try:
            # 경로가 정확한지 확인하며 로드
            data['general']['ban'] = self._read_file(paths.get('general_ban'), "공산품_금칙어")
            data['general']['ftc'] = self._read_file(paths.get('general_ftc'), "공산품_공정위")
            data['general']['except'] = self._read_file(paths.get('general_except'), "공산품_예외어")
            data['food']['ban'] = self._read_file(paths.get('food_ban'), "식품_금칙어")
            data['food']['ftc'] = self._read_file(paths.get('food_ftc'), "식품_공정위")
            data['food']['except'] = set() 
            return data
        except Exception as e:
            self.main_logger.error(f"[마스터로드] 로드 실패: {e}")
            raise e

    def _read_file(self, path, name):
        """
        [통합 수정] 
        1. 경로 확인
        2. 엑셀 모든 시트/모든 열 읽기
        3. 탭, 특수공백 제거 (Clean)
        """
        if not path or not os.path.exists(path):
            self.main_logger.warning(f"[마스터로드] 파일 없음: {name} (경로: {path})")
            return set()
        
        keywords = set()
        try:
            # 1. 파일 읽기 (모든 시트)
            if path.endswith('.xlsx') or path.endswith('.xls'):
                try:
                    dfs = pd.read_excel(path, sheet_name=None)
                    df_list = dfs.values()
                except:
                    # openpyxl 없거나 에러 시 CSV로 시도해볼 수 있으나 보통은 여기서 해결됨
                    self.main_logger.warning(f"[마스터로드] {name} 엑셀 읽기 실패. 포맷 확인 필요.")
                    return set()
            else:
                try: df_list = [pd.read_csv(path)]
                except: df_list = [pd.read_csv(path, encoding='cp949')]

            # 2. 데이터 정제
            for df in df_list:
                for val in df.values.flatten():
                    if pd.isna(val): continue
                    text = str(val)
                    # 쉼표, 줄바꿈, 세미콜론 등으로 구분된 단어 분리
                    for word in re.split(r'[,;\n]', text):
                        # 공백/탭/특수문자 모두 제거
                        clean_word = re.sub(r'\s+', '', self._normalize(word))
                        # 숫자만 있는 경우(순번) 제외
                        if clean_word and not clean_word.isdigit():
                            keywords.add(clean_word)

            self.main_logger.info(f"[마스터로드] '{name}' 로드 완료: {len(keywords)}개 키워드")
            return keywords

        except Exception as e:
            self.main_logger.error(f"[마스터로드] {name} 읽기 에러: {e}")
            return set()

    def _prepare_patterns(self, category):
        target_sets = self.master_data.get(category, {})
        patterns = {}
        for key in ['except', 'ban', 'ftc']:
            keywords = target_sets.get(key, set())
            if not keywords: patterns[key] = None; continue
            
            regex_parts = []
            for k in sorted(keywords, key=len, reverse=True):
                # 글자 사이 공백(Space, Tab, Newline 등) 허용 패턴 생성
                fuzzy_k = r"[\s\W]*".join(re.escape(c) for c in k)
                if len(k) == 1:
                    pattern = rf"(?<![가-힣]){fuzzy_k}(?![가-힣])"
                else:
                    pattern = fuzzy_k
                regex_parts.append(pattern)
            patterns[key] = re.compile("|".join(regex_parts), re.IGNORECASE)
        return patterns

    def _group_into_lines(self, ocr_texts):
        if not ocr_texts: return []
        sorted_texts = sorted(ocr_texts, key=lambda t: min(v.y for v in t.bounding_poly.vertices))
        
        lines = []
        current_line = []
        last_y = -1
        y_tolerance = 15

        for text in sorted_texts:
            vertices = text.bounding_poly.vertices
            min_y, max_y = min(v.y for v in vertices), max(v.y for v in vertices)
            center_y = (min_y + max_y) / 2

            if last_y == -1:
                current_line.append(text)
                last_y = center_y
            else:
                if abs(center_y - last_y) <= y_tolerance:
                    current_line.append(text)
                else:
                    lines.append(current_line)
                    current_line = [text]
                    last_y = center_y
        if current_line: lines.append(current_line)

        structured_lines = []
        for line in lines:
            line.sort(key=lambda t: min(v.x for v in t.bounding_poly.vertices))
            # [중요] join할 때 공백을 넣었으므로, 좌표 계산 시에도 공백을 고려해야 함
            raw_text = " ".join([t.description for t in line])
            full_text = self._normalize(raw_text)
            
            all_vertices = [v for t in line for v in t.bounding_poly.vertices]
            min_x, min_y = min(v.x for v in all_vertices), min(v.y for v in all_vertices)
            max_x, max_y = max(v.x for v in all_vertices), max(v.y for v in all_vertices)
            
            structured_lines.append({"text": full_text, "bbox": [min_x, min_y, max_x, max_y], "raw_words": line})
        return structured_lines

    def _get_match_bbox(self, raw_words, match_obj):
        """
        Regex Match 객체의 위치를 기반으로 실제 단어들의 Bbox 계산
        """
        if not match_obj or not raw_words: return None
        start_idx = match_obj.start()
        end_idx = match_obj.end()
        current_idx = 0
        target_vertices = []
        
        for word in raw_words:
            w_len = len(self._normalize(word.description))
            w_start = current_idx
            w_end = current_idx + w_len
            
            # Intersection 확인
            if max(start_idx, w_start) < min(end_idx, w_end):
                for v in word.bounding_poly.vertices: target_vertices.append(v)
            
            current_idx += (w_len + 1) # 공백 포함

        if not target_vertices: return None
        return [min(v.x for v in target_vertices), min(v.y for v in target_vertices),
                max(v.x for v in target_vertices), max(v.y for v in target_vertices)]

    def process_one_image(self, item, start_index=1, logger=None):
        log = logger if logger else self.main_logger
        temp_path = item['temp_path']
        page_number = item['index'] + 1
        ocr_result = item.get('ocr_data', [])
        
        category_raw = str(item.get('category', 'general')).lower()
        category = 'food' if 'food' in category_raw else 'general'
        review_type_raw = str(item.get('review_type', '사전심의'))
        is_pre_review = '사전' in review_type_raw 

        current_issues = []
        issue_counter = start_index

        try:
            if not ocr_result:
                if os.path.exists(item['input_path']): Image.open(item['input_path']).save(temp_path)
                return [], temp_path

            with Image.open(item['input_path']) as img:
                draw = ImageDraw.Draw(img)
                try: font = ImageFont.truetype("malgun.ttf", 20)
                except: font = ImageFont.load_default()

                lines = self._group_into_lines(ocr_result[1:])
                cat_patterns = self.patterns.get(category)

                for line_info in lines:
                    text_content = line_info['text']
                    if not text_content.strip(): continue

                    # [Step 1] 예외어(Except) 확인 -> 엑셀 저장 O, 마킹 X, 건너뛰기
                    except_match = cat_patterns['except'].search(text_content) if cat_patterns['except'] else None
                    if except_match:
                        current_issues.append({
                            "type": "except", 
                            "category": category,
                            "data": {
                                "단어": except_match.group(),
                                "실증자료여부 표시": "", 
                                "페이지 번호": page_number,
                                "금지어 또는 한정표현 사전 단어": except_match.group()
                            }
                        })
                        continue 

                    # [Step 2] 금칙어/공정위 검사
                    matched_obj = None 
                    found_type = None
                    box_color = ""

                    ban_match = cat_patterns['ban'].search(text_content) if cat_patterns['ban'] else None
                    if ban_match:
                        found_type = 'ban'
                        matched_obj = ban_match
                        box_color = "red"
                    else:
                        if is_pre_review:
                            ftc_match = cat_patterns['ftc'].search(text_content) if cat_patterns['ftc'] else None
                            if ftc_match:
                                found_type = 'ftc'
                                matched_obj = ftc_match
                                box_color = "blue"

                    # [Step 3] 결과 마킹
                    if found_type and matched_obj:
                        precise_bbox = self._get_match_bbox(line_info['raw_words'], matched_obj)
                        if not precise_bbox: x1, y1, x2, y2 = line_info['bbox']
                        else: x1, y1, x2, y2 = precise_bbox
                        
                        draw.rectangle([x1, y1, x2, y2], outline=box_color, width=4)
                        index_str = str(issue_counter)
                        
                        if hasattr(font, 'getbbox'):
                            bbox = font.getbbox(index_str)
                            text_w, text_h = bbox[2], bbox[3]
                        else:
                            text_w, text_h = font.getsize(index_str)
                        
                        tag_w, tag_h = text_w + 10, text_h + 10
                        draw.rectangle([x1, y1 - tag_h, x1 + tag_w, y1], fill=box_color)
                        draw.text((x1 + 5, y1 - tag_h + 2), index_str, fill="white", font=font)

                        current_issues.append({
                            "type": found_type, 
                            "category": category,
                            "data": {
                                "단어": matched_obj.group(),
                                "실증자료여부 표시": "",
                                "페이지 번호": page_number,
                                "금지어 또는 한정표현 사전 단어": matched_obj.group()
                            }
                        })
                        issue_counter += 1

                img.save(temp_path)
            return current_issues, temp_path
            
        except Exception as e:
            log.error(f"상세분석 에러: {e}")
            return [], temp_path

    def save_excel(self, all_issues, output_dir, p_code, logger=None):
        log = logger if logger else self.main_logger
        try:
            grouped = {'ban': [], 'ftc': [], 'except': []}
            cat = all_issues[0].get('category', 'general') if all_issues else 'general'
            for issue in all_issues: grouped[issue['type']].append(issue['data'])

            prefix = "식품" if cat == "food" else "공산품"
            cols = ['단어', '실증자료여부 표시', '페이지 번호', '금지어 또는 한정표현 사전 단어']
            
            # [수정] 사전/사후 파일명 구분 (executor에서 넘겨받은 review_type 활용 가능하지만 여기선 심플하게)
            # 파일이 덮어써지지 않게 주의 필요
            
            for t_key, t_name in {"ban":"금칙어", "ftc":"공정위", "except":"예외어"}.items():
                save_path = os.path.join(output_dir, f"{prefix}_{t_name} 리스트_Result_final.xlsx")
                df = pd.DataFrame(grouped[t_key], columns=cols)
                # 데이터가 없어도 빈 파일 생성 (사용자 확인용)
                df.to_excel(save_path, index=False)
                log.info(f"[결과저장] {t_name} 저장 완료 ({len(df)}건)")

        except Exception as e:
            log.error(f"엑셀 저장 실패: {e}")