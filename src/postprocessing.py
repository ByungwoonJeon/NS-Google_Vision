import os
import re
import pandas as pd
import unicodedata
from PIL import Image, ImageDraw, ImageFont

class PostProcessor:
    def __init__(self, master_paths, logger):
        self.main_logger = logger
        self.main_logger.info("[마스터로드] 데이터 로드 시작...")
        self.master_data = self._load_master_data(master_paths)
        self.patterns = self._prepare_patterns()
        self.main_logger.info("[마스터로드] 검사 준비 완료.")

    def _normalize(self, text):
        if not isinstance(text, str): text = str(text)
        return unicodedata.normalize('NFC', text)

    def _load_master_data(self, paths):
        data = {}
        data['ban'] = self._read_file(paths.get('ban'), "금칙어")
        data['ftc'] = self._read_file(paths.get('ftc'), "공정위")
        data['except'] = self._read_file(paths.get('except'), "예외어")
        return data

    def _read_file(self, path, name):
        if not path or not os.path.exists(path):
            if path: 
                self.main_logger.warning(f"[마스터로드] 파일 없음: {name} (경로: {path})")
            return set()
        
        keywords = set()
        try:
            if path.endswith('.xlsx') or path.endswith('.xls'):
                try:
                    dfs = pd.read_excel(path, sheet_name=None)
                    df_list = dfs.values()
                except:
                    return set()
            else:
                try: df_list = [pd.read_csv(path)]
                except: df_list = [pd.read_csv(path, encoding='cp949')]

            for df in df_list:
                for val in df.values.flatten():
                    if pd.isna(val): continue
                    text = str(val)
                    for word in re.split(r'[,;\n]', text):
                        clean_word = re.sub(r'\s+', '', self._normalize(word))
                        if not clean_word or clean_word.isdigit(): continue
                        if len(clean_word) == 1 and '가' <= clean_word <= '힣': continue
                        keywords.add(clean_word)

            self.main_logger.info(f"[마스터로드] '{name}' 로드 완료: {len(keywords)}개 키워드")
            return keywords

        except Exception as e:
            self.main_logger.error(f"[마스터로드] {name} 읽기 에러: {e}")
            return set()

    def _prepare_patterns(self):
        """정규식과 원본 사전 단어를 매핑하기 위해 튜플 리스트 형태로 반환합니다."""
        patterns = {}
        for key in ['except', 'ban', 'ftc']:
            keywords = self.master_data.get(key, set())
            if not keywords:
                patterns[key] = []
                continue
            
            regex_list = []
            for k in sorted(keywords, key=len, reverse=True):
                fuzzy_k = r"[\s\W]*".join(re.escape(c) for c in k)
                if len(k) == 1:
                    pattern = rf"(?<![가-힣]){fuzzy_k}(?![가-힣])"
                else:
                    pattern = fuzzy_k
                regex_list.append((k, re.compile(pattern, re.IGNORECASE)))
            patterns[key] = regex_list
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
            raw_text = " ".join([t.description for t in line])
            full_text = self._normalize(raw_text)
            all_vertices = [v for t in line for v in t.bounding_poly.vertices]
            min_x, min_y = min(v.x for v in all_vertices), min(v.y for v in all_vertices)
            max_x, max_y = max(v.x for v in all_vertices), max(v.y for v in all_vertices)
            structured_lines.append({"text": full_text, "bbox": [min_x, min_y, max_x, max_y], "raw_words": line})
        return structured_lines

    def _get_match_bbox(self, raw_words, match_obj):
        if not match_obj or not raw_words: return None
        start_idx, end_idx = match_obj.start(), match_obj.end()
        current_idx = 0
        target_vertices = []
        for word in raw_words:
            w_len = len(self._normalize(word.description))
            w_start, w_end = current_idx, current_idx + w_len
            if max(start_idx, w_start) < min(end_idx, w_end):
                target_vertices.extend(word.bounding_poly.vertices)
            current_idx += (w_len + 1)
        if not target_vertices: return None
        return [min(v.x for v in target_vertices), min(v.y for v in target_vertices),
                max(v.x for v in target_vertices), max(v.y for v in target_vertices)]

    def _find_all_matches(self, text):
        """한 줄의 텍스트에서 예외어, 금칙어, 공정위 단어를 모두 겹치지 않게 찾아냅니다."""
        all_issues = []
        mask = [False] * len(text)

        for k, pat in self.patterns.get('except', []):
            for m in pat.finditer(text):
                all_issues.append({'type': 'except', 'match': m, 'keyword': k})
                for i in range(m.start(), m.end()): mask[i] = True

        for k, pat in self.patterns.get('ban', []):
            for m in pat.finditer(text):
                if not any(mask[m.start():m.end()]):
                    all_issues.append({'type': 'ban', 'match': m, 'keyword': k})
                    for i in range(m.start(), m.end()): mask[i] = True

        for k, pat in self.patterns.get('ftc', []):
            for m in pat.finditer(text):
                if not any(mask[m.start():m.end()]):
                    all_issues.append({'type': 'ftc', 'match': m, 'keyword': k})
                    for i in range(m.start(), m.end()): mask[i] = True

        all_issues.sort(key=lambda x: x['match'].start())
        return all_issues

    def process_one_image(self, item, start_index=1, logger=None):
        log = logger if logger else self.main_logger
        temp_path = item['temp_path']
        page_number = 0 # [수정] 레거시 호환을 위해 0으로 하드코딩
        ocr_result = item.get('ocr_data', [])
        category_name = item.get('category', '공산품')

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
                
                for line_info in lines:
                    text_content = line_info['text']
                    if not text_content.strip(): continue

                    # [수정] 한 줄에서 모든 키워드를 다 찾아냄
                    found_matches = self._find_all_matches(text_content)
                    
                    for match_info in found_matches:
                        issue_type = match_info['type']
                        matched_obj = match_info['match']
                        dict_word = match_info['keyword']
                        
                        if issue_type == 'except':
                            current_issues.append({
                                "type": "except", "category": category_name,
                                "data": {
                                    "matched_text": matched_obj.group(), 
                                    "page": page_number, 
                                    "dict_word": dict_word
                                }
                            })
                            continue

                        box_color = "red" if issue_type == 'ban' else "blue"
                        
                        precise_bbox = self._get_match_bbox(line_info['raw_words'], matched_obj)
                        if not precise_bbox: x1, y1, x2, y2 = line_info['bbox']
                        else: x1, y1, x2, y2 = precise_bbox
                        
                        # [수정 1 & 2] 박스에 여백(padding)을 주고, 번호(인덱스) 그리는 코드는 삭제
                        padding = 6  # 텍스트를 침범하지 않도록 여백 추가 (필요시 조절)
                        expanded_x1 = max(0, x1 - padding)
                        expanded_y1 = max(0, y1 - padding)
                        expanded_x2 = min(img.width, x2 + padding)
                        expanded_y2 = min(img.height, y2 + padding)
                        
                        draw.rectangle([expanded_x1, expanded_y1, expanded_x2, expanded_y2], outline=box_color, width=4)

                        current_issues.append({
                            "type": issue_type, "category": category_name,
                            "data": {
                                "matched_text": matched_obj.group(), 
                                "page": page_number, 
                                "dict_word": dict_word
                            }
                        })
                        issue_counter += 1

                img.save(temp_path)
            return current_issues, temp_path
            
        except Exception as e:
            log.error(f"상세분석 에러: {e}")
            return [], temp_path

    # [수정] category와 review_type을 필수로 받아 처리하도록 수정
    def save_excel(self, all_issues, output_dir, p_code, category="공산품", review_type="사전", logger=None):
            log = logger if logger else self.main_logger
            try:
                grouped = {'ban': [], 'ftc': [], 'except': []}
                for issue in all_issues: 
                    grouped[issue['type']].append(issue['data'])

                for t_key, t_name in {"ban":"금칙어", "ftc":"공정위", "except":"예외어"}.items():
                    # 심의 타입(사전/사후)에 따른 폴더 및 파일명 분기
                    if review_type == "사후":
                        target_dir = os.path.join(output_dir, t_name)
                        os.makedirs(target_dir, exist_ok=True)
                        # [수정 3] _py 삭제
                        file_name = f"{category}_{review_type}_{t_name}리스트_Result_final.xlsx"
                    else:
                        target_dir = output_dir
                        # [수정 3] _py 삭제
                        file_name = f"{category}_{review_type}_{t_name} 리스트_Result_final.xlsx"
                    
                    save_path = os.path.join(target_dir, file_name)

                    if self.master_data.get(t_key):
                        type_issues = grouped[t_key]
                        
                        tokens_A = []  
                        pages_C = []   
                        unique_D = []  
                        seen_D = set()
                        
                        for data in type_issues:
                            matched_text = data['matched_text'].strip()
                            page_num = data['page']
                            
                            # 한글/영문/숫자 및 특수문자 개별 분리
                            tokens = re.findall(r'[A-Za-z0-9가-힣]+|[^\w\s]', matched_text)
                            
                            for tk in tokens:
                                tokens_A.append(tk)
                                pages_C.append(page_num)
                                
                            dict_word_clean = data['dict_word'].replace(" ", "")
                            if dict_word_clean not in seen_D:
                                seen_D.add(dict_word_clean)
                                unique_D.append(dict_word_clean)
                                
                        max_len = max(len(tokens_A), len(unique_D))
                        
                        if max_len == 0:
                            df = pd.DataFrame(columns=['단어', '실증자료여부 표시', '페이지 번호', '금지어 또는 한정표현 사전 단어'])
                        else:
                            df = pd.DataFrame({
                                '단어': tokens_A + [''] * (max_len - len(tokens_A)),
                                '실증자료여부 표시': [''] * max_len,
                                '페이지 번호': pages_C + [''] * (max_len - len(pages_C)),
                                '금지어 또는 한정표현 사전 단어': unique_D + [''] * (max_len - len(unique_D))
                            })
                        
                        df.to_excel(save_path, index=False)
                        log.info(f"[결과저장] {os.path.basename(save_path)} 저장 완료 (출력 길이: {max_len}행)")

                    else:
                        # 마스터 데이터가 없을 경우 빈 파일 생성
                        if not os.path.exists(save_path):
                            pd.DataFrame(columns=['단어', '실증자료여부 표시', '페이지 번호', '금지어 또는 한정표현 사전 단어']).to_excel(save_path, index=False)
                            log.info(f"[결과저장] {os.path.basename(save_path)} 빈 파일 생성")

            except Exception as e:
                log.error(f"엑셀 저장 실패: {e}")