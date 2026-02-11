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
        patterns = {}
        for key in ['except', 'ban', 'ftc']:
            keywords = self.master_data.get(key, set())
            if not keywords:
                patterns[key] = None
                continue
            
            regex_parts = []
            for k in sorted(keywords, key=len, reverse=True):
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

    def process_one_image(self, item, start_index=1, logger=None):
        log = logger if logger else self.main_logger
        temp_path = item['temp_path']
        page_number = item['index'] + 1
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

                    if self.patterns['except']:
                        match = self.patterns['except'].search(text_content)
                        if match:
                            current_issues.append({
                                "type": "except", "category": category_name,
                                "data": {"단어": match.group(), "실증자료여부 표시": "", "페이지 번호": page_number, "금지어 또는 한정표현 사전 단어": match.group()}
                            })
                            continue 

                    matched_obj = None 
                    found_type = None
                    box_color = ""

                    if self.patterns['ban']:
                        match = self.patterns['ban'].search(text_content)
                        if match:
                            found_type = 'ban'
                            matched_obj = match
                            box_color = "red"
                    
                    if not found_type and self.patterns['ftc']:
                        match = self.patterns['ftc'].search(text_content)
                        if match:
                            found_type = 'ftc'
                            matched_obj = match
                            box_color = "blue"

                    if found_type and matched_obj:
                        precise_bbox = self._get_match_bbox(line_info['raw_words'], matched_obj)
                        if not precise_bbox: x1, y1, x2, y2 = line_info['bbox']
                        else: x1, y1, x2, y2 = precise_bbox
                        
                        draw.rectangle([x1, y1, x2, y2], outline=box_color, width=4)
                        index_str = str(issue_counter)
                        
                        bbox = font.getbbox(index_str) if hasattr(font, 'getbbox') else (0,0, *font.getsize(index_str))
                        text_w, text_h = bbox[2], bbox[3]
                        tag_w, tag_h = text_w + 10, 20
                        
                        draw.rectangle([x1, y1 - tag_h, x1 + tag_w, y1], fill=box_color)
                        draw.text((x1 + 5, y1 - tag_h + 2), index_str, fill="white", font=font)

                        current_issues.append({
                            "type": found_type, "category": category_name,
                            "data": {"단어": matched_obj.group(), "실증자료여부 표시": "", "페이지 번호": page_number, "금지어 또는 한정표현 사전 단어": matched_obj.group()}
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
            cat_prefix = all_issues[0].get('category', p_code) if all_issues else p_code
            
            for issue in all_issues: grouped[issue['type']].append(issue['data'])

            cols = ['단어', '실증자료여부 표시', '페이지 번호', '금지어 또는 한정표현 사전 단어']
            for t_key, t_name in {"ban":"금칙어", "ftc":"공정위", "except":"예외어"}.items():
                
                # [수정] 파일명 뒤에 _py 추가
                save_path = os.path.join(output_dir, f"{cat_prefix}_{t_name} 리스트_Result_final_py.xlsx")
                
                pd.DataFrame(grouped[t_key], columns=cols).to_excel(save_path, index=False)
                log.info(f"[결과저장] {os.path.basename(save_path)} 저장 완료")

        except Exception as e:
            log.error(f"엑셀 저장 실패: {e}")