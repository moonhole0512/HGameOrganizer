import os
import re
import sqlite3
import requests
import json
from bs4 import BeautifulSoup
import customtkinter as ctk
from PIL import Image, ImageTk
from pathlib import Path
import concurrent.futures
from CTkMessagebox import CTkMessagebox
import threading
import queue
import time
from io import BytesIO
import urllib.parse
from send2trash import send2trash

# 안전한 메시지 박스 클래스 추가
class SafeCTkMessagebox(CTkMessagebox):
    """CTkMessagebox의 안전한 버전 - 부모 창이 닫힌 경우 오류 방지"""
    def button_event(self, text):
        """버튼 이벤트 처리 - 부모 창 접근 시 오류 방지"""
        try:
            # 부모 창이 유효한지 확인
            if self.master_window.winfo_exists():
                self.master_window.focus_force()
        except Exception as e:
            print(f"메시지 박스 부모 창 접근 오류: {e}")
            
        # 원래 기능 수행
        self.grab_release()
        self.destroy()
        
        # 결과 반환
        self._running = False
        self.result = text

# 이미지 크기 상수 추가
THUMBNAIL_SIZE = (100, 100)  # 썸네일 크기
CARD_IMAGE_SIZE = (250, 180)  # 카드 이미지 크기

# 비율 유지하며 이미지 크기 조정 함수
def resize_image_maintain_aspect(image, target_size):
    """비율을 유지하면서 이미지 크기 조정"""
    width, height = image.size
    target_width, target_height = target_size
    
    # 가로세로 비율 계산
    aspect = width / height
    target_aspect = target_width / target_height
    
    if aspect > target_aspect:
        # 원본이 더 넓은 경우
        new_width = target_width
        new_height = int(new_width / aspect)
    else:
        # 원본이 더 높은 경우
        new_height = target_height
        new_width = int(new_height * aspect)
    
    return image.resize((new_width, new_height), Image.Resampling.LANCZOS)

class DLGameViewer:
    def __init__(self):
        # 게임 경로 관련 변경
        self.game_paths = []
        self.load_game_paths()  # 저장된 게임 경로 로드
        
        # URL 딕셔너리로 변경
        self.base_urls = {
            "RJ": "https://www.dlsite.com/maniax/work/=/product_id/{}.html",
            "VJ": "https://www.dlsite.com/pro/work/=/product_id/{}.html"  # VJ 코드용 URL
        }
        self.api_urls = {
            "RJ": "https://www.dlsite.com/maniax/product/info/ajax?product_id={}&cdn_cache_min=1",
            "VJ": "https://www.dlsite.com/pro/product/info/ajax?product_id={}&cdn_cache_min=1"  # VJ 코드용 API URL
        }
        self.db_path = "games.db"
        self.skipped_folders = []
        self.invalid_folders = []
        self.no_exe_folders = []  # 실행 파일이 없는 폴더 리스트 추가
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.dlsite.com/',
            'Accept': 'application/json',
            'Cookie': 'locale=ko_KR; locale_flag=1'  # 한국어 설정을 위한 쿠키 추가
        }
        
        self.init_database()
    
    def load_game_paths(self):
        """설정 파일에서 게임 경로 목록 로드"""
        try:
            if os.path.exists("settings.json"):
                with open("settings.json", "r", encoding="utf-8") as f:
                    settings = json.load(f)
                    # 경로를 정규화하여 로드
                    self.game_paths = [os.path.normpath(path) for path in settings.get("game_paths", [])]
            
            # 기본 경로 추가 (설정 파일이 없거나 game_paths가 비어있는 경우)
            if not self.game_paths:
                default_path = r"D:\_AboutHen\_Game"
                if os.path.exists(default_path):
                    self.game_paths = [os.path.normpath(default_path)]
                    self.save_game_paths()
        except Exception as e:
            print(f"게임 경로 로드 중 오류: {e}")
            # 오류 발생 시 기본 경로 사용
            self.game_paths = [os.path.normpath(r"D:\_AboutHen\_Game")]
            self.save_game_paths()
    
    def save_game_paths(self):
        """설정 파일에 게임 경로 목록 저장"""
        try:
            settings = {}
            if os.path.exists("settings.json"):
                with open("settings.json", "r", encoding="utf-8") as f:
                    settings = json.load(f)
            
            # 경로를 정규화하여 저장
            settings["game_paths"] = [os.path.normpath(path) for path in self.game_paths]
            
            with open("settings.json", "w", encoding="utf-8") as f:
                json.dump(settings, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"게임 경로 저장 중 오류: {e}")

    def init_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_code TEXT,
            title TEXT,
            rating REAL,
            circle TEXT,
            work_type TEXT,
            genres TEXT,
            cover_image TEXT,
            folder_path TEXT,
            exe_files TEXT
        )
        ''')
        # 인덱스 추가
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_code ON games (game_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_title ON games (title)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_circle ON games (circle)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_work_type ON games (work_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_genres ON games (genres)')
        conn.commit()
        conn.close()

    def get_rating(self, game_code):
        """API를 통해 평점 정보를 가져옵니다."""
        try:
            # 코드 접두어(RJ 또는 VJ)에 따라 올바른 API URL 선택
            prefix = game_code[:2]
            response = requests.get(
                self.api_urls[prefix].format(game_code),
                headers=self.headers
            )
            response.raise_for_status()
            data = response.json()
            # game_code를 키로 사용하여 중첩된 데이터에서 평점을 가져옵니다
            return float(data[game_code].get('rate_average_2dp', 0))
        except Exception as e:
            print(f"평점 정보 가져오기 실패 {game_code}: {str(e)}")
            return 0.0

    def extract_game_code(self, folder_name):
        # RJ 또는 VJ로 시작하는 코드 모두 인식
        match = re.search(r'([RV]J\d+)', folder_name)
        return match.group(1) if match else None

    def download_images(self, image_urls, game_code, folder_path):
        # 최대 5개의 이미지만 다운로드
        image_urls = image_urls[:5]
        thumb_dir = os.path.join(folder_path, "thumb_imgs")
        os.makedirs(thumb_dir, exist_ok=True)
        
        # 이미지 최대 크기 설정
        MAX_WIDTH = 200
        
        for idx, url in enumerate(image_urls):
            img_path = os.path.join(thumb_dir, f"{idx:02d}.jpg")
            
            # 이미지가 이미 존재하는 경우 건너뛰기
            if os.path.exists(img_path):
                print(f"이미지가 이미 존재함: {img_path}")
                continue
            
            if not url.startswith('http'):
                url = f"https://{url.lstrip('/')}"
            try:
                # requests를 사용한 이미지 다운로드
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()  # 에러 체크
                
                # 이미지 데이터를 메모리에 로드
                img_data = BytesIO(response.content)
                img = Image.open(img_data)
                
                # 이미지 크기 조정 (가로 200px, 세로는 비율 유지)
                width, height = img.size
                if width > MAX_WIDTH:
                    # 비율 계산
                    ratio = MAX_WIDTH / width
                    new_height = int(height * ratio)
                    # 이미지 리사이즈
                    img = img.resize((MAX_WIDTH, new_height), Image.Resampling.LANCZOS)
                
                # 조정된 이미지 저장
                img.save(img_path, 'JPEG', quality=50)
                
                print(f"이미지 다운로드 및 최적화 성공: {url}")
            except Exception as e:
                print(f"이미지 다운로드 실패: {url} - {str(e)}")

    def scrape_game_info(self, game_code, folder_path):
        # 코드 접두어(RJ 또는 VJ)에 따라 올바른 URL 선택
        prefix = game_code[:2]
        url = self.base_urls[prefix].format(game_code)
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 기본 정보 추출
            title = soup.find('h1', {'itemprop': 'name'}).text.strip()
            
            # API를 통해 평점 가져오기
            rating = self.get_rating(game_code)
            
            # 서클명 처리
            circle_element = soup.find('span', {'class': 'maker_name'})
            circle = circle_element.text.strip() if circle_element else "Unknown"
            
            # 작품 형식 처리
            work_type_div = soup.find('div', {'id': 'category_type'})
            work_type = []
            if work_type_div:
                work_type = [span.get('title', '') for span in work_type_div.find_all('span')]
            work_type = ', '.join(filter(None, work_type))
            
            # 장르 처리
            genres_div = soup.find('div', {'class': 'main_genre'})
            genres = []
            if genres_div:
                genres = [a.text for a in genres_div.find_all('a')]
            genres = ', '.join(genres)
            
            # 이미지 URL 추출 및 다운로드
            image_data = soup.find('div', {'class': 'product-slider-data'})
            image_urls = []
            if image_data:
                for div in image_data.find_all('div'):
                    if 'data-src' in div.attrs:
                        image_urls.append(div['data-src'])
            
            cover_image = ""
            if image_urls:
                thumb_dir = os.path.join(folder_path, "thumb_imgs")
                os.makedirs(thumb_dir, exist_ok=True)
                cover_image = os.path.join(folder_path, "thumb_imgs", "00.jpg")
                
                # 이미지가 없거나 손상된 경우 다시 다운로드
                if not os.path.exists(cover_image):
                    self.download_images(image_urls, game_code, folder_path)
            
            return {
                'game_code': game_code,
                'title': title,
                'rating': rating,
                'circle': circle,
                'work_type': work_type,
                'genres': genres,
                'cover_image': cover_image
            }
        except Exception as e:
            print(f"정보 수집 실패 {game_code}: {str(e)}")
            return None

    def get_exe_files(self, folder_path):
        """폴더 내의 모든 exe 파일 목록을 찾습니다."""
        exe_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.exe'):
                    # 전체 경로에서 folder_path를 제외한 상대 경로로 저장
                    relative_path = os.path.relpath(os.path.join(root, file), folder_path)
                    exe_files.append(relative_path)
        return json.dumps(exe_files, ensure_ascii=False)  # JSON 형식으로 저장

    def process_folders(self):
        # 스캔 시작 전에 관련 리스트 초기화
        self.invalid_folders = []
        self.no_exe_folders = []
        self.skipped_folders = []
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # DB에서 현재 등록된 모든 폴더 경로를 가져옴
        cursor.execute("SELECT folder_path FROM games")
        # 경로 슬래시를 윈도우 형식으로 정규화하여 저장
        registered_folders = set([os.path.normpath(row[0]) for row in cursor.fetchall()])
        
        # 모든 게임 경로에 대해 처리
        for game_path in self.game_paths:
            if not os.path.exists(game_path):
                print(f"경로가 존재하지 않습니다: {game_path}")
                continue
                
            for folder_name in os.listdir(game_path):
                folder_path = os.path.join(game_path, folder_name)
                # 경로 슬래시를 윈도우 형식으로 정규화
                folder_path = os.path.normpath(folder_path)
                
                if not os.path.isdir(folder_path):
                    continue
                
                # 이미 DB에 등록된 경로인지 확인 - 정규화된 경로로 비교
                if folder_path in registered_folders:
                    self.skipped_folders.append(folder_name)
                    continue
                    
                game_code = self.extract_game_code(folder_name)
                
                # 게임 코드가 없는 경우 invalid_folders에 추가
                if not game_code:
                    self.invalid_folders.append(folder_path)
                    continue
                
                # 게임 코드 기반 중복 체크 제거 (사용자가 직접 중복체크 버튼으로 관리)
                
                # 새 게임 정보 수집 및 저장
                game_info = self.scrape_game_info(game_code, folder_path)
                if game_info:
                    # exe 파일 목록 가져오기
                    exe_files = self.get_exe_files(folder_path)
                    exe_list = json.loads(exe_files)
                    
                    # exe 파일이 없는 경우 처리
                    if not exe_list:
                        self.no_exe_folders.append(folder_name)
                        continue
                    
                    cursor.execute('''
                    INSERT INTO games 
                    (game_code, title, rating, circle, work_type, genres, cover_image, folder_path, exe_files)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        game_info['game_code'],
                        game_info['title'],
                        game_info['rating'],
                        game_info['circle'],
                        game_info['work_type'],
                        game_info['genres'],
                        game_info['cover_image'],
                        folder_path,  # 정규화된 경로 저장
                        exe_files
                    ))
                    conn.commit()
                    # 등록 완료된 폴더 경로를 추가
                    registered_folders.add(folder_path)
                else:
                    # 정보 수집 실패한 경우도 invalid_folders에 추가
                    self.invalid_folders.append(folder_path)
        
        conn.close()
        
        # 결과 출력
        print("\n=== 처리 결과 ===")
        
        if self.invalid_folders:
            print("\n게임 코드가 없거나 정보 수집에 실패한 폴더:")
            for folder in self.invalid_folders:
                print(f"- {folder}")
        
        if self.no_exe_folders:
            print("\n실행 파일이 없는 폴더:")
            for folder in self.no_exe_folders:
                print(f"- {folder}")
                
        #if self.skipped_folders:
        #    print("\n건너뛴 폴더:")
        #    for folder in self.skipped_folders:
        #        print(f"- {folder}")

class ExeSelectionDialog:
    def __init__(self, parent, exe_files, game_id):
        self.dialog = ctk.CTkToplevel(parent)
        self.dialog.title("실행 파일 선택")
        self.dialog.geometry("400x300")
        
        # 모달 창으로 설정
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        self.exe_files = exe_files
        self.game_id = game_id
        self.selected_exe = None
        
        # 안내 레이블
        ctk.CTkLabel(self.dialog, text="실행할 파일을 선택하세요:").pack(pady=10)
        
        # exe 파일 목록 버튼
        for exe in exe_files:
            btn = ctk.CTkButton(
                self.dialog, 
                text=exe,
                command=lambda x=exe: self.on_select(x)
            )
            btn.pack(pady=5, padx=20, fill="x")
    
    def on_select(self, exe):
        self.selected_exe = exe
        # DB 업데이트 - id로 검색
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE games SET exe_files = ? WHERE id = ?",
            (json.dumps([exe], ensure_ascii=False), self.game_id)
        )
        conn.commit()
        conn.close()
        self.dialog.destroy()

class GameInfoDialog:
    def __init__(self, parent, game, refresh_callback=None):
        self.dialog = ctk.CTkToplevel(parent)
        self.dialog.title(f"게임 정보 - {game[1]}")
        self.dialog.geometry("600x800")
        
        # 모달 창으로 설정
        self.dialog.transient(parent)
        self.dialog.grab_set()
        self.dialog.focus_set()  # 포커스 설정
        
        # 항상 최상위에 표시
        self.dialog.lift()
        self.dialog.attributes('-topmost', True)
        
        self.refresh_callback = refresh_callback
        self.game = game
        self.entries = {}
        
        # 스크롤 가능한 프레임 생성
        self.scroll_frame = ctk.CTkScrollableFrame(self.dialog)
        self.scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 정보 필드 생성
        fields = [
            ("game_code", "게임 코드", 1),  # 인덱스를 1로 변경 (id가 0번)
            ("title", "제목", 2),
            ("rating", "평점", 3),
            ("circle", "서클", 4),
            ("work_type", "작품 형식", 5),
            ("genres", "장르", 6),
            ("cover_image", "커버 이미지 경로", 7),
            ("folder_path", "폴더 경로", 8),
            ("exe_files", "실행 파일", 9)
        ]
        
        for field, label, index in fields:
            frame = ctk.CTkFrame(self.scroll_frame)
            frame.pack(fill="x", padx=5, pady=5)
            
            ctk.CTkLabel(frame, text=label).pack(anchor="w", padx=5)
            
            if field == "cover_image":
                # 커버 이미지 선택을 위한 특별 처리
                entry = ctk.CTkTextbox(frame, height=50)
                entry.pack(fill="x", padx=5, pady=(0,5))
                entry.insert("1.0", str(game[index]))
                
                # 이미지 표시 프레임
                image_frame = ctk.CTkFrame(frame)
                image_frame.pack(fill="x", padx=5, pady=5)
                
                # thumb_imgs 폴더의 이미지들 로드
                thumb_dir = os.path.join(game[8], "thumb_imgs")  # game[8]은 folder_path
                if os.path.exists(thumb_dir):
                    images = []
                    for img_file in sorted(os.listdir(thumb_dir)):
                        if img_file.lower().endswith(('.png', '.jpg', '.jpeg')):
                            img_path = os.path.join(thumb_dir, img_file)
                            try:
                                if os.path.exists(img_path):  # 파일 존재 여부 확인
                                    # PIL 이미지 로드
                                    pil_img = Image.open(img_path)
                                    # 비율 유지하면서 크기 조정
                                    pil_img = resize_image_maintain_aspect(pil_img, THUMBNAIL_SIZE)
                                    
                                    # CTkImage로 변환 (실제 크기 전달)
                                    ctk_img = ctk.CTkImage(
                                        light_image=pil_img, 
                                        dark_image=pil_img, 
                                        size=(pil_img.width, pil_img.height)
                                    )
                                    
                                    # 이미지 버튼 생성
                                    img_btn = ctk.CTkButton(
                                        image_frame,
                                        text="",
                                        image=ctk_img,
                                        width=THUMBNAIL_SIZE[0],
                                        height=THUMBNAIL_SIZE[1],
                                        command=lambda p=img_path, e=entry: self.select_cover_image(p, e)
                                    )
                                    img_btn.image = ctk_img  # 참조 유지
                                    img_btn.pack(side="left", padx=2)
                                    
                                    # 현재 선택된 이미지에 테두리 표시
                                    if img_path == game[7]:  # game[7]은 cover_image
                                        img_btn.configure(border_width=2, border_color="blue")
                                    
                                    images.append(img_btn)
                            except Exception as e:
                                print(f"이미지 로드 실패 ({img_path}): {e}")
            else:
                # 일반 필드
                entry = ctk.CTkTextbox(frame, height=50)
                entry.pack(fill="x", padx=5, pady=(0,5))
                entry.insert("1.0", str(game[index]))
            
            self.entries[field] = entry
        
        # 갱신 버튼
        self.update_btn = ctk.CTkButton(
            self.dialog,
            text="정보 갱신",
            command=self.update_info
        )
        self.update_btn.pack(pady=10)
    
    def refresh_exe_files(self, entry, folder_path):
        """폴더 내의 모든 exe 파일을 다시 검색"""
        exe_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.exe'):
                    relative_path = os.path.relpath(os.path.join(root, file), folder_path)
                    exe_files.append(relative_path)
        
        # 텍스트박스 내용 갱신
        entry.delete("1.0", "end")
        entry.insert("1.0", json.dumps(exe_files, ensure_ascii=False))
    
    def update_info(self):
        """DB 정보 갱신"""
        try:
            conn = sqlite3.connect("games.db")
            cursor = conn.cursor()
            
            update_query = """
            UPDATE games 
            SET game_code=?, title=?, rating=?, circle=?, work_type=?, genres=?, 
                cover_image=?, folder_path=?, exe_files=?
            WHERE id=?
            """
            
            cursor.execute(update_query, (
                self.entries['game_code'].get("1.0", "end-1c"),
                self.entries['title'].get("1.0", "end-1c"),
                float(self.entries['rating'].get("1.0", "end-1c")),
                self.entries['circle'].get("1.0", "end-1c"),
                self.entries['work_type'].get("1.0", "end-1c"),
                self.entries['genres'].get("1.0", "end-1c"),
                self.entries['cover_image'].get("1.0", "end-1c"),
                self.entries['folder_path'].get("1.0", "end-1c"),
                self.entries['exe_files'].get("1.0", "end-1c"),
                self.game[0]  # id를 WHERE 조건으로 사용
            ))
            
            conn.commit()
            conn.close()
            
            SafeCTkMessagebox(title="성공", message="게임 정보가 갱신되었습니다.", icon="check")
            
            # 창 닫기 전에 설정 해제
            self.dialog.attributes('-topmost', False)
            self.dialog.grab_release()
            
            # 콜백 함수 호출하여 화면 갱신
            if self.refresh_callback:
                self.refresh_callback()
                
            self.dialog.destroy()
            
        except Exception as e:
            SafeCTkMessagebox(title="오류", message=f"정보 갱신 중 오류가 발생했습니다:\n{str(e)}", icon="cancel")

    def select_cover_image(self, image_path, entry):
        """커버 이미지 선택 처리"""
        # 텍스트박스 내용 갱신
        entry.delete("1.0", "end")
        entry.insert("1.0", image_path)
        
        # 모든 이미지 버튼의 테두리 초기화
        for widget in entry.master.winfo_children():
            if isinstance(widget, ctk.CTkFrame):  # image_frame
                for btn in widget.winfo_children():
                    if isinstance(btn, ctk.CTkButton):
                        btn.configure(border_width=0)
        
        # 선택된 이미지 버튼에 테두리 표시
        for widget in entry.master.winfo_children():
            if isinstance(widget, ctk.CTkFrame):  # image_frame
                for btn in widget.winfo_children():
                    if isinstance(btn, ctk.CTkButton):
                        if btn.cget("command").__closure__[0].cell_contents == image_path:
                            btn.configure(border_width=2, border_color="blue")

class GameInfoSearchDialog:
    def __init__(self, parent, folder_name, folder_path, callback=None):
        self.dialog = ctk.CTkToplevel(parent)
        self.dialog.title(f"게임 정보 검색 - {folder_name}")
        self.dialog.geometry("800x700")
        
        # 모달 창으로 설정
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        self.folder_name = folder_name
        self.folder_path = folder_path
        self.callback = callback
        self.search_results = []
        self.selected_image_url = None
        self.downloaded_image_path = None
        
        # 스크롤 가능한 프레임 생성
        self.main_frame = ctk.CTkFrame(self.dialog)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 검색 프레임
        search_frame = ctk.CTkFrame(self.main_frame)
        search_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(search_frame, text="게임 제목:").pack(side="left", padx=5)
        self.search_entry = ctk.CTkEntry(search_frame, width=400)
        self.search_entry.pack(side="left", padx=5, fill="x", expand=True)
        self.search_entry.insert(0, folder_name)  # 폴더명을 기본값으로 설정
        self.search_entry.bind('<Return>', self.search_images)  # 엔터 키 바인딩 추가
        
        search_btn = ctk.CTkButton(search_frame, text="이미지 검색", command=self.search_images)
        search_btn.pack(side="left", padx=5)
        
        # 이미지 검색 결과 프레임
        self.image_result_label = ctk.CTkLabel(self.main_frame, text="이미지 검색 결과:")
        self.image_result_label.pack(anchor="w", padx=5, pady=(10, 5))
        
        self.image_frame = ctk.CTkScrollableFrame(self.main_frame, height=250)
        self.image_frame.pack(fill="x", padx=5, pady=5)
        
        # 선택된 이미지 프레임
        self.selected_image_frame = ctk.CTkFrame(self.main_frame)
        self.selected_image_frame.pack(fill="x", padx=5, pady=10)
        
        ctk.CTkLabel(self.selected_image_frame, text="선택된 이미지:").pack(anchor="w", padx=5, pady=5)
        
        self.selected_image_label = ctk.CTkLabel(self.selected_image_frame, text="이미지를 선택하세요", image=None)
        self.selected_image_label.pack(pady=10)
        
        # 수동 입력 프레임
        manual_frame = ctk.CTkFrame(self.main_frame)
        manual_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(manual_frame, text="게임 정보 입력:").pack(anchor="w", padx=5, pady=5)
        
        # 수동 입력 필드들
        fields = [
            ("game_code", "게임 코드 (선택사항)"),
            ("title", "제목"),
            ("circle", "제작사/서클"),
            ("work_type", "작품 형식"),
            ("genres", "장르")
        ]
        
        self.manual_entries = {}
        for field, label in fields:
            field_frame = ctk.CTkFrame(manual_frame)
            field_frame.pack(fill="x", padx=5, pady=2)
            
            ctk.CTkLabel(field_frame, text=label, width=150).pack(side="left", padx=5)
            
            entry = ctk.CTkEntry(field_frame)
            entry.pack(side="left", padx=5, fill="x", expand=True)
            self.manual_entries[field] = entry
            
            # 기본값으로 폴더명 설정 (제목 필드만)
            if field == "title":
                entry.insert(0, folder_name)
        
        # 저장 버튼
        save_btn = ctk.CTkButton(manual_frame, text="저장", command=self.save_manual_info)
        save_btn.pack(pady=10)
        
        # 초기 검색 실행
        self.search_images()
    
    def search_images(self, event=None):
        """이미지 검색 실행"""
        search_term = self.search_entry.get().strip()
        if not search_term:
            return
            
        # 기존 이미지 결과 제거
        for widget in self.image_frame.winfo_children():
            widget.destroy()
        
        # 로딩 표시
        loading_label = ctk.CTkLabel(self.image_frame, text="이미지 검색 중...")
        loading_label.pack(pady=20)
        self.dialog.update()
        
        # 백그라운드에서 검색 실행
        threading.Thread(target=self.perform_image_search, args=(search_term, loading_label), daemon=True).start()
    
    def perform_image_search(self, search_term, loading_label):
        """백그라운드에서 이미지 검색 수행"""
        try:
            # 구글 이미지 검색
            image_urls = self.search_google_images(search_term)
            
            # 결과가 없으면 직접 URL 생성
            if not image_urls:
                print("구글 이미지 검색 결과가 없어 기본 이미지를 생성합니다.")
                # 기본 이미지 URL 몇 개 추가 (게임 관련 일반 이미지)
                image_urls = [
                    f"https://via.placeholder.com/300x200.png?text={urllib.parse.quote(search_term)}",
                    f"https://dummyimage.com/300x200/000/fff&text={urllib.parse.quote(search_term)}"
                ]
            
            # UI 업데이트는 메인 스레드에서
            self.dialog.after(0, lambda: self.display_image_results(image_urls, loading_label))
        except Exception as e:
            self.dialog.after(0, lambda: self.show_error(str(e), loading_label))
    
    def search_google_images(self, search_term):
        """구글에서 이미지 검색"""
        results = []
        try:
            # 검색어 인코딩
            query = urllib.parse.quote(f"{search_term} game")
            
            # 구글 이미지 검색 URL 수정 - 이미지 탭으로 직접 이동
            url = f"https://www.google.com/search?q={query}&tbm=isch"
            
            # 헤더 설정 (브라우저처럼 보이게)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Referer': 'https://www.google.com/'
            }
            
            # 요청 보내기 (타임아웃 설정 추가)
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 응답 텍스트에서 이미지 URL 추출 (정규식 사용)
            import re
            
            # 구글 이미지 검색 결과에서 이미지 URL을 추출하는 패턴
            # 암호화된 URL 패턴 제외하고 일반 이미지 URL만 추출
            patterns = [
                r'\"(https://[^\"]+\.(?:jpg|jpeg|png|gif))\"',
                r'src=\"(https://[^\"]+\.(?:jpg|jpeg|png|gif))\"'
            ]
            
            img_urls = []
            try:
                for pattern in patterns:
                    matches = re.findall(pattern, response.text)
                    img_urls.extend(matches)
                
                # 중복 제거 및 최대 20개로 제한
                img_urls = list(dict.fromkeys(img_urls))[:20]
            except Exception as regex_error:
                print(f"정규식 패턴 매칭 오류: {regex_error}")
            
            # 결과가 없으면 BeautifulSoup으로 다시 시도
            if not img_urls:
                try:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # 모든 img 태그 찾기
                    for img in soup.find_all('img'):
                        if 'src' in img.attrs and img['src'].startswith('http') and not 'encrypted-tbn0.gstatic.com' in img['src']:
                            img_urls.append(img['src'])
                        elif 'data-src' in img.attrs and not 'encrypted-tbn0.gstatic.com' in img['data-src']:
                            img_urls.append(img['data-src'])
                    
                    # 중복 제거 및 최대 20개로 제한
                    img_urls = list(dict.fromkeys(img_urls))[:20]
                except Exception as bs_error:
                    print(f"BeautifulSoup 파싱 오류: {bs_error}")
            
            # 디버깅 정보 출력
            print(f"검색어: {search_term}, 찾은 이미지 URL 수: {len(img_urls)}")
            
            return img_urls
        except requests.exceptions.Timeout:
            print(f"구글 이미지 검색 타임아웃: 검색어 '{search_term}'")
            return []
        except Exception as e:
            print(f"구글 이미지 검색 오류: {e}")
            return []
    
    def display_image_results(self, image_urls, loading_label):
        """이미지 검색 결과 표시"""
        # 로딩 레이블 제거
        loading_label.destroy()
        
        if not image_urls:
            ctk.CTkLabel(self.image_frame, text="이미지 검색 결과가 없습니다. 직접 이미지를 추가하거나 다른 검색어를 시도해보세요.").pack(pady=20)
            return
        
        # 이미지 그리드 프레임
        grid_frame = ctk.CTkFrame(self.image_frame, fg_color="transparent")
        grid_frame.pack(fill="both", expand=True)
        
        # 이미지 표시 (그리드 형태로)
        row, col = 0, 0
        max_cols = 4  # 한 줄에 4개 이미지
        
        # 이미지 로딩 상태 표시
        status_label = ctk.CTkLabel(self.image_frame, text="이미지 로딩 중... (0/{})".format(len(image_urls)))
        status_label.pack(pady=5)
        
        # 이미지 로딩 및 표시를 위한 함수
        def load_and_display_images():
            nonlocal row, col
            loaded_count = 0
            
            for idx, img_url in enumerate(image_urls):
                try:
                    # 이미지 다운로드 시도
                    try:
                        # 암호화된 URL 건너뛰기
                        if 'encrypted-tbn0.gstatic.com' in img_url:
                            continue
                            
                        response = requests.get(img_url, stream=True, timeout=5)
                        response.raise_for_status()
                        
                        # 이미지 데이터를 메모리에 로드
                        img_data = BytesIO(response.content)
                        pil_img = Image.open(img_data)
                        
                        # 이미지 크기 조정
                        pil_img = resize_image_maintain_aspect(pil_img, (100, 100))
                        
                        # CTkImage로 변환
                        ctk_img = ctk.CTkImage(
                            light_image=pil_img,
                            dark_image=pil_img,
                            size=(pil_img.width, pil_img.height)
                        )
                        
                        # 이미지 버튼 생성
                        img_btn = ctk.CTkButton(
                            grid_frame,
                            text="",
                            image=ctk_img,
                            width=100,
                            height=100,
                            command=lambda url=img_url: self.select_image(url)
                        )
                        img_btn.image = ctk_img  # 참조 유지
                        
                        # UI 업데이트는 메인 스레드에서 수행
                        self.dialog.after(0, lambda btn=img_btn, r=row, c=col: btn.grid(row=r, column=c, padx=5, pady=5))
                        
                        # 다음 위치 계산
                        col += 1
                        if col >= max_cols:
                            col = 0
                            row += 1
                            
                        loaded_count += 1
                        # 상태 업데이트
                        self.dialog.after(0, lambda count=loaded_count: status_label.configure(text=f"이미지 로딩 중... ({count}/{len(image_urls)})"))
                        
                    except Exception as img_error:
                        print(f"개별 이미지 로드 실패 ({img_url}): {img_error}")
                        continue
                        
                except Exception as e:
                    print(f"이미지 처리 실패 ({img_url}): {e}")
            
            # 모든 이미지 로딩 완료 후 상태 업데이트
            self.dialog.after(0, lambda: status_label.configure(text=f"이미지 로딩 완료 ({loaded_count}/{len(image_urls)})"))
            
            # 그리드 설정
            for i in range(max_cols):
                self.dialog.after(0, lambda i=i: grid_frame.grid_columnconfigure(i, weight=1))
            
            # 이미지가 하나도 표시되지 않았을 경우 메시지 표시
            if loaded_count == 0:
                self.dialog.after(0, lambda: ctk.CTkLabel(self.image_frame, text="이미지를 표시할 수 없습니다. 다른 검색어를 시도해보세요.").pack(pady=20))
        
        # 백그라운드 스레드에서 이미지 로딩 및 표시
        threading.Thread(target=load_and_display_images, daemon=True).start()
    
    def select_image(self, img_url):
        """이미지 선택 처리"""
        self.selected_image_url = img_url
        
        try:
            # 이미지 다운로드
            response = requests.get(img_url, stream=True)
            response.raise_for_status()
            
            # 이미지 데이터를 메모리에 로드
            img_data = BytesIO(response.content)
            pil_img = Image.open(img_data)
            
            # 이미지 크기 조정
            pil_img = resize_image_maintain_aspect(pil_img, (200, 200))
            
            # CTkImage로 변환
            ctk_img = ctk.CTkImage(
                light_image=pil_img,
                dark_image=pil_img,
                size=(pil_img.width, pil_img.height)
            )
            
            # 선택된 이미지 표시
            self.selected_image_label.configure(image=ctk_img, text="")
            self.selected_image_label.image = ctk_img  # 참조 유지
            
            # 이미지 저장
            self.save_selected_image(pil_img)
            
        except Exception as e:
            self.show_error(f"이미지 선택 중 오류 발생: {e}")
    
    def save_selected_image(self, pil_img):
        """선택한 이미지를 저장"""
        try:
            # thumb_imgs 폴더 생성
            thumb_dir = os.path.join(self.folder_path, "thumb_imgs")
            os.makedirs(thumb_dir, exist_ok=True)
            
            # 이미지 저장 경로
            img_path = os.path.join(thumb_dir, "00.jpg")
            
            # 이미지 저장
            pil_img.save(img_path, 'JPEG', quality=85)
            
            # 저장된 경로 기록
            self.downloaded_image_path = img_path
            
            print(f"이미지 저장 완료: {img_path}")
        except Exception as e:
            print(f"이미지 저장 실패: {e}")
    
    def show_error(self, error_msg, loading_label=None):
        """오류 메시지 표시"""
        if loading_label:
            loading_label.destroy()
        
        try:
            SafeCTkMessagebox(
                master=self.dialog,
                title="오류",
                message=f"오류가 발생했습니다: {error_msg}",
                icon="cancel"
            )
        except Exception as e:
            print(f"오류 메시지 표시 실패: {e}")
    
    def save_manual_info(self):
        """수동 입력 정보 저장"""
        # 필수 필드 확인
        title = self.manual_entries['title'].get().strip()
        if not title:
            SafeCTkMessagebox(
                master=self.dialog,
                title="오류",
                message="제목은 필수 입력 항목입니다.",
                icon="warning"
            )
            return
        
        # 게임 정보 수집
        game_code = self.manual_entries['game_code'].get().strip()
        if not game_code:
            # 게임 코드가 없으면 "MANUAL_" + 타임스탬프로 생성
            game_code = f"MANUAL_{int(time.time())}"
        
        circle = self.manual_entries['circle'].get().strip()
        work_type = self.manual_entries['work_type'].get().strip()
        genres = self.manual_entries['genres'].get().strip()
        
        # DB에 저장
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        
        # exe 파일 목록 가져오기
        exe_files = []
        for root, _, files in os.walk(self.folder_path):
            for file in files:
                if file.lower().endswith('.exe'):
                    relative_path = os.path.relpath(os.path.join(root, file), self.folder_path)
                    exe_files.append(relative_path)
        
        # 커버 이미지 경로
        cover_image = self.downloaded_image_path if self.downloaded_image_path else ""
        
        cursor.execute('''
        INSERT INTO games 
        (game_code, title, rating, circle, work_type, genres, cover_image, folder_path, exe_files)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            game_code,
            title,
            0.0,  # 평점 기본값
            circle,
            work_type,
            genres,
            cover_image,
            self.folder_path,
            json.dumps(exe_files, ensure_ascii=False)
        ))
        
        conn.commit()
        conn.close()
        
        # 성공 메시지 표시 후 콜백 호출 및 창 닫기
        try:
            msg_box = SafeCTkMessagebox(
                master=self.dialog,
                title="성공",
                message="게임 정보가 등록되었습니다.",
                icon="check"
            )
            
            # 메시지 박스가 닫힌 후에 콜백 호출 및 창 닫기
            def on_msgbox_close():
                # 등록된 폴더를 invalid_folders 리스트에서 제거
                if hasattr(self.dialog.master, 'viewer'):
                    try:
                        self.dialog.master.viewer.invalid_folders.remove(self.folder_path)
                    except ValueError:
                        pass  # 이미 제거된 경우 무시
                
                # 콜백 호출
                if self.callback:
                    self.callback()
                
                # 창 닫기
                self.dialog.destroy()
            
            # 메시지 박스의 OK 버튼에 콜백 연결
            msg_box.after(200, on_msgbox_close)
        except Exception as e:
            print(f"성공 메시지 표시 실패: {e}")
            # 메시지 박스 표시에 실패해도 invalid_folders 업데이트 및 콜백 호출
            if hasattr(self.dialog.master, 'viewer'):
                try:
                    self.dialog.master.viewer.invalid_folders.remove(self.folder_path)
                except ValueError:
                    pass  # 이미 제거된 경우 무시
            
            if self.callback:
                self.callback()
            self.dialog.destroy()

class GameViewerGUI:
    def __init__(self):
        self.window = ctk.CTk()
        self.window.title("DL Game Viewer")
        self.window.geometry("800x600")
        
        # 로딩 표시를 위한 프레임
        self.loading_frame = ctk.CTkFrame(self.window)
        self.loading_label = ctk.CTkLabel(self.loading_frame, text="로딩 중...", font=("Arial", 20))
        self.loading_label.pack(pady=200)
        
        # 이미지 로딩 스레드 및 큐 추가
        self.image_queue = queue.Queue()
        self.image_threads = []
        
        # 데이터 관련 초기화
        self.all_games = []
        self.game_frames = []
        self.selected_frame = None
        self.search_timer = None
        
        # 페이징 관련 변수
        self.items_per_page = 20
        self.current_page = 1
        self.total_pages = 1
        
        self.viewer = DLGameViewer()  # DLGameViewer 인스턴스를 클래스 멤버로 저장
        # 초기 자동 스캔 제거 (self.viewer.process_folders() 호출 제거)
        
        # 화면 구성은 별도 스레드에서 완료 후 호출
        threading.Thread(target=self.delayed_init, daemon=True).start()
    
    def delayed_init(self):
        """UI 초기화 및 데이터 로딩을 백그라운드에서 처리"""
        # 데이터베이스에서 게임 정보 로드
        self.preload_data()
        
        # UI 구성 완료 후 메인 스레드에서 UI 표시
        self.window.after(10, self.show_main_ui)
    
    def preload_data(self):
        """초기 데이터 로드"""
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        
        # 여기서는 기본 정보만 가져오고 이미지는 필요할 때 로드
        cursor.execute("SELECT id, game_code, title, rating, circle, work_type, genres, cover_image, folder_path, exe_files FROM games")
        self.all_games = cursor.fetchall()
        conn.close()
        
        # 정렬 적용 - 기본값은 "내림차순"
        self.all_games = sorted(self.all_games, key=lambda x: x[0], reverse=True)

    def show_main_ui(self):
        """메인 UI 표시"""
        # 로딩 화면 제거
        self.loading_frame.destroy()
        
        # 메뉴 프레임 추가
        self.menu_frame = ctk.CTkFrame(self.window)
        self.menu_frame.pack(fill="x", padx=10, pady=(10,0))
        
        # 스캔 버튼 추가 - 코드 없는 게임 처리 버튼 왼쪽에 배치
        self.scan_btn = ctk.CTkButton(
            self.menu_frame,
            text="스캔",
            command=self.scan_games
        )
        self.scan_btn.pack(side="left", padx=5)
        
        # 게임 코드 없는 폴더 처리 버튼 추가
        self.process_invalid_btn = ctk.CTkButton(
            self.menu_frame,
            text="코드 없는 게임 처리",
            command=self.process_invalid_folders
        )
        self.process_invalid_btn.pack(side="left", padx=5)
        
        # 중복 체크 버튼 추가
        self.check_duplicates_btn = ctk.CTkButton(
            self.menu_frame,
            text="중복체크",
            command=self.check_duplicate_codes
        )
        self.check_duplicates_btn.pack(side="left", padx=5)
        
        # 폴더 관리 버튼 추가
        self.folder_manage_btn = ctk.CTkButton(
            self.menu_frame,
            text="폴더관리",
            command=self.open_folder_management
        )
        self.folder_manage_btn.pack(side="left", padx=5)
        
        # 검색 프레임 추가
        self.search_frame = ctk.CTkFrame(self.window)
        self.search_frame.pack(fill="x", padx=10, pady=(10,0))
        
        # 검색 필드 생성 - 장르 필드 제거
        self.search_entries = {}
        search_fields = [
            ("game_code", "게임 코드"),
            ("title", "제목"),
            ("circle", "서클"),
            ("work_type", "작품 형식/장르")  # 작품 형식 레이블 변경
        ]
        
        # 검색 필드 배치
        for i, (field, placeholder) in enumerate(search_fields):
            field_frame = ctk.CTkFrame(self.search_frame)
            field_frame.grid(row=0, column=i, padx=5, pady=5, sticky="ew")
            
            ctk.CTkLabel(field_frame, text=placeholder).pack(anchor="w", padx=5)
            
            entry = ctk.CTkEntry(field_frame, placeholder_text=f"{placeholder} 검색...")
            entry.pack(fill="x", padx=5, pady=(0,5))
            entry.bind('<Return>', self.perform_search)  # 엔터 키 바인딩 추가
            # 검색 텍스트 박스에 포커스 시 하이라이트 해제
            entry.bind('<FocusIn>', self.clear_selection)
            self.search_entries[field] = entry
        
        # 정렬 옵션 프레임 추가
        sort_frame = ctk.CTkFrame(self.search_frame)
        sort_frame.grid(row=0, column=5, padx=5, pady=5, sticky="ew")
        
        ctk.CTkLabel(sort_frame, text="정렬").pack(anchor="w", padx=5)
        
        self.sort_var = ctk.StringVar(value="내림차순")
        self.sort_combobox = ctk.CTkComboBox(
            sort_frame,
            values=["내림차순", "오름차순", "랜덤"],
            variable=self.sort_var,
            command=self.apply_sort
        )
        self.sort_combobox.pack(fill="x", padx=5, pady=(0,5))
        
        # 그리드 설정
        for i in range(6):
            self.search_frame.grid_columnconfigure(i, weight=1)
        
        # 스크롤 가능한 프레임 생성
        self.scroll_frame = ctk.CTkScrollableFrame(self.window)
        self.scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 그리드를 위한 컨테이너 프레임
        self.grid_frame = ctk.CTkFrame(self.scroll_frame, fg_color="transparent")
        self.grid_frame.pack(fill="both", expand=True)
        
        # 페이징 컨트롤 프레임 추가
        self.page_control_frame = ctk.CTkFrame(self.window)
        self.page_control_frame.pack(fill="x", padx=10, pady=5)
        
        self.prev_btn = ctk.CTkButton(
            self.page_control_frame,
            text="이전",
            width=60,
            command=self.prev_page
        )
        self.prev_btn.pack(side="left", padx=5)
        
        self.page_label = ctk.CTkLabel(
            self.page_control_frame,
            text="1 / 1"
        )
        self.page_label.pack(side="left", padx=5)
        
        self.next_btn = ctk.CTkButton(
            self.page_control_frame,
            text="다음",
            width=60,
            command=self.next_page
        )
        self.next_btn.pack(side="left", padx=5)
        
        # 창 크기 변경 이벤트 바인딩
        self.window.bind('<Configure>', self.on_window_configure)
        
        # 키보드 이벤트 바인딩
        self.window.bind('<Key>', self.on_key_press)
        
        # 첫 페이지 표시
        self.refresh_display()
        
        # 이미지 로딩 스레드 시작
        for _ in range(3):  # 3개의 스레드 사용
            t = threading.Thread(target=self.image_loader_thread, daemon=True)
            t.start()
            self.image_threads.append(t)

    def scan_games(self):
        """게임 폴더 스캔 및 정보 수집"""
        # 스캔 버튼 비활성화 및 텍스트 변경
        self.scan_btn.configure(state="disabled", text="스캔 중...")
        self.window.update()
        
        # 백그라운드 스레드에서 스캔 작업 수행
        threading.Thread(target=self.perform_scan, daemon=True).start()
    
    def perform_scan(self):
        """백그라운드에서 게임 스캔 수행"""
        try:
            # 게임 폴더 처리
            self.viewer.process_folders()
            
            # UI 업데이트는 메인 스레드에서 수행
            self.window.after(0, self.scan_complete)
            
        except Exception as e:
            # 오류 발생 시 UI에 메시지 표시
            self.window.after(0, lambda e=e: self.scan_error(e))
    
    def scan_complete(self):
        """스캔 완료 후 처리"""
        # 데이터 다시 로드
        self.preload_data()
        
        # 화면 갱신
        self.refresh_display()
        
        # 스캔 버튼 다시 활성화
        self.scan_btn.configure(state="normal", text="스캔")
        
        # 결과 메시지 표시
        invalid_count = len(self.viewer.invalid_folders)
        no_exe_count = len(self.viewer.no_exe_folders)
        skipped_count = len(self.viewer.skipped_folders)
        
        message = f"스캔 완료!\n\n"
        if skipped_count > 0:
            message += f"기존 등록 폴더: {skipped_count}개\n"
        if invalid_count > 0:
            message += f"게임 코드 없는 폴더: {invalid_count}개\n"
        if no_exe_count > 0:
            message += f"실행 파일 없는 폴더: {no_exe_count}개\n"
        
        SafeCTkMessagebox(
            title="스캔 결과",
            message=message,
            icon="info"
        )
    
    def scan_error(self, error):
        """스캔 오류 처리"""
        # 스캔 버튼 다시 활성화
        self.scan_btn.configure(state="normal", text="스캔")
        
        # 오류 메시지 표시
        SafeCTkMessagebox(
            title="스캔 오류",
            message=f"게임 스캔 중 오류가 발생했습니다:\n{str(error)}",
            icon="cancel"
        )
        
    def image_loader_thread(self):
        """이미지를 백그라운드에서 로드하는 스레드"""
        while True:
            try:
                task = self.image_queue.get(timeout=60)  # 60초 타임아웃
                if task is None:  # 종료 신호
                    break
                    
                img_label, img_path, size = task
                
                if not os.path.exists(img_path):
                    self.image_queue.task_done()
                    continue
                    
                try:
                    # 이미지 로드 및 처리
                    pil_img = Image.open(img_path)
                    pil_img = resize_image_maintain_aspect(pil_img, size)
                    
                    # UI 업데이트는 메인 스레드에서 수행
                    ctk_img = ctk.CTkImage(
                        light_image=pil_img, 
                        dark_image=pil_img, 
                        size=(pil_img.width, pil_img.height)
                    )
                    
                    # UI 업데이트를 메인 스레드에서 실행
                    self.window.after(0, lambda i=img_label, img=ctk_img: self.update_image_label(i, img))
                except Exception as e:
                    print(f"이미지 로드 실패 ({img_path}): {e}")
                    
                self.image_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"이미지 로딩 스레드 오류: {e}")
                time.sleep(1)  # 오류 발생 시 잠시 대기
    
    def update_image_label(self, img_label, ctk_img):
        """이미지 레이블 업데이트 (메인 스레드에서 호출)"""
        try:
            img_label.configure(image=ctk_img, text="")  # 텍스트를 비워서 "로딩 중..." 텍스트 제거
            img_label.image = ctk_img  # 참조 유지
        except Exception as e:
            # 위젯이 이미 삭제된 경우 등 예외 처리
            print(f"이미지 업데이트 실패: {e}")

    def apply_sort(self, choice=None):
        """정렬 적용"""
        if not choice:
            choice = self.sort_var.get()
        
        # 기존 프레임 제거
        for frame in self.game_frames:
            frame.destroy()
        self.game_frames.clear()
        
        # 정렬 적용
        if choice == "랜덤":
            import random
            self.all_games = list(self.all_games)
            random.shuffle(self.all_games)
        elif choice == "오름차순":
            self.all_games = sorted(self.all_games, key=lambda x: x[0])
        elif choice == "내림차순":
            self.all_games = sorted(self.all_games, key=lambda x: x[0], reverse=True)
        
        # 페이지 초기화 및 화면 새로고침
        self.current_page = 1
        self.refresh_display()

    def schedule_search(self, event):
        """검색 타이머를 설정하는 함수"""
        # 이전 타이머가 있다면 취소
        if self.search_timer is not None:
            self.window.after_cancel(self.search_timer)
        
        # 0.5초 후에 검색 실행
        self.search_timer = self.window.after(500, self.perform_search)

    def perform_search(self, event=None):
        """실제 검색을 수행하는 함수"""
        # 각 필드의 검색어 가져오기
        search_terms = {
            'game_code': self.search_entries['game_code'].get().strip().lower(),
            'title': self.search_entries['title'].get().strip().lower(),
            'circle': self.search_entries['circle'].get().strip().lower(),
            'work_type': self.search_entries['work_type'].get().strip().lower()
        }
        
        # 백그라운드 스레드에서 검색 수행
        threading.Thread(target=self.search_in_background, args=(search_terms,), daemon=True).start()

    def search_in_background(self, search_terms):
        """백그라운드에서 검색을 수행하고 결과를 UI에 반영"""
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        
        # 검색 조건 구성
        conditions = []
        params = []
        
        if search_terms['game_code']:
            conditions.append("LOWER(game_code) LIKE ?")
            params.append(f"%{search_terms['game_code']}%")
        
        if search_terms['title']:
            # 제목과 폴더 경로에서 검색
            conditions.append("(LOWER(title) LIKE ? OR LOWER(folder_path) LIKE ?)")
            params.extend([f"%{search_terms['title']}%", f"%{search_terms['title']}%"])
        
        if search_terms['circle']:
            conditions.append("LOWER(circle) LIKE ?")
            params.append(f"%{search_terms['circle']}%")
        
        if search_terms['work_type']:
            # 작품 형식과 장르 모두에서 검색
            conditions.append("(LOWER(work_type) LIKE ? OR LOWER(genres) LIKE ?)")
            params.extend([f"%{search_terms['work_type']}%", f"%{search_terms['work_type']}%"])
        
        # SQL 쿼리 구성
        query = "SELECT * FROM games"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # 쿼리 실행
        cursor.execute(query, params)
        self.all_games = cursor.fetchall()
        conn.close()
        
        # UI 업데이트는 메인 스레드에서 수행
        # 정렬 기준 적용 후 화면 갱신
        self.window.after(0, lambda: self.apply_sort(self.sort_var.get()))

    def on_window_configure(self, event):
        # 창 크기가 변경될 때마다 그리드 재배치
        if hasattr(self, 'game_frames'):
            window_width = self.scroll_frame.winfo_width()
            frame_width = 340  # 이미지 너비(300) + 패딩(40)
            columns = max(1, window_width // frame_width)
            
            # 모든 프레임 숨기기
            for frame in self.game_frames:
                frame.grid_forget()
            
            # 프레임 재배치
            for i, frame in enumerate(self.game_frames):
                row = i // columns
                col = i % columns
                frame.grid(row=row, column=col, padx=5, pady=5, sticky="nsew")
                
            # 컬럼 가중치 설정
            for i in range(columns):
                self.grid_frame.grid_columnconfigure(i, weight=1)

    def run_game(self, folder_path, exe_files, game_id):
        """게임 실행"""
        # DB에서 최신 exe_files 정보를 가져옴 - game_id로 검색
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        cursor.execute("SELECT exe_files FROM games WHERE id = ?", (game_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            exe_files = result[0]  # DB의 최신 정보 사용
        
        exe_list = json.loads(exe_files)
        
        if not exe_list:
            return
        
        if len(exe_list) == 1:
            # 실행 파일이 하나만 있는 경우
            full_path = os.path.join(folder_path, exe_list[0])
            os.startfile(full_path)
        else:
            # 여러 개의 실행 파일이 있는 경우
            dialog = ExeSelectionDialog(self.window, exe_list, game_id)
            self.window.wait_window(dialog.dialog)
            
            if dialog.selected_exe:
                full_path = os.path.join(folder_path, dialog.selected_exe)
                os.startfile(full_path)

    def highlight_frame(self, frame, game):
        # 이전에 선택된 프레임이 있다면 원래 상태로 복원
        if self.selected_frame:
            self.selected_frame.configure(border_width=0)
        
        # 새로 선택된 프레임 하이라이트
        frame.configure(border_width=2, border_color="blue")
        self.selected_frame = frame
        
        # 모든 검색 입력창의 포커스 해제
        self.window.focus_set()  # 윈도우로 포커스 이동하여 입력창 포커스 해제

    def create_game_frame(self, game):
        # 고정 크기의 프레임 생성
        frame = ctk.CTkFrame(self.grid_frame, width=300, height=300)  # 높이를 줄임
        self.game_frames.append(frame)
        
        # 프레임 크기 고정 (내부 위젯이 프레임 크기를 변경하지 않도록)
        frame.pack_propagate(False)
        frame.grid_propagate(False)
        
        # 프레임에 게임 데이터 저장
        frame.game_data = game
        
        # 클릭 이벤트 추가
        frame.bind('<Button-1>', 
                  lambda e, f=frame, g=game: self.highlight_frame(f, g))
        
        # 더블 클릭 이벤트를 위한 바인딩 수정 - game_code(g[1]) 대신 game_id(g[0]) 사용
        frame.bind('<Double-Button-1>', 
                  lambda e, f=frame, g=game: self.run_game(g[8], g[9], g[0]))
        
        # 이미지 컨테이너 프레임 생성 (고정 크기)
        img_container = ctk.CTkFrame(frame, width=CARD_IMAGE_SIZE[0], height=CARD_IMAGE_SIZE[1])
        img_container.pack(padx=5, pady=5)
        # 프레임 크기 고정
        img_container.pack_propagate(False)
        
        # 초기 이미지 레이블 (나중에 이미지로 업데이트)
        img_label = ctk.CTkLabel(img_container, text="로딩 중...", image=None)
        img_label.pack(expand=True)
        
        # 이미지 컨테이너에 이벤트 바인딩 수정
        img_container.bind('<Button-1>', 
                          lambda e, f=frame, g=game: self.highlight_frame(f, g))
        img_container.bind('<Double-Button-1>', 
                          lambda e, f=frame, g=game: self.run_game(g[8], g[9], g[0]))
        
        # 이미지 레이블에도 이벤트 바인딩 수정
        img_label.bind('<Button-1>', 
                       lambda e, f=frame, g=game: self.highlight_frame(f, g))
        img_label.bind('<Double-Button-1>', 
                       lambda e, f=frame, g=game: self.run_game(g[8], g[9], g[0]))
        
        # 이미지 로딩을 별도 스레드에 요청
        if os.path.exists(game[7]):  # cover_image 경로 확인
            self.image_queue.put((img_label, game[7], CARD_IMAGE_SIZE))
        else:
            # 이미지가 없는 경우 텍스트만 표시
            img_label.configure(text="이미지 없음")
        
        # 제목만 표시하는 레이블 추가
        title_label = ctk.CTkLabel(
            frame,
            text=game[2],  # 게임의 제목 (index 2)
            font=("Arial", 12),
            wraplength=280,  # 텍스트 줄바꿈 너비 설정
            justify="center"
        )
        title_label.pack(fill="x", padx=10, pady=(0, 5))
        
        # 제목 레이블에도 이벤트 바인딩 수정
        title_label.bind('<Double-Button-1>', 
                       lambda e, f=frame, g=game: self.run_game(g[8], g[9], g[0]))

    def refresh_display(self):
        """화면 새로고침"""
        # 선택 상태 초기화
        self.selected_frame = None
        
        # 기존 프레임 제거
        for frame in self.game_frames:
            frame.destroy()
        self.game_frames.clear()
        
        # 현재 페이지에 해당하는 게임만 표시
        start_idx = (self.current_page - 1) * self.items_per_page
        end_idx = start_idx + self.items_per_page
        current_page_games = self.all_games[start_idx:end_idx]
        
        # 게임 정보 다시 표시
        for game in current_page_games:
            self.create_game_frame(game)
        
        # 페이지 컨트롤 업데이트
        self.update_page_controls(len(self.all_games))
        
        # 그리드 재배치
        self.window.update_idletasks()
        self.on_window_configure(None)
        
        # 스크롤을 최상단으로 이동
        self.scroll_frame._parent_canvas.yview_moveto(0)

    def on_key_press(self, event):
        """키보드 이벤트 처리"""
        if not self.selected_frame:
            return
        
        if event.char == 'i':
            # 선택된 프레임에서 직접 게임 데이터 가져오기
            if hasattr(self.selected_frame, 'game_data'):
                GameInfoDialog(self.window, self.selected_frame.game_data, self.refresh_display)
        elif event.char == 'f':
            # 선택된 게임의 폴더 열기
            if hasattr(self.selected_frame, 'game_data'):
                folder_path = self.selected_frame.game_data[8]  # folder_path는 인덱스 8
                try:
                    os.startfile(folder_path)
                except Exception as e:
                    SafeCTkMessagebox(
                        title="오류",
                        message=f"폴더를 열 수 없습니다:\n{str(e)}",
                        icon="cancel"
                    )
        elif event.keysym == 'Delete':  # Delete 키 처리
            if hasattr(self.selected_frame, 'game_data'):
                self.delete_game(self.selected_frame.game_data)

    def delete_game(self, game_data):
        """게임 삭제"""
        game_id = game_data[0]  # ID는 인덱스 0
        folder_path = game_data[8]  # folder_path는 인덱스 8
        game_title = game_data[2]  # title은 인덱스 2
        
        # 삭제 확인 메시지
        msg = CTkMessagebox(  # SafeCTkMessagebox 대신 CTkMessagebox 사용
            master=self.window,
            title="삭제 확인",
            message=f"다음 게임을 삭제하시겠습니까?\n\n제목: {game_title}\n폴더: {folder_path}",
            icon="warning",
            option_1="취소",
            option_2="삭제"
        )
        
        result = msg.get()  # 결과 가져오기
        if result != "삭제":  # 결과 확인
            return
        
        try:
            # 폴더를 휴지통으로 이동
            if os.path.exists(folder_path):
                send2trash(folder_path)
            
            # DB에서 데이터 삭제
            conn = sqlite3.connect("games.db")
            cursor = conn.cursor()
            cursor.execute("DELETE FROM games WHERE id = ?", (game_id,))
            conn.commit()
            conn.close()
            
            # 현재 표시된 게임 목록에서도 제거
            self.all_games = [game for game in self.all_games if game[0] != game_id]
            
            # 화면 갱신
            self.refresh_display()
            
            # 성공 메시지
            CTkMessagebox(  # SafeCTkMessagebox 대신 CTkMessagebox 사용
                master=self.window,
                title="삭제 완료",
                message="게임이 성공적으로 삭제되었습니다.",
                icon="check"
            )
            
        except Exception as e:
            # 오류 메시지
            CTkMessagebox(  # SafeCTkMessagebox 대신 CTkMessagebox 사용
                master=self.window,
                title="삭제 오류",
                message=f"게임 삭제 중 오류가 발생했습니다:\n{str(e)}",
                icon="cancel"
            )

    def update_page_controls(self, total_items):
        """페이지 컨트롤 업데이트"""
        self.total_pages = max(1, (total_items + self.items_per_page - 1) // self.items_per_page)
        self.current_page = min(self.current_page, self.total_pages)
        
        # 페이지 레이블 업데이트
        self.page_label.configure(text=f"{self.current_page} / {self.total_pages}")
        
        # 버튼 상태 업데이트
        self.prev_btn.configure(state="normal" if self.current_page > 1 else "disabled")
        self.next_btn.configure(state="normal" if self.current_page < self.total_pages else "disabled")
    
    def prev_page(self):
        """이전 페이지로 이동"""
        if self.current_page > 1:
            self.current_page -= 1
            self.refresh_display()
    
    def next_page(self):
        """다음 페이지로 이동"""
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.refresh_display()

    def process_invalid_folders(self):
        """게임 코드가 없거나 정보 수집에 실패한 폴더 처리"""
        # DB에 이미 등록된 폴더 제외
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        
        # 현재 invalid_folders 리스트에서 이미 DB에 등록된 폴더 제외
        self.viewer.invalid_folders = [
            folder_path for folder_path in self.viewer.invalid_folders
            if not cursor.execute("SELECT 1 FROM games WHERE folder_path = ?", (folder_path,)).fetchone()
        ]
        
        conn.close()
        
        # 남은 invalid_folders로 리스트 생성
        invalid_folders = []
        for folder_path in self.viewer.invalid_folders:
            folder_name = os.path.basename(folder_path)
            invalid_folders.append((folder_name, folder_path))
        
        if not invalid_folders:
            SafeCTkMessagebox(
                title="알림",
                message="처리할 게임 코드 없는 폴더나 정보 수집 실패한 폴더가 없습니다.",
                icon="info"
            )
            return
        
        # 폴더 선택 다이얼로그
        self.show_folder_selection_dialog(invalid_folders)
    
    def show_folder_selection_dialog(self, folders):
        """폴더 선택 다이얼로그 표시"""
        dialog = ctk.CTkToplevel(self.window)
        dialog.title("게임 코드 없는 폴더 선택")
        dialog.geometry("500x400")
        
        # 모달 창으로 설정
        dialog.transient(self.window)
        dialog.grab_set()
        
        # 안내 레이블
        ctk.CTkLabel(dialog, text="처리할 폴더를 선택하세요:").pack(pady=10)
        
        # 스크롤 가능한 프레임
        scroll_frame = ctk.CTkScrollableFrame(dialog)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 폴더 목록 버튼
        for folder_name, folder_path in folders:
            btn = ctk.CTkButton(
                scroll_frame, 
                text=folder_name,
                command=lambda p=folder_path, n=folder_name: self.open_search_dialog(n, p, dialog)
            )
            btn.pack(pady=5, padx=20, fill="x")
    
    def open_search_dialog(self, folder_name, folder_path, parent_dialog=None):
        """검색 다이얼로그 열기"""
        # 부모 다이얼로그가 있으면 닫기
        if parent_dialog:
            parent_dialog.destroy()
        
        # 검색 다이얼로그 열기
        search_dialog = GameInfoSearchDialog(
            self.window, 
            folder_name, 
            folder_path,
            self.refresh_display  # 콜백으로 화면 갱신 함수 전달
        )

    def check_duplicate_codes(self):
        """게임 코드 중복 검사"""
        conn = sqlite3.connect("games.db")
        cursor = conn.cursor()
        
        # 중복된 game_code를 찾는 쿼리
        cursor.execute("""
            SELECT game_code, COUNT(*) as count 
            FROM games 
            GROUP BY game_code 
            HAVING count > 1
        """)
        duplicates = cursor.fetchall()
        
        if not duplicates:
            SafeCTkMessagebox(
                title="중복 검사",
                message="중복된 게임 코드가 없습니다.",
                icon="info"
            )
            conn.close()
            return
        
        # 중복된 게임 코드들의 상세 정보 가져오기
        duplicate_games = []
        for game_code, _ in duplicates:
            cursor.execute("""
                SELECT id, game_code, title, rating, circle, work_type, genres, cover_image, folder_path, exe_files 
                FROM games 
                WHERE game_code = ?
            """, (game_code,))
            duplicate_games.extend(cursor.fetchall())
        
        conn.close()
        
        # 검색 결과를 화면에 표시
        self.all_games = duplicate_games
        self.current_page = 1
        self.refresh_display()
        
        # 중복 개수 메시지 표시
        total_duplicates = len(duplicates)
        total_entries = len(duplicate_games)
        SafeCTkMessagebox(
            title="중복 검사 결과",
            message=f"중복된 게임 코드: {total_duplicates}개\n총 항목 수: {total_entries}개",
            icon="info"
        )

    def run(self):
        self.window.mainloop()

    def open_folder_management(self):
        """폴더 관리 다이얼로그 열기"""
        FolderManagementDialog(self.window, self.viewer, self.refresh_folder_settings)
    
    def refresh_folder_settings(self):
        """폴더 설정 변경 후 갱신"""
        # DB에서 데이터 다시 로드
        self.preload_data()
        
        # UI 갱신
        self.refresh_display()

    def clear_selection(self, event=None):
        """검색 텍스트 박스 선택 시 하이라이트된 프레임 해제"""
        if self.selected_frame:
            self.selected_frame.configure(border_width=0)
            self.selected_frame = None

class FolderManagementDialog:
    def __init__(self, parent, viewer, refresh_callback=None):
        self.dialog = ctk.CTkToplevel(parent)
        self.dialog.title("폴더 관리")
        self.dialog.geometry("700x500")
        
        # 모달 창으로 설정
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        self.viewer = viewer
        self.refresh_callback = refresh_callback
        
        # 메인 프레임
        self.main_frame = ctk.CTkFrame(self.dialog)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 안내 텍스트
        ctk.CTkLabel(
            self.main_frame, 
            text="게임 폴더 목록", 
            font=("Arial", 16, "bold")
        ).pack(pady=(0, 10))
        
        # 폴더 목록 프레임
        self.folder_list_frame = ctk.CTkScrollableFrame(self.main_frame)
        self.folder_list_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 폴더 목록 표시
        self.refresh_folder_list()
        
        # 버튼 프레임
        button_frame = ctk.CTkFrame(self.main_frame)
        button_frame.pack(fill="x", pady=10)
        
        # 폴더 추가 버튼
        add_btn = ctk.CTkButton(
            button_frame, 
            text="폴더 추가", 
            command=self.add_folder
        )
        add_btn.pack(side="left", padx=5)
        
        # 저장 버튼
        save_btn = ctk.CTkButton(
            button_frame, 
            text="저장", 
            command=self.save_settings
        )
        save_btn.pack(side="right", padx=5)
    
    def refresh_folder_list(self):
        """폴더 목록 새로고침"""
        # 기존 폴더 목록 위젯 제거
        for widget in self.folder_list_frame.winfo_children():
            widget.destroy()
        
        # 폴더 목록이 비어있는 경우
        if not self.viewer.game_paths:
            ctk.CTkLabel(
                self.folder_list_frame,
                text="등록된 폴더가 없습니다. 폴더 추가 버튼을 눌러 게임 폴더를 추가하세요.",
                wraplength=600
            ).pack(pady=20)
            return
        
        # 각 폴더에 대한 행 추가
        for i, folder_path in enumerate(self.viewer.game_paths):
            row_frame = ctk.CTkFrame(self.folder_list_frame)
            row_frame.pack(fill="x", pady=5)
            
            # 폴더 경로 레이블
            path_label = ctk.CTkLabel(
                row_frame,
                text=folder_path,
                anchor="w",
                wraplength=550
            )
            path_label.pack(side="left", fill="x", expand=True, padx=5)
            
            # 삭제 버튼
            delete_btn = ctk.CTkButton(
                row_frame,
                text="X",
                width=30,
                command=lambda idx=i: self.remove_folder(idx)
            )
            delete_btn.pack(side="right", padx=5)
    
    def add_folder(self):
        """게임 폴더 추가"""
        from tkinter import filedialog
        
        # 폴더 선택 다이얼로그
        folder_path = filedialog.askdirectory(
            title="게임 폴더 선택",
            initialdir=os.path.expanduser("~")
        )
        
        if folder_path:
            # 경로 슬래시를 윈도우 형식으로 정규화
            folder_path = os.path.normpath(folder_path)
            
            # 중복 확인 - 정규화된 경로로 비교
            normalized_paths = [os.path.normpath(path) for path in self.viewer.game_paths]
            if folder_path in normalized_paths:
                SafeCTkMessagebox(
                    master=self.dialog,
                    title="중복",
                    message="이미 등록된 폴더입니다.",
                    icon="warning"
                )
                return
            
            # 경로 추가
            self.viewer.game_paths.append(folder_path)
            
            # 목록 갱신
            self.refresh_folder_list()
    
    def remove_folder(self, index):
        """폴더 제거"""
        if 0 <= index < len(self.viewer.game_paths):
            # 확인 메시지
            folder_path = self.viewer.game_paths[index]
            msg = CTkMessagebox(
                master=self.dialog,
                title="폴더 제거",
                message=f"다음 폴더를 목록에서 제거하시겠습니까?\n\n{folder_path}",
                icon="question",
                option_1="취소",
                option_2="제거"
            )
            
            result = msg.get()
            if result == "제거":
                # 목록에서 제거
                del self.viewer.game_paths[index]
                
                # 목록 갱신
                self.refresh_folder_list()
    
    def save_settings(self):
        """설정 저장"""
        # 설정 저장
        self.viewer.save_game_paths()
        
        # 메시지 표시
        SafeCTkMessagebox(
            master=self.dialog,
            title="저장 완료",
            message="폴더 설정이 저장되었습니다.",
            icon="check"
        )
        
        # 콜백 함수 호출
        if self.refresh_callback:
            self.refresh_callback()
        
        # 다이얼로그 닫기
        self.dialog.destroy()

if __name__ == "__main__":
    # GUI 실행 (DLGameViewer 인스턴스 생성은 GUI 클래스 내부로 이동)
    gui = GameViewerGUI()
    gui.run()