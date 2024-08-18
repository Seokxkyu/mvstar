import requests
import os
import time
import json
from tqdm import tqdm

API_KEY = os.getenv('MOVIE_API_KEY')

def save_json(data, file_path):
    """
    JSON 데이터를 파일에 저장하는 함수

    Args:
        data: 저장할 JSON 데이터
        file_path: 저장할 파일 경로
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def req(url):
    r = requests.get(url)
    j = r.json()
    return j

def get_data2(year, per_page=10, sleep_time=1):
    url = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}"
    url += f"&openStartDt={year}&openEndDt={year}"

    j = req(url + f"&curPage=1")
    tot_cnt = j['movieListResult']['totCnt']
    total_pages = (tot_cnt // per_page + 1)
    for page in range(2, total_pages + 1):
        json_data = req(url + f"&curPage={page}")
        time.sleep(sleep_time)

def get_data(year, per_page=10, sleep_time=1):
    """
    API를 호출하여 영화 데이터를 가져오고 JSON 파일로 저장하는 함수

    Args:
        year: 검색할 연도
        per_page: 한 페이지에 가져올 영화 수
        sleep_time: API 호출 간의 대기 시간
    """
    # 파일 저장 경로 설정
    file_path = f"data/movies/year={year}/data.json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # 폴더 생성 (존재하면 무시)

    # 파일이 이미 존재하면 함수 종료
    if os.path.exists(file_path):
        print(f"데이터가 이미 존재합니다: {file_path}")
        return False

    # API 호출 및 데이터 수집
    url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}"
    total_pages = (req(url_base + f"&curPage=1")['movieListResult']['totCnt'] // per_page) + 1
    all_data = []

    for page in tqdm(range(2, total_pages + 1)):
        url = url_base + f"&curPage={page}"
        all_data.extend(req(url)['movieListResult']['movieList'])
        time.sleep(sleep_time)

    # 데이터 저장
    save_json(all_data, file_path)
    return True
