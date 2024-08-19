import requests
import os
import json
import time
from tqdm import tqdm

API_KEY = os.getenv('MOVIE_API_KEY')

def save_json(data, file_path):
    # 파일저장 경로
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def req(url):
    r = requests.get(url).json()
    return r

def save_movies(year=2015, per_page=10, sleep_time=1):
    file_path = f'data/movies/year={year}/movieList.json'
    
    # 위 경로가 있으면 API 호출을 멈추고 프로그램 종료
    if os.path.exists(file_path):
        print(f"데이터가 이미 존재합니다: {file_path}")
        return True

    # totCnt 가져와서 total_pages 계산
    url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieList.json?key={API_KEY}&openStartDt={year}&openEndDt={year}"
    r = req(url_base + "&curPage=1")
    tot_cnt = r['movieListResult']['totCnt']
    total_pages = (tot_cnt // per_page) + 1

    all_data = []
    # total_pages 만큼 Loop 돌면서 API 호출
    
    for page in range(1, 11):   
        time.sleep(sleep_time)  
        r = req(url_base + f"&curPage={page}&itemPerPage={per_page}")  
        d = r.get('movieListResult', {}).get('movieList', [])  
        all_data.extend(d)  

    save_json(all_data, file_path)
    return True
