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

def read_movies(year):
    file_path = f'data/movies/year={year}/movieList.json'

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data

def save_movieInfo(year=2015, sleep_time=1):
    file_path = f'data/movies/year={year}/movieInfo.json'

    movies = read_movies(year)
    movieCd = []

    for mv in movies:
        movieCd.append(mv['movieCd'])

    # 위 경로가 있으면 API 호출을 멈추고 프로그램 종료
    if os.path.exists(file_path):
        print(f"데이터가 이미 존재합니다: {file_path}")
        return True

    url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json?key={API_KEY}&movieCd="

    all_data = []
    for i in tqdm(movieCd):
        time.sleep(sleep_time)
        r = req(url_base + i)
        d = r['movieInfoResult']['movieInfo']
        all_data.append(d)
    
    save_json(all_data, file_path)
    return True
