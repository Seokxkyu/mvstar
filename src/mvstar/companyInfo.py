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

def read_companies():
    file_path = f'data/company/companyList.json'

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data

def save_companyInfo(sleep_time=1):
    companies = read_companies()
    companyCd = []

    for cp in companies:
        # companyCd가 8자리인 경우에만 list에 추가
        if len(cp['companyCd']) == 8:
            companyCd.append(cp['companyCd'])

    cnt = len(companyCd)

    for c in range(cnt//2000 + 1):
        file_path = f'data/company/companyInfo/companyInfo_{c+1}.json'

        if os.path.exists(file_path):
            print(f"데이터가 이미 존재합니다: {file_path}")
            continue

        url_base = f"https://kobis.or.kr/kobisopenapi/webservice/rest/company/searchCompanyInfo.json?key={API_KEY}&companyCd="
        all_data = []

        for i in tqdm(range(c*2000, (c+1)*2000)):
            if i == cnt:
                break
            
            time.sleep(sleep_time)
            r = req(url_base + companyCd[i])
            d = r['companyInfoResult']['companyInfo']
            all_data.append(d)

        save_json(all_data, file_path)

    return True
