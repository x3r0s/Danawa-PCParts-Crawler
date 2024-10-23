import json
import os
import requests
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import threading
import zipfile
from datetime import datetime
import argparse
import shutil
import traceback

# 프로젝트 루트 디렉토리 설정
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# 웹드라이버 설정
def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

# 스크롤 함수
def scroll_to_bottom(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# 제품 정보 추출 함수
def extract_product_info(product, category, save_images):
    name = product.find_element(By.CSS_SELECTOR, "div.main_info > div.head_info > a > strong").text
    
    prod_link = product.find_element(By.CSS_SELECTOR, "div.main_info > div.head_info > a").get_attribute('href')
    parsed_url = urlparse(prod_link)
    query_params = parse_qs(parsed_url.query)
    product_id = query_params.get('billingInternalProductSeq', [None])[0]
    
    price_element = product.find_element(By.CSS_SELECTOR, "div.price_info > div.main_price.prod_price_set > dl:nth-child(1) > dd > span.text__number")
    price_text = price_element.text.replace(',', '')
    price = int(price_text) if price_text.isdigit() else None
    
    specs = product.find_elements(By.CSS_SELECTOR, "div.main_info > dl > dd > ul.spec_list > li")
    spec_list = [spec.text for spec in specs]
    
    img_url = product.find_element(By.CSS_SELECTOR, "div.thumb_info > div > a > img").get_attribute('src')
    img_url = img_url.split('?')[0]  # '?' 이후의 문자열 제거
    
    reg_date = product.find_element(By.CSS_SELECTOR, "div.main_info > div.prod_sub_info > div.prod_sub_meta > dl").text
    
    product_info = {
        "제품명": name,
        "제품ID": product_id,
        "가격": price,
        "스펙": spec_list,
        "prod_danawa_href": prod_link,
        "이미지URL": img_url,
        "등록년월": reg_date
    }
    
    if save_images:
        save_image(img_url, product_id, category)
    
    print(f"제품명: {name}")
    print(f"제품ID: {product_id}")
    print(f"가격: {price}원" if price is not None else "가격: 정보 없음")
    print(f"스펙: {', '.join(spec_list)}")
    print(f"상품 링크: {prod_link}")
    print(f"이미지 URL: {img_url}")
    print(f"등록년월: {reg_date}")
    print("-" * 50)
    
    return product_info

def save_image(img_url, product_id, category):
    img_dir = os.path.join(PROJECT_ROOT, 'dataset', 'product-images', category)
    os.makedirs(img_dir, exist_ok=True)
    
    img_path = os.path.join(img_dir, f"{product_id}.jpg")
    
    response = requests.get(img_url)
    if response.status_code == 200:
        with open(img_path, 'wb') as f:
            f.write(response.content)
        print(f"이미지 저장 완료: {img_path}")
    else:
        print(f"이미지 다운로드 실패: {img_url}")

# 전역 변수로 데이터 저장소 생성
data_store = {}
data_store_lock = threading.Lock()

# 페이지별 크롤링 함수
def crawl_page(driver, page_num, category, save_images):
    print(f"\n{category} - {page_num}번째 페이지 품 정보:")
    scroll_to_bottom(driver)
    wait = WebDriverWait(driver, 20)
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.prod_item")))
    time.sleep(3)
    return extract_product_info(driver, category, save_images)

# JSON 파일에 데이터 추가 함수 (실시간 저장)
def append_to_json(product, filename):
    if os.path.exists(filename):
        with open(filename, 'r+', encoding='utf-8') as file:
            file_data = json.load(file)
            file_data.append(product)
            file.seek(0)
            json.dump(file_data, file, ensure_ascii=False, indent=4)
    else:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump([product], file, ensure_ascii=False, indent=4)

# 카테고리별 크롤링 함수
def crawl_category(url, category, save_images):
    print(f"카테고리 '{category}' 크롤링 시작")
    try:
        driver = setup_driver()
        driver.get(url)
        wait = WebDriverWait(driver, 20)
        page_num = 1
        max_pages = 1
        
        products_data = []
        products_count = 0

        while True:
            try:
                print(f"\n{category} - {page_num}번째 페이지 제품 정보 크롤링 중...")
                scroll_to_bottom(driver)
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.prod_item")))
                time.sleep(3)
                
                products = driver.find_elements(By.CSS_SELECTOR, "li.prod_item")
                for product in products:
                    try:
                        product_info = extract_product_info(product, category, save_images)
                        products_data.append(product_info)
                        products_count += 1
                    except Exception as e:
                        print(f"제품 정보 추출 중 오류 발생: {e}")
                
                print(f"{category}: 현재까지 {products_count}개의 제품 정보를 저장했습니다. (총 {page_num} 페이지)")
                
                # 다음 버튼 확인
                next_button = driver.find_elements(By.CSS_SELECTOR, "a.nav_next")
                if not next_button:
                    # 다음 버튼이 없을 경우, 페이지 번호 확인
                    page_numbers = driver.find_elements(By.CSS_SELECTOR, "div.number_wrap > a")
                    if page_numbers:
                        max_pages = max(int(page.text) for page in page_numbers if page.text.isdigit())
                    
                    if page_num >= max_pages:
                        print(f"{category} 마지막 페이지입니다. 크롤링을 종료합니다.")
                        break
                    else:
                        # 다음 페이지로 직접 이동
                        next_page_url = f"{url}&page={page_num + 1}"
                        driver.get(next_page_url)
                else:
                    driver.execute_script("arguments[0].click();", next_button[0])
                
                time.sleep(3)
                page_num += 1
            except Exception as e:
                print(f"{category} 페이지 처리 중 오류 발생: {e}")
                print(traceback.format_exc())
                break

    except Exception as e:
        print(f"카테고리 '{category}' 크롤링 중 오류 발생: {str(e)}")
        print(traceback.format_exc())
    finally:
        driver.quit()
        print(f"카테고리 '{category}' 크롤링 완료")
        print(f"총 {products_count}개의 제품 정보를 저장했습니다.")
        
        # 데이터를 전역 저장소에 저장
        with data_store_lock:
            data_store[category] = products_data

def save_data_to_files():
    output_dir = os.path.join(PROJECT_ROOT, 'dataset')
    os.makedirs(output_dir, exist_ok=True)
    
    for category, data in data_store.items():
        output_file = os.path.join(output_dir, f'{category}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"{category} 데이터를 {output_file}에 저장했습니다.")

# 데이터 압축 함수
def compress_data(output_dir):
    today = datetime.now().strftime("%Y%m%d")
    zip_filename = os.path.join(PROJECT_ROOT, 'dataset', 'history', f'data_{today}.zip')
    os.makedirs(os.path.dirname(zip_filename), exist_ok=True)
    
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for root, _, files in os.walk(output_dir):
            for file in files:
                if file.endswith('.json'):
                    zipf.write(os.path.join(root, file), 
                               os.path.relpath(os.path.join(root, file), output_dir))
    
    print(f"데이터 압축 완료: {zip_filename}")

# 메인 함수
def main(save_images=False, verbose=False):
    if verbose:
        print("상세 로그 모드 활성화")
    
    with open(os.path.join(PROJECT_ROOT, 'target-list.json'), 'r') as f:
        targets = json.load(f)

    threads = []
    for category, url in targets.items():
        thread = threading.Thread(target=crawl_category, args=(url, category, save_images))
        threads.append(thread)
        thread.start()
        print(f"{category} 크롤링 시작")

    for thread in threads:
        thread.join()

    print("모든 카테고리 크롤링 완료")
    
    # 데이터를 파일로 저장
    save_data_to_files()
    
    print("크롤링 결과:")
    for category, data in data_store.items():
        print(f"{category}: {len(data)}개 제품")
    
    # 누락된 카테고리 확인
    missing_categories = set(targets.keys()) - set(data_store.keys())
    if missing_categories:
        print("누락된 카테고리:")
        for category in missing_categories:
            print(f"- {category}")
    else:
        print("모든 카테고리가 정상적으로 크롤링되었습니다.")

    output_dir = os.path.join(PROJECT_ROOT, 'dataset')
    compress_data(output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Danawa 제품 정보 크롤러')
    parser.add_argument('--save-images', action='store_true', help='이미지 저장 여부')
    parser.add_argument('--verbose', action='store_true', help='상세 로그 출력')
    args = parser.parse_args()

    main(save_images=args.save_images, verbose=args.verbose)
