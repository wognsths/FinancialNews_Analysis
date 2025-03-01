import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../.."))
sys.path.insert(0, project_root)

import time
import datetime
import schedule
import pandas as pd
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from Collection.Crawler.utils.logger import setup_logger

class NaverGlobalCrawler:
    def __init__(self, output_dir: str, driver_path: str = None, year: str = "2015"):
        # super().__init__(output_dir)  
        self.output_dir = output_dir
        self.year = year  # year by argument (ex: "2015")
        self.base_url = "https://news.naver.com/breakingnews/section/101/262"
        self.logger = setup_logger(log_file='./utils/naver_global.log')
        self.output_file = os.path.join(self.output_dir, f'Naver_Global_{self.year}.csv')
        self.detailed_articles = []

    def start_browser(self):
        self.logger.info('Initialize browser...')
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--start-maximized')
        # chrome_options.add_argument('--headless')
        try:
            if hasattr(self, 'driver_path') and self.driver_path and os.path.exists(self.driver_path):
                self.driver = webdriver.Chrome(
                    service=Service(self.driver_path),
                    options=chrome_options
                )  
            else:
                self.driver = webdriver.Chrome(options=chrome_options)
            self.logger.info('Browser initialized successfully!')
        except Exception as e:
            self.logger.info('Something went wrong... Message below:')
            self.logger.info(e)
            sys.exit(1)

    def scrape_articles(self, date):
        self.logger.info('Starting crawling process...')
        self.start_browser()

        self.driver.get(f'{self.base_url}?date={date}')
        time.sleep(1)

        last_height = self.driver.execute_script('return document.body.scrollHeight')

        # Scroll Down & Click [LOAD MORE] Button
        while True:
            self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(0.5)
            try:
                self.driver.find_element(By.CSS_SELECTOR, 'a.section_more_inner._CONTENT_LIST_LOAD_MORE_BUTTON').click()
            except:
                self.logger.info('No more <Load more articles> button available')
                break
            time.sleep(0.5)
            new_height = self.driver.execute_script('return document.body.scrollHeight')
            if new_height == last_height:
                break
            last_height = new_height

        time.sleep(0.5)
        article_list = []
        self.detailed_articles = []
        news_items = self.driver.find_elements(By.CSS_SELECTOR, "div.sa_item_inner div.sa_item_flex")

        cnt = 0
        for item in tqdm(news_items, desc='Scraping Meta'):
            try:
                title_element = item.find_element(By.CSS_SELECTOR, "strong.sa_text_strong")
                title = title_element.text.strip()

                link_element = item.find_element(By.CSS_SELECTOR, "div.sa_text a")
                link = link_element.get_attribute("href")

                press_element = item.find_element(By.CSS_SELECTOR, "div.sa_text_press")
                press = press_element.text.strip() if press_element else ""

                article_list.append([title, press, link])
            except Exception:
                self.logger.info(f"[ERROR] Failed to extract article metadata, Count: {cnt}")
                cnt += 1
                pass

        for (title, press, link) in tqdm(article_list, desc='Scraping Details'):
            try:
                self.driver.get(link)
                time.sleep(0.5)

                try:
                    body_elem = self.driver.find_element(By.CSS_SELECTOR, 'article#dic_area')
                    body = body_elem.text.strip()
                except:
                    self.logger.info(f'[ERROR] Failed to fetch article content. Link: {link}')
                    continue

                b, d, r, ur = "", "", "", ""
                try:
                    comment_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.u_cbox_comment_box.u_cbox_type_profile")
                    for item in comment_elements:
                        try:
                            b_elem = item.find_element(By.CSS_SELECTOR, "span.u_cbox_contents")
                            b += f"{b_elem.text.strip()}\n"

                            d_elem = item.find_element(By.CSS_SELECTOR, "div.u_cbox_info_base span.u_cbox_date")
                            d += f"{d_elem.get_attribute('data-value')}\n"

                            r_elem = item.find_element(By.CSS_SELECTOR, "a.u_cbox_btn_recomm em.u_cbox_cnt_recomm")
                            r += f"{r_elem.text.strip()}\n"

                            ur_elem = item.find_element(By.CSS_SELECTOR, "a.u_cbox_btn_unrecomm em.u_cbox_cnt_unrecomm")
                            ur += f"{ur_elem.text.strip()}\n"
                        except:
                            pass
                except:
                    pass

                try:
                    e_elems = self.driver.find_elements(By.CSS_SELECTOR, "span.u_likeit_list_count._count")
                    emo = [e.text.strip() for e in e_elems]
                    emotion = f"Good:{emo[5]} Warm:{emo[6]} Sad:{emo[7]} Angry:{emo[8]} Want:{emo[9]}"
                except:
                    emotion = ""
                
                try:
                    n_elems = self.driver.find_element(By.CSS_SELECTOR, "span.u_cbox_count")
                    num_comment = n_elems.text.strip()
                except:
                    num_comment = ""

                self.detailed_articles.append({
                    'Title': title,
                    'Date': date,
                    'Press': press,
                    'Link': link,
                    'Body': body,
                    'Emotion': emotion,
                    'Comment_body': b,
                    'Comment_date': d,
                    'Comment_recomm': r,
                    'Comment_unrecomm': ur,
                    'Num_comment': num_comment
                })
            except Exception as e:
                self.logger.info(f"[ERROR] Failed to extract detail article({link}): {e}")
        
        self.driver.quit()

    def save_to_database(self):
        if not self.detailed_articles:
            self.logger.info('No articles to save')
            if hasattr(self, 'driver'):
                self.driver.quit()
            return
        
        df = pd.DataFrame(self.detailed_articles)
        
        if os.path.exists(self.output_file):
            df.to_csv(self.output_file, mode='a', header=False, encoding='utf-8-sig')
        else:
            df.to_csv(self.output_file, index=False, encoding='utf-8-sig')

        self.logger.info(f'[INFO] Finished scraping | PATH -> {self.output_file}')

    def set_start_date(self) -> str:
        if os.path.exists(self.output_file):
            df = pd.read_csv(self.output_file)
            if 'Date' in df.columns and not df.empty:
                latest_date = df['Date'].max()
                start_date = datetime.datetime.strptime(str(latest_date), "%Y%m%d") + datetime.timedelta(days=1)
                return start_date.strftime("%Y%m%d")
        return f"{self.year}0101"

def run_crawler():
    output_dir = "../../Database/Local/Naver"
    driver_path = "./chromedriver"
    
    # Pass year to commandline (ex: python NaverNewsGlobal.py 2015)
    year_arg = sys.argv[1] if len(sys.argv) > 1 else "2015"
    
    crawler = NaverGlobalCrawler(output_dir, driver_path, year=year_arg)
    start_date_str = crawler.set_start_date()
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")

    end_date = start_date + datetime.timedelta(days=1)
    today = datetime.datetime.today()

    if end_date > today:
        end_date = today

    print(f"Crawling Start: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    current_date = start_date
    while current_date < end_date:
        crawler.scrape_articles(current_date.strftime("%Y%m%d"))
        current_date += datetime.timedelta(days=1)
        print(f"Current Date: {current_date.strftime('%Y-%m-%d')}")

    crawler.save_to_database()
    print(f"✅ Finished crawling: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

schedule.every(1/3600).hours.do(run_crawler)


# $env:PYTHONPATH="C:\Users\Shon\Documents\FinalcialNews_Analysis"
if __name__ == "__main__":
    run_crawler()
    print("🔄")
    while True:
        schedule.run_pending()
