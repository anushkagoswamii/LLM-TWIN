# Libraries
import os, re
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException
from webdriver_manager.chrome import ChromeDriverManager
from tempfile import mkdtemp

from helper import push_to_mongo, get_company_url, get_company_names
from logger import Logger
from dotenv import load_dotenv
load_dotenv()


class LinkedInCrawler():

    def __init__(self, username, password):
        self.LINKEDIN_USERNAME = username
        self.LINKEDIN_PASSWORD = password
        # Initialise Logger to store logs for post links and post pushed into MongoDb
        self.logger = Logger(path=os.getenv('LOG_PATH'))
        # Set options for WebDriver
        self.service = ChromeService(ChromeDriverManager().install())
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--headless=new")
        self.options.add_argument("--start-maximized")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--log-level=3")
        self.options.add_argument("--disable-popup-blocking")
        self.options.add_argument("--disable-notifications")
        self.options.add_argument("--disable-dev-tools")
        self.options.add_argument("--ignore-certificate-errors")
        self.options.add_argument("--no-zygote")
        self.options.add_argument(f"--user-data-dir={mkdtemp()}")
        self.options.add_argument(f"--data-path={mkdtemp()}")
        self.options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        # Give permission to the WebDriver to get data from clipboard
        prefs = {"profile.content_settings.exceptions.clipboard": {
            '*': {'setting': 1}
        }}
        self.options.add_experimental_option('prefs', prefs)

    def check_options(self, get_last_days, resume, full_extract):
        if full_extract and (get_last_days or resume):
            raise ValueError('Cannot scrape full data with last day or resume')
        if get_last_days and resume:
            raise ValueError('Cannot scrape last day posts from log. Either last day or resume has to be false')

    def extract_post_links(self, company_links, company_names_list, get_last_days=True, resume=False, full_extract=False):
        log = self.logger.get_log()
        if not resume:
            if not company_links:
                company_links = log['posts'].keys()
            for company_name, link in zip(company_names_list, company_links):
                print(f"Started extracting links for {company_name}")
                self.get_post_links(link, get_last_days, full_extract)

    def extract_post_data(self, company_links, company_names_list):
        log = self.logger.get_log()
        if not company_links:
            company_links = log['posts'].keys()
        for company_name, link in zip(company_names_list, company_links):
            if link in log['posts']:
                self.scrape_data(link, log['posts'][link], company_name)

    def extract(self, company_names=None, get_last_days=False, resume=True, full_extract=False):
        # Checks if the options set for extraction are valid
        self.check_options(get_last_days, resume, full_extract)
        # Get company names and urls from comma separated company names
        company_names_list = get_company_names(company_names)
        company_links = get_company_url(company_names)

        try:
            # Initialise driver and login into LinkedIn
            self.driver = webdriver.Chrome(service = self.service, options = self.options)
            self.login()
            # Extract post links and then scrape data from those posts
            self.extract_post_links(company_links, company_names_list, get_last_days, resume, full_extract)
            self.extract_post_data(company_links, company_names_list)

        except (KeyboardInterrupt, NoSuchWindowException):
            pass

        finally:
            self.logger.update_status()
            self.driver.quit()

    def get_last_posts(self, post_items, days):
        selected_items = []
        tries=10
        for item in post_items:
            try:
                x, t = item.find_element(By.CLASS_NAME, 'update-components-actor__sub-description-link').get_attribute('aria-label').split()[:-1]
            except:
                continue
            if t in ['week', 'weeks', 'month', 'months', 'year', 'years']:
                tries-=1
            elif t in ['second', 'seconds', 'minute', 'minutes', 'hour', 'hours']:
                selected_items.append(item)
            elif t in ['day', 'days']:
                if int(x)<days:
                    selected_items.append(item)
        return selected_items if len(selected_items)>0 or tries>0 else None

    def no_new_item(self, post_items, exisiting_ids):
        for item in post_items:
            if item.get_attribute("data-urn").split(":")[-1] not in exisiting_ids:
                return False
        return True

    def get_post_links(self, url, get_last_days, full_extract):
        self.get_post_content(url)
        
        # Get already pushed post ids
        existing_post_ids = []
        log = self.logger.get_log()
        if url in log['posts']:
            existing_post_ids.extend([[ele for ele in re.split(r'-|/|:', link) if ele.isdigit() and len(ele)>10][0] for link in log['posts'][url]])
        if url in log['pushed_posts']:
            existing_post_ids.extend([[ele for ele in re.split(r'-|/|:', link) if ele.isdigit() and len(ele)>10][0] for link in log['pushed_posts'][url]])

        # Post Links Extraction
        post_items = set()
        buttons = set()
        previous_scroll_pos = self.driver.execute_script("return window.scrollY;")
        last_days = int(os.getenv('EXTRACT_LAST_DAYS'))
        max_posts = int(os.getenv('EXTRACT_LAST_POSTS'))
        no_new_item_tries = 30
        ct=0
        
        while True:
            self.driver.execute_script("window.scrollBy(0, 500);")
            sleep(0.5)
            try:
                new_items = set(WebDriverWait(self.driver, 3).until(
                                    EC.presence_of_all_elements_located((By.CLASS_NAME, 'feed-shared-update-v2'))
                                )) - post_items
            except:
                new_items = []

            if get_last_days:
                new_items = self.get_last_posts(new_items, last_days)
                if new_items is None:
                    break
            elif not full_extract:
                if max_posts<len(buttons):
                    break
                if self.no_new_item(new_items, existing_post_ids):
                    no_new_item_tries-=1
                    if not no_new_item_tries:
                        break
                else:
                    no_new_item_tries = 30

            post_items.update(new_items)
            buttons.update([self.get_driver_element(item, By.CLASS_NAME, "feed-shared-control-menu__trigger")
                            for item in new_items 
                            if full_extract or 
                            (item.get_attribute('data-urn').split(':')[2]=='activity' and 
                            item.get_attribute('data-urn').split(':')[-1] not in existing_post_ids)
                            ])

            new_scroll_pos = self.driver.execute_script("return window.scrollY;")
            if new_scroll_pos == previous_scroll_pos:
                ct+=1
                if ct==10:
                    break
            else:
                ct=0
            previous_scroll_pos = new_scroll_pos

        if not last_days and not full_extract:
            buttons = list(buttons)[:min(len(buttons),max_posts)]
        print(f'{len(buttons)} posts Found')

        for button in buttons:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", button)
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.ID, button.get_attribute('id')))).click()
                sleep(1)
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'option-share-via'))).click()
                copied_link = self.driver.execute_script('return navigator.clipboard.readText()')
                self.logger.add_log((url, copied_link))
            except:
                pass

        print(f'Extracted {len(self.logger.get_log()["posts"][url]) if url in self.logger.get_log()["posts"] else 0 - len(log["posts"][url]) if url in log["posts"] else 0} urls from {url}')

    def get_element_text(self, element, tag, class_, default=None):
        try:
            return element.find(tag, class_=class_).get_text().strip()
        except:
            return default

    def get_comments_data(self, post_id):
        # Show all comments and replies
        self.expand_all_comments()

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        comments_data = []

        # get all comment items
        comment_items = soup.find_all("article", class_="comments-comment-item comments-comments-list__comment-item")

        for comment_item in comment_items:
            comment_id = comment_item.get('data-id').split(',')[-1].strip(') ')
            comment_text = comment_item.find("span", class_="comments-comment-item__main-content").get_text().strip()
            comment_author_profile_url = comment_item.find("a").get('href')
            comment_likes = self.get_element_text(comment_item, "button", "comments-comment-social-bar__reactions-count", "0")
            comment_replies = re.findall(r'\d+\,?\d*', self.get_element_text(comment_item, "span", "comments-comment-social-bar__replies-count", "0"))[0]
            comments_data.append(self.new_comment(comment_author_profile_url, comment_id, comment_text, post_id, comment_likes, comment_replies))

            # get all reply items in a comment
            reply_items = comment_item.find_all("article", class_="comments-comment-item comments-reply-item reply-item")
            for reply_item in reply_items:
                reply_id = reply_item.get('data-id').split(',')[-1].strip(') ')
                reply_text = reply_item.find("div", class_="comments-reply-item-content-body").find("span", {"dir": "ltr"}).get_text(separator="!@#$").strip().split("!@#$")[-1]
                reply_author_profile_url = reply_item.find("a").get('href')
                reply_likes = self.get_element_text(reply_item, "button", "comments-comment-social-bar__reactions", "0")
                comments_data.append(self.new_comment(reply_author_profile_url, reply_id, reply_text, comment_id, reply_likes, None))

        return comments_data

    def get_social_data(self):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        social_count = [soc_count.strip() for soc_count in soup.find("div", class_="social-details-social-counts").get_text().split('\n') if soc_count.strip()]
        social_data = {'likes': '0', 'comments': '0', 'reposts': '0'}
        for key, val in zip([re.sub(r'\d+\,?\d*', '', text).strip() for text in social_count], [re.findall(r'\d+\,?\d*', text)[0] for text in social_count]):
            if key=='':
                social_data['likes'] = val
            elif key=='comment' or key=='comments':
                social_data['comments'] = val
            else:
                social_data['reposts'] = val
        return social_data.values()

    def get_hashtags(self, post):
        hashtags = re.findall(r'#\w+', post)
        return hashtags

    def get_driver_element(self, element, tag, class_, wait=3):
        return WebDriverWait(element, wait).until(
            EC.presence_of_element_located((tag, class_))
        )

    def get_soup_element(self, element, tag, class_):
        try:
            return element.find(tag, class_=class_)
        except:
            return None

    def get_content(self):
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        content = {'Text': None, 'Image': [], 'Video': []}
        
        image_item = self.get_soup_element(soup, "div", "update-components-image")
        if image_item:
            for img in image_item.find_all('img'):
                content['Image'].append(img.get('src'))

        video_item = self.get_soup_element(soup, "div", "update-components-linkedin-video")
        if video_item:
            for vid in video_item.find_all('video'):
                content['Video'].append((vid.get('poster'), vid.get('src')))

        text_item = self.get_soup_element(soup, "div", "update-components-text")
        if text_item:
            content["Text"] = text_item.get_text().strip()

        content = {key: value for key, value in content.items() if value}
        return content

    def get_like_data(self):
        try:
            button = self.driver.find_element(By.CLASS_NAME, 'social-details-social-counts__count-value')
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", button)
            sleep(1)
            button.click()
            sleep(1)
            win = self.driver.find_element(By.CLASS_NAME, 'social-details-reactors-modal__content')
            previous_scroll_pos = self.driver.execute_script("return arguments[0].scrollTop;", win)
            tries=3

            while tries:
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", win)
                sleep(1)
                new_scroll_pos = self.driver.execute_script("return arguments[0].scrollTop;", win)
                if new_scroll_pos == previous_scroll_pos:
                    tries-=1
                    try:
                        self.driver.find_element(By.CLASS_NAME, 'scaffold-finite-scroll__load-button').click()
                    except:
                        pass
                else:
                    tries=3
                previous_scroll_pos = new_scroll_pos

            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            profiles = [link.get('href') for link in soup.find('div', class_='social-details-reactors-modal__content').find_all('a')]

            self.driver.find_element(By.CLASS_NAME, 'artdeco-modal__dismiss').click()
            return profiles

        except:
            return []

    def scrape_data(self, url, links, company_name):

        inserted_ids = []

        for i, link in enumerate(links):

            self.driver.get(link)
            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Get post id
            post_id = soup.find("div", class_="feed-shared-update-v2").get("data-urn").split(":")[-1]
            # Get post owner
            post_owner = self.get_name(soup.find("span", class_="update-components-actor__name"))
            # Get post content
            content = self.get_content()
            # Get social data
            like_count, comment_count, repost_count = self.get_social_data()
            # Get likes data
            like_data = self.get_like_data()
            # Get comments data
            comment_data = self.get_comments_data(post_id)
            # Get post hashtags
            hashtags = self.get_hashtags(content['Text'] if 'Text' in content else "")
            

            data = {
                '_id': post_id,
                'date': datetime.now().strftime("%d-%m-%Y"),
                'company_name': company_name,
                'post_owner': post_owner, 
                'content': content,
                'like_count': like_count,
                'comment_count': comment_count,
                'repost_count': repost_count,
                'likes_data': like_data,
                'comments_data': comment_data,
                'hashtags': hashtags,
                'keywords': None,
                'clean_post': None,
                'topic': None
            }

            inserted_ids.append(push_to_mongo(data, os.getenv("POSTS_COLLECTION")))
            self.logger.pop_log((url, link))
            print(f"{company_name} url_{i+1}:", "Link processed successfully")

        return inserted_ids

    def get_company_name(self, company_link):
        return company_link.split('/')[-2] if company_link[-1]=='/' else company_link.split('/')[-1]

    def expand_all_comments(self):

        previous_scroll_pos = self.driver.execute_script("return window.scrollY;")
        while True:
            self.driver.execute_script("window.scrollBy(0, 500);")

            try:
                button = self.driver.find_element(By.CLASS_NAME, 'show-prev-replies')
                while button:
                    self.driver.execute_script("arguments[0].click();", button)
                    button = self.driver.find_element(By.CLASS_NAME, 'show-prev-replies')
            except:
                pass

            new_scroll_pos = self.driver.execute_script("return window.scrollY;")
            if new_scroll_pos == previous_scroll_pos:
                try:
                    button = self.driver.find_element(By.CLASS_NAME, 'comments-comments-list__load-more-comments-button')
                    self.driver.execute_script("arguments[0].click();", button)
                    sleep(1)
                except:
                    break

            previous_scroll_pos = new_scroll_pos
        
        # go back to top
        self.driver.execute_script("window.scrollTo(0,0);")

    def new_comment(self, user_profile, comment_id, comment_text, parent_id, comment_likes, comment_replies):
        new_comment = {
            "user_profile": user_profile,
            "comment_id": comment_id,
            "text": comment_text,
            "reaction_count": comment_likes,
            "reply_count": comment_replies,
            "parent_id": parent_id
        }
        return new_comment
    
    def get_name(self, soup):
        return soup.get_text(separator="!@#$").strip().split("!@#$")[1]

    def get_post_content(self, url):
        if url[-1]=='/':
            self.driver.get(url+'posts/')
        else:
            self.driver.get(url+'/posts/')
        sleep(2)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def login(self):
        self.driver.get("https://www.linkedin.com/login")
        if not self.LINKEDIN_USERNAME and not self.LINKEDIN_PASSWORD:
            raise ValueError("LinkedIn scraper requires an valid account to perform extraction")

        self.driver.find_element(By.ID, "username").send_keys(self.LINKEDIN_USERNAME)
        self.driver.find_element(By.ID, "password").send_keys(self.LINKEDIN_PASSWORD)
        self.driver.find_element(By.CSS_SELECTOR, ".login__form_action_container button").click()
        WebDriverWait(self.driver, 60).until( lambda _: self.driver.current_url == 'https://www.linkedin.com/feed/' )
        print("**LOGGED IN**")


# MAIN
if __name__ == "__main__" :

    crawler = LinkedInCrawler(username=os.getenv('LINKEDIN_USERNAME'), 
                            password=os.getenv('LINKEDIN_PASSWORD'))

    crawler.extract(os.getenv('COMPANY_NAMES'),
                    get_last_days=True,
                    resume=False,
                    full_extract=False)

