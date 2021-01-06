import gc
import json
import time
import random
import numpy as np
from tqdm import tqdm
from pprint import pprint
from multiprocessing import Pool
from selenium.webdriver.common.by import By
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


DELAY = 7
NUM_RETRY = 5
BATCH_SIZE = 5000
NUM_PROCESSES = 4


# based on https://leimao.github.io/blog/Python-tqdm-Multiprocessing/
def run_apply_multiprocessing(func, argument_list, num_processes):
    with Pool(processes=num_processes) as p:
        jobs = list(tqdm(p.imap(func, argument_list), total=len(argument_list)))
    return jobs


def process_urls(urls):
    options = ChromeOptions()

    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    options.add_argument('--headless')
    options.add_argument("--test-type")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-first-run")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--ignore-certificate-errors")
    chrome = Chrome('./data/chromedriver', options=options)

    error_urls = []
    user_idx_aliases = {}
    try:
        for url in urls:
            user_id = url.split("/")[-1]
            chrome.get(url)
            actual_name = None
            try:
                not_ok = False
                to_break = False
                e = None
                for i in range(NUM_RETRY):
                    if to_break:
                        break
                    try:
                        wait = WebDriverWait(chrome, DELAY)
                        actual_name = wait.until(
                            EC.presence_of_element_located(
                                (By.CLASS_NAME, "actual_persona_name")
                            )
                        )
                        not_ok = False
                        to_break = True
                        e = None
                    except Exception as ex:
                        not_ok = True
                        e = ex
                if not_ok:
                    raise e
            except TimeoutException:
                error_urls.append(url)
                continue
            except Exception:
                error_urls.append(url)
                continue
            actual_name = actual_name.get_attribute("textContent").strip()

            wait = WebDriverWait(chrome, DELAY)
            button = wait.until(
                EC.element_to_be_clickable(
                    (By.CLASS_NAME, "namehistory_link")
                )
            )
            button.click()
            time.sleep(DELAY)

            wait = WebDriverWait(chrome, DELAY)
            old_names = wait.until(
                EC.presence_of_element_located(
                    (By.ID, "NamePopupAliases")
                )
            )
            old_names = old_names.find_elements_by_tag_name("p")
            old_names = [x.get_attribute("textContent").strip() for x in old_names]
            if 'This user has no known aliases' in old_names:
                old_names = []
            old_names.append(actual_name)
            user_idx_aliases[user_id] = old_names
    except Exception:
        pass
    finally:
        chrome.quit()
    gc.collect()
    return user_idx_aliases, error_urls


with open("./data/user_info.json") as f:
    user_info = json.loads(f.read())
user_urls = list(set([v['user_url'] for k, v in user_info.items()]))
error_urls = []
user_idx_aliases = {}
random.shuffle(user_urls)

try:
    url_chunks = [user_urls[i * BATCH_SIZE:(i + 1) * BATCH_SIZE] for i in range(int(np.ceil(len(user_urls) / BATCH_SIZE)))]
    results = run_apply_multiprocessing(process_urls, url_chunks, num_processes=NUM_PROCESSES)
    for r in results:
        uidx, err = r
        user_idx_aliases.update(uidx)
        error_urls.extend(err)
except Exception as e:
    pprint(e)
finally:
    pass

with open('./data/user_idx_aliases1.json', 'w') as f:
    json.dump(user_idx_aliases, f)
with open('./data/error_urls1.json', 'w') as f:
    json.dump(error_urls, f)
print("That's all, folks!")
