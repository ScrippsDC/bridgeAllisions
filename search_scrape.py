from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
import bs4,pandas,math,os,random,time


SEARCH_URL = "https://cgmix.uscg.mil/IIR/IIRSearch.aspx"
SAVE_FILE = "data/source/IIR/IIR_search.csv"
MAX_WAIT = 60
ROWS_PER_PAGE = 10
CURRENT_PAGE_START = 1
NEXT_PAGE_START = 2
RANDOM_SEED = 1
random.seed(RANDOM_SEED)



def search(dr):
    dr.get(SEARCH_URL)
    kw_box = dr.find_element(By.NAME, "TextBoxKeyWord")
    kw_box.clear()
    kw_box.send_keys("bridge")

    dt_box = dr.find_element(By.NAME, "TextBoxFromDate")
    dt_box.clear()
    dt_box.send_keys("01/01/2000")
    dt_box.send_keys(Keys.RETURN)
    WebDriverWait(dr, MAX_WAIT).until(expected_conditions.presence_of_element_located((By.ID, "lblTopMessage")))
    return


def parse_title(text):
    text_ls = text.split("\n")
    activity_number = text_ls[0].split(":")[1].strip()
    title = text_ls[1].split(":")[1].strip()
    start_date = text_ls[2].split(":")[1].strip().split(" ")[0]
    end_date = text_ls[3].split(":")[1].strip()
    return {"activity_id":activity_number, "title":title, "start_dt":start_date, "end_dt":end_date}


def get_record_count(dr):
    soup = bs4.BeautifulSoup(dr.page_source)
    return int(soup.find(id="lblTopMessage").get("title").split(" ")[0])

def get_page_data(s):
    titles = []
    for tr in s.find_all("tr"):
        title = tr.get('title',None)
        if title:
            titles.append(parse_title(title))
    return pandas.DataFrame(titles)

def nav_page(dr, page_num):
    dr.execute_script(f"__doPostBack('GridViewIIR','Page${str(page_num)}')")
    if page_num%10 == 1:
        WebDriverWait(dr, MAX_WAIT).until(expected_conditions.visibility_of_element_located((By.LINK_TEXT, str(page_num+1))))
    else:
        WebDriverWait(dr, MAX_WAIT).until(expected_conditions.visibility_of_element_located((By.LINK_TEXT, str(page_num-1))))
    return

def nav_next_page(dr,page):
    next_page = dr.find_element(By.LINK_TEXT, str(page))
    next_page.click()
    WebDriverWait(dr, MAX_WAIT).until(expected_conditions.visibility_of_element_located((By.LINK_TEXT, str(page-1))))

def get_data_from_driver(dr):
    html_response = dr.page_source
    soup = bs4.BeautifulSoup(html_response)
    return get_page_data(soup)


prev_max_page = 0
next_page_num = NEXT_PAGE_START
mode = "w"
header = True
if os.path.exists(SAVE_FILE):
    done_data = pandas.read_csv(SAVE_FILE)
    prev_max_page = done_data["page"].max()
    mode = "a"
    header = False


driver = webdriver.Firefox()
search(driver)
record_count = get_record_count(driver)
max_pages = math.floor(record_count/ROWS_PER_PAGE)+1
if mode == "w":
    print("writing first page")
    data = get_data_from_driver(driver)
    data["page"] = 1
    data.to_csv(SAVE_FILE, mode=mode, header=header, index=False)
    mode = "a"
    header = False
while True:
    if next_page_num > max_pages:
        break
    if next_page_num > prev_max_page:
        print("navigating to page", next_page_num)
        time.sleep(random.gauss(1,.1))
        nav_page(driver, next_page_num)
        data = get_data_from_driver(driver)
        data["page"] = next_page_num
        data.to_csv(SAVE_FILE, mode=mode, header=header, index=False)
    next_page_num += 1