import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import pymongo
import json
import time
from multiprocessing import Pool
client = pymongo.MongoClient()
db = client['lagou']


headers = {
    'Cookie': '_ga=GA1.2.1564323471.1533559871; user_trace_token=20180806205111-655ad495-9977-11e8-b72b-525400f775ce; LGUID=20180806205111-655ad82a-9977-11e8-b72b-525400f775ce; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1533559871,1534122957; index_location_city=%E6%88%90%E9%83%BD; JSESSIONID=ABAAABAABEEAAJAA8EE27FDD25258B7B2CC1CFCB213519E; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1534126713; LGSID=20180813091558-6f3b8c86-9e96-11e8-bb10-525400f775ce; LGRID=20180813101834-2e4dcc11-9e9f-11e8-bb1a-525400f775ce; _gid=GA1.2.461412857.1534122957; TG-TRACK-CODE=index_navigation; SEARCH_ID=9242e5f8b3f84d9cb536911a8819d7a1; X_HTTP_TOKEN=9b6efda664937a4f49ea8dde2f4a2161; ab_test_random_num=0; _putrc=1F750A8943745476123F89F2B170EADC; login=true; unick=%E5%BC%A0%E5%85%83%E9%AB%98; hasDeliver=0; gate_login_token=b4cde2fd3b4bf6505323a14ed96edf4518f9699b10501a1839019bd0cc08eef5',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
# 获取页面源码函数
def get_html(url):

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            print('ok')
            return resp.text
        return None
    except RequestException:
        return None


# soup = BeautifulSoup(get_page_resp(url), 'lxml')
# all_positions = soup.select('div.category-list > a')
# print(all_positions)
# print(len(all_positions))
# joburls = [i['href'] for i in all_positions]
# jobnames = [i.get_text() for i in all_positions]
# print(joburls)
# print(jobnames)

def parse_index():
    url = 'https://www.lagou.com/'
    soup = BeautifulSoup(get_html(url), 'lxml')
    all_positions = soup.select('div.category-list > a')
    joburls = [i['href'] for i in all_positions]
    jobnames = [i.get_text() for i in all_positions]

    for joburl, jobname in zip(joburls, jobnames):
        data = {
            'url': joburl,
            'name': jobname
        }
        # 这里使用yield语法糖，不熟悉的同学自己查看资料哦
        yield data
        # print(data)

def parse_link(url, MONGO_TABLE):
    for page in range(1, 31):
        link = '{}{}/?filterOption=3'.format(url, str(page))
    # link = 'https://www.lagou.com/zhaopin/Java/1/?filterOption=3'
        resp = requests.get(link, headers=headers)
        if resp.status_code == 404:
            pass
        else:
            soup = BeautifulSoup(resp.text, 'lxml')

            positions = soup.select('ul > li > div.list_item_top > div.position > div.p_top > a > h3')
            adds = soup.select('ul > li > div.list_item_top > div.position > div.p_top > a > span > em')
            publishs = soup.select('ul > li > div.list_item_top > div.position > div.p_top > span')
            moneys = soup.select('ul > li > div.list_item_top > div.position > div.p_bot > div > span')
            needs = soup.select('ul > li > div.list_item_top > div.position > div.p_bot > div')
            companys = soup.select('ul > li > div.list_item_top > div.company > div.company_name > a')
            tags = []
            if soup.find('div', class_='li_b_l'):
                tags = soup.select('ul > li > div.list_item_bot > div.li_b_l')
            fulis = soup.select('ul > li > div.list_item_bot > div.li_b_r')

            for position,add,publish,money,need,company,tag,fuli in zip(positions,adds,publishs,moneys,needs,companys,tags,fulis):
                data = {
                    'position' : position.get_text(),
                    'add' : add.get_text(),
                    'publish' : publish.get_text(),
                    'money' : money.get_text(),
                    'need' : need.get_text().split('\n')[2],
                    'company' : company.get_text(),
                    'tag' : tag.get_text().replace('\n','-'),
                    'fuli' : fuli.get_text()
                }
                save_database(data, MONGO_TABLE)

def save_database(data, MONGO_TABLE):
    if db[MONGO_TABLE].insert_one(data):
        print('保存数据库成功', data)

def main(data):
    url = data['url']
    print(url)
    mongo_table = data['name']
    if mongo_table[0] == '.':
        mongo_table = mongo_table[1:]
    parse_link(url, mongo_table)


if __name__ == '__main__':
    t1 = time.time()

    pool = Pool()

    datas = (data for data in parse_index())
    pool.map(main, datas)
    pool.close()
    pool.join()

    print(time.time() - t1)