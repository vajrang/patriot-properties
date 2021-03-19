import os
import re
from multiprocessing import Pool
from string import Template

import pandas as pd
import requests
from bs4 import BeautifulSoup

import utils


class PatriotProperties():

    GIS_URL_ROOT_TEMPLATE = Template('http://${town}.patriotproperties.com/')
    GIS_ORIGIN_PAGE = 'SearchResults.asp'
    DATA_FOLDER_ROOT = 'data'

    def __init__(self, town: str, datafolder: str = None) -> None:
        if not datafolder:
            datafolder = f'{self.DATA_FOLDER_ROOT}/{town}/'

        self.urlroot = self.GIS_URL_ROOT_TEMPLATE.substitute(town=town)
        self.town = town
        self.datafolder = datafolder

        os.makedirs(self.datafolder, exist_ok=True)

    def step1_get_page_urls(self) -> list[str]:
        one_page = requests.get(f'{self.urlroot}{self.GIS_ORIGIN_PAGE}?page=1').text
        bs = BeautifulSoup(one_page, 'lxml')
        last_page_link = bs.find('a', target='bottom', text='Last Page')['href']
        f = re.findall(r'SearchResults\.asp\?page=(\d+)', last_page_link)
        assert len(f) == 1
        num_pages = int(f[0])

        retval = []
        for i in range(1, num_pages + 1):
            retval.append(f'{self.urlroot}{self.GIS_ORIGIN_PAGE}?page={i}')
        return retval

    def step2_download_html_pages(self, urls: list[str]):
        pages = utils.fetch_all(urls)
        filenames = [f'{self.datafolder}page{i}.html' for i in range(1, len(pages) + 1)]
        utils.write_all(filenames, pages)

    def step3_read_from_htmls(self) -> pd.DataFrame:
        files = [f'{self.datafolder}{file}' for file in os.listdir(self.datafolder)]

        dfs = []
        with Pool() as pool:
            res = pool.map(utils.parse_one_html, files)
            dfs = list(res)
        assert len(dfs) == len(files)

        data = pd.concat(dfs).reset_index()
        data = utils.clean_data(data)
        return data
