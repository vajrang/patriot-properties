import asyncio
import re

import aiofile
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

id_pattern = re.compile(r'Summary\.asp\?AccountNumber=(\d+)')


async def co_fetch_one(session, url):
    async with session.get(url) as response:
        return await response.text()


async def co_fetch_all(urls):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            task = asyncio.create_task(co_fetch_one(session, url))
            tasks.append(task)
        results = await asyncio.gather(*tasks)

    return results


def fetch_all(urls):
    retval = asyncio.run(co_fetch_all(urls))
    return retval


async def co_write_one(filename, text):
    async with aiofile.async_open(filename, 'w') as f:
        await f.write(text)


async def co_write_files(filenames, texts):
    tasks = []
    for filename, text in zip(filenames, texts):
        task = asyncio.create_task(co_write_one(filename, text))
        tasks.append(task)
    await asyncio.gather(*tasks)


def write_all(filenames, texts):
    asyncio.run(co_write_files(filenames, texts))


def parse_one_html(filename) -> pd.DataFrame:
    print(filename)
    text = ''
    df = None
    with open(filename, 'r') as f:
        text = f.read()

        f.seek(0)
        dfs = pd.read_html(f)
        assert len(dfs) == 3
        df = dfs[1].set_index('Parcel ID')

    bs = BeautifulSoup(text, 'lxml')
    tables = bs.find_all('table')
    assert len(tables) == 3
    table = tables[1]
    idtags = table.find_next('tbody').find_all('a', target='_top')

    for idtag in idtags:
        id = idtag.text
        hrefs = id_pattern.findall(idtag['href'])
        assert len(hrefs) == 1
        df.loc[id, 'Account Number'] = hrefs[0]

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.join(df['Beds  Baths'].str.split(n=1, expand=True))\
    .drop('Beds  Baths', axis=1)\
    .rename({0:'Beds', 1:'Baths'}, axis=1)
    df['Beds'] = df['Beds'].astype(float)
    df['Baths'] = df['Baths'].astype(float)

    df = df.join(df['Lot size  Fin area'].str.split(n=1, expand=True))\
        .drop('Lot size  Fin area', axis=1)\
        .rename({0:'Lot size', 1:'Fin area'}, axis=1)
    df['Lot size'] = df['Lot size'].str.replace(',', '').astype(float)
    df['Fin area'] = df['Fin area'].str.replace(',', '').astype(float)

    df = df.join(df['Sale date  Sale price'].str.split(n=1, expand=True))\
        .drop('Sale date  Sale price', axis=1)\
        .rename({0:'Sale date', 1:'Sale price'}, axis=1)
    df['Sale date'] = pd.to_datetime(df['Sale date'])
    df['Sale price'] = df['Sale price'].str.replace(r'[$,]', '', regex=True).astype(float)

    df = df.join(df['LUC  Description'].str.split(n=1, expand=True))\
        .drop('LUC  Description', axis=1)\
        .rename({0:'LUC', 1:'Description'}, axis=1)

    df = df.join(df['Built  Type'].str.split(n=1, expand=True))\
        .drop('Built  Type', axis=1)\
        .rename({0:'Built', 1:'Type'}, axis=1)

    df['Total Value'] = df['Total Value'].str.replace(r'[$,]', '', regex=True).astype(float)
    return df
