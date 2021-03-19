import pandas as pd

from patriotproperties import PatriotProperties


def main():
    pp = PatriotProperties('bedford')
    # urls = pp.step1_get_page_urls()
    # htmls = pp.step2_download_html_pages(urls)
    # df = pp.step3_read_from_htmls()
    # df.to_csv('bedford.csv', index=False)

    df = pd.read_csv('bedford.csv')

    types = [
        'SNGL-FAM-RES',
        'TWO-FAM-RES',
        'THREE-FM-RES',
        'CONDOMINIUM',
    ]
    df = df[df['Description'].isin(types)]

    return df


if __name__ == '__main__':
    df = main()
