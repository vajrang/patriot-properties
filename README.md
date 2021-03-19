# Patriot Properties

A library to download data from patriotproprties.com for a given town.

## Basic Usage

```python
    # 0. create instance with given town name
    # must match http://{town}.patriotproperties.com
    pp = PatriotProperties('bedford')

    # 1. generates urls for all search pages
    urls = pp.step1_get_page_urls()

    # 2. downloads all the pages
    htmls = pp.step2_download_html_pages(urls)

    # 3. parses all HTMLs, concats and cleans into a pandas DataFrame
    df = pp.step3_read_from_htmls()
```

## Notes

- See ```examples.py``` for more.
- Known to work for ```bedford```.
