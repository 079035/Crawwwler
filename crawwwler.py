import requests
import lxml
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin

import re
from scrapy.spiders import Spider
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from w3lib.url import url_query_cleaner
import extruct

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)

class Crawwwler:
    def __init__(self, urls=[]):
        self.visited=[]
        self.to_visit = urls

    def download(self, url):
        return requests.get(url).text

    def get_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def add_url(self, url):
        if url not in self.visited and url not in self.to_visit:
            self.to_visit.append(url)

    def crawl(self, url):
        html = self.download(url)
        for u in self.get_urls(url, html):
            self.to_visit.append(u)

    def run(self):
        while self.to_visit:
            url = self.to_visit.pop(0)
            logging.info(f'crawling: {url}')
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f'Failed to crawl: {url}')
            finally:
                self.visited.append(url)

def process(links):
    for link in links:
        link.url = url_query_cleaner(link.url)
        yield link

class ImbdCrawwwler(CrawlSpider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/']
    rules = (
        Rule(
            LinkExtractor(
                deny=[
                    re.escape('https://www.imdb.com/offsite'),
                    re.escape('https://www.imdb.com/whitelist-offsite'),
                    ],
            ),
            process_links=process,
            callback='parse',
            follow=True
        ),
    )

    def parse(self, response):
        return {
            'url': response.url,
            'metadata': extruct.extract(
                response.text,
                response.url,
                syntaxes=['opengraph', 'json-ld']
            ),
        }
"""
if __name__ == '__main__':
    Crawwwler(urls=['https://www.imdb.com/']).run()
"""
