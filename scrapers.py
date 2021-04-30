# -*- coding: utf-8 -*-
import json
import logging
import scrapy
import pymysql
import json
from pprint import pformat
from itertools import chain
from dbconfig import mysql_prop

logger = logging.getLogger(__name__)


class MainScraper(scrapy.Spider):
    handle_httpstatus_list = [403, 404, 503]
    allowed_domains = []
    start_urls = []

    @classmethod
    def update_settings(cls, settings):
        identifier = settings.get('identifier')
        db_settings = cls.get_db_settings(identifier)

        cls.custom_settings = db_settings.get('settings', {})
        settings.setdict(cls.custom_settings or {}, priority='spider')

        settings_copy = settings.copy_to_dict()
        logger.debug('VIEW SETTINGS:\n %s' % pformat(settings_copy))

    @classmethod
    def get_db_settings(cls, identifier):
        cls.conn = pymysql.connect(**mysql_prop)

        cls.cursor = cls.conn.cursor()
        sql = "SELECT id, identifier, content FROM `organizations` WHERE `identifier`=%s"
        cls.cursor.execute(sql, (identifier,))
        result = cls.cursor.fetchone()

        if result:
            return json.loads(result[2])
        else:
            logger.error('Nothing wrong')
            return

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        identifier = kwargs.get('identifier')

        if identifier:
            cls.identifier = identifier

        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)

        return spider

    def start_requests(self):

        self.allowed_domains = self.settings.get('ALLOWED_DOMAINS', [])
        self.start_urls = self.settings.get('START_URLS', [])

        request_params = self.start_request_params()

        for url in self.start_urls:
            yield scrapy.Request(url, **request_params)

    def start_request_params(self):
        params = {
            'callback': self.parse,
            'meta': {
                'handle_httpstatus_list': self.handle_httpstatus_list,
                'max_proxies_to_try': self.settings.get('START_REQUESTS_MAX_PROXIES_TO_TRY', 30)
            },
            'cookies': self.settings.get('START_REQUESTS_COOKIES', None),
            'headers': self.settings.get('START_REQUESTS_HEADERS', None),
            'dont_filter': True
        }

        return params

    def parse(self, response):
        pass


class PagesScraper(MainScraper):

    def parse(self, response):

        for req in chain(self.start_requests_iter(response)):
            yield req

    def fetch_entry_pages(self):
        return self.settings['ENTRY_PAGES']

    def start_requests_iter(self, response):
        for entry in self.fetch_entry_pages():
            for url in entry['links']:
                yield self.list_url_request(url,
                                            meta={'organization': entry.get('organization'),
                                                  'target': entry.get('target', ''),
                                                  'filter': entry.get('filter', '')
                                                  })

    def list_url_request(self, list_url, **kwargs):
        options = {
            'callback': self.scrape_listing_pages,
            'dont_filter': True,
        }
        options.update(kwargs)

        return scrapy.Request(list_url, **options)

    def scrape_listing_pages(self, response):
        raise NotImplementedError

    def scrape_page(self, response):
        raise NotImplementedError
