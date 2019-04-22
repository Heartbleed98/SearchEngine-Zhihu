import scrapy
import mysql.connector
from bs4 import BeautifulSoup
from scrapy import Request
import datetime
import re

class ZhihuSpider(scrapy.Spider):
    name = 'Zhihu'

    allowed_domains = ['zhihu.com']

    start_urls = ['https://www.zhihu.com/question/27914723']

    mydb = mysql.connector.connect(
	  host="localhost",
	  user="root",
	  passwd="121314",
	  database="spider"
	)
    mycursor = mydb.cursor()
    
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        if response.url in self.start_urls:
            self.mycursor.execute("SELECT * FROM `urls` WHERE `url` = '" + response.url + "'")
            lines = self.mycursor.fetchall()
            if len(lines) == 0:
                dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.mycursor.execute("INSERT INTO `page` (`url`, `datetime`, `data`) VALUES (%s, %s, %s)", (response.url, dt, str(soup.find('body').text)))
                self.mycursor.execute("INSERT INTO `urls` (`url`) VALUES ('" + response.url + "')")
        else:
            dt=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.mycursor.execute("INSERT INTO `page` (`url`, `datetime`, `data`) VALUES (%s, %s, %s)", (response.url, dt, str(soup.find('body').text)))
            self.mycursor.execute("INSERT INTO `urls` (`url`) VALUES ('" + response.url + "')")

        self.mydb.commit()

        new_urls = soup.find_all('a', href=True)
        for new_url in new_urls:
            href = str(new_url['href'])
            if href[0] == '/':
                href = "https:" + href
            self.mycursor.execute("SELECT * FROM urls WHERE url = '" + href + "'")
            lines = self.mycursor.fetchall()
            if len(lines) == 0:
                yield Request(url = href, callback = self.parse, )

        # new_urls = soup.find_all(text=re.compile('https://www\.zhihu\.com/question[0-9a-z/]*'))

        # for new_url in new_urls:
        #     href = str(new_url)
        #     if href[0] == '/':
        #         href = "https:" + href
        #     self.mycursor.execute("SELECT * FROM urls WHERE url = '" + href + "'")
        #     lines = self.mycursor.fetchall()
        #     if len(lines) == 0:
        #         yield Request(url = href, callback = self.parse, )

        # str = 'https://www.zhihu.com/question/27914723'
        # m = re.search('https://www\.zhihu\.com/question[0-9a-z/]*', str)
        # if m:
        #     print("Match")
