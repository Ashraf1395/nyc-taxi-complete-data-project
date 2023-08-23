import requests
from bs4 import BeautifulSoup
from collections import defaultdict

class LinkExtractor:
    def __init__(self, year, url):
        self.year = year
        self.url = url

    def extract_links(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.text, 'html.parser')
        link_tags = soup.find_all('a')
        links = defaultdict(list)
        
        final_year = '' if self.year == 'all' else self.year
        
        for link_tag in link_tags:
            href = link_tag.get('href')
            if f"yellow_tripdata_{final_year}" in href:
                month = href.split('-')[2].split('.')[0]
                year_num = href.split('_')[2].split('-')[0]
                if self.year == 'all':
                    links[year_num].append(href)
                else:
                    links[month].append(href)
        
        return links
