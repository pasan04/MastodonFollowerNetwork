"""
Purpose:
    The script is used to go grab the link counts for each Mastodon Server.
        1. How many users appeared.
        2. Count how many MBFC links shared in content.
Inputs:
    - MBFC List
    - Streamer files
Output:
    None

Author: Pasan Kamburugamuwa
"""
import json
import pandas as pd
import glob
import gzip
import os
import logging
import yaml
import re
from typing import List, Dict
import time
from urllib.parse import urlparse


with open('config.yml') as f:
    config = yaml.safe_load(f)

#Base folder to save the extected mbfc-posts
base_folder = config['BASE_DIR']

#MBFC file path
mbfc_file_path = os.path.join(base_folder, "MBFC.csv")

#Data directory
data_folder = config['DATA_DIR']

log_dir_path = os.path.join(base_folder, 'mstdn_links_count.log')
logging.basicConfig(filename=log_dir_path, level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

def read_csv_file(path: str) -> List[str]:
    """
    :param path: path to csv
    :return: str
    """
    mbfc_list_data = pd.read_csv(path)
    mbfc_data = mbfc_list_data.fillna('')
    domain_list = []
    for index, row in mbfc_data.iterrows():
        urls = row['actual_URL'].split(',')
        for url in urls:
            cleaned_url = url.strip().replace("https://", "").replace("www.", "").replace("http://", "").replace("/", "")
            domain_list.append(cleaned_url)
    return domain_list

def parse_domain_and_username(url):
    """
    Extract the domain and username
    Parameters:
    url: Account url
    Returns:
    domain, username
    """
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    username = parsed_url.path.split('/')[-1].strip("@")
    return domain, username

#Get all the gzip files
def list_gz_files(directory) -> List:
    """
    Go through each directory and list out the gz files.

    :param directory: Path to main directory.
    :return: All gzip directories.
    """
    gz_files = []
    for root, dirs, files in os.walk(directory):
        for file in glob.glob(os.path.join(root, "*.gz")):
            gz_files.append(file)
    return gz_files

def get_authors_with_mbfc_links(all_gz_files: List[str], domain_list: List[str]) -> None:
    """
    Go through each gz file, parse JSON content, and check if any content contains the domains from domain_list.

    :param all_gz_files: List of paths to gzipped JSON files.
    :param domain_list: List of domains to check for in the content.
    :return: List of authors whose content contains any of the domains.
    """
    all_urls_collected = 0
    all_mbfc_domains_appeared  = 0
    no_of_posts = 0
    no_of_posts_with_valid_content = 0
    start_time = time.time()
    domain_pattern = re.compile('|'.join(re.escape(domain) for domain in domain_list))

    #Go through each gz file.
    for gz_file in all_gz_files:
        logging.info(f"Start extracting data from file - {gz_file}")
        with gzip.open(gz_file, 'rt', encoding='utf-8') as file:
            for line in file:
                no_of_posts += 1
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if 'content' in data and 'account' in data and data['content']:
                    no_of_posts_with_valid_content +=1
                    content = data['content']
                    url_pattern = re.compile(r'href="(https?://[^"]+)"')

                    urls_in_content = url_pattern.findall(content)
                    urls_with_mbfc_domain = [url for url in urls_in_content if domain_pattern.search(url)]
                    all_urls_collected += len(urls_in_content)
                    all_mbfc_domains_appeared += len(urls_with_mbfc_domain)

        end_time = time.time()
        time_diff = end_time - start_time
        logging.info(f"End of extracting data from file - {gz_file} - time taken :{time_diff}")
        logging.info(f"Total number of posts with in {gz_file} : {no_of_posts}")
        logging.info(f"Number of posts with valid content in {gz_file} : {no_of_posts_with_valid_content}")
        logging.info(f"All urls within the {gz_file} : {all_urls_collected}")
        logging.info(f"All MBFC domains apperead in {gz_file} : {all_mbfc_domains_appeared}")
        logging.info("######################################################################")

if __name__ == '__main__':

    domain_list = read_csv_file(mbfc_file_path)
    all_gz_files = list_gz_files(data_folder)

    get_authors_with_mbfc_links(all_gz_files, domain_list)