"""
Purpose:
    The script is used to go through each of the Mastodon files and grab the
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

log_dir_path = os.path.join(base_folder, 'mstdn_logger.log')
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

def get_authors_with_mbfc_links(all_gz_files: List[str], domain_list: List[str]) -> str:
    """
    Go through each gz file, parse JSON content, and check if any content contains the domains from domain_list.

    :param all_gz_files: List of paths to gzipped JSON files.
    :param domain_list: List of domains to check for in the content.
    :return: List of authors whose content contains any of the domains.
    """
    no_of_mbfc_post = 0
    no_of_posts = 0
    start_time = time.time()
    domain_pattern = re.compile('|'.join(re.escape(domain) for domain in domain_list))

    #Go through each gz file.
    for gz_file in all_gz_files:
        logging.info(f"Start extracting data from file - {gz_file}")
        with gzip.open(gz_file, 'rt', encoding='utf-8') as file:
            for line in file:
                no_of_posts += 0
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if 'content' in data and 'account' in data and data['content']:
                    content = data['content']
                    url_pattern = re.compile(r'href="(https?://[^"]+)"')

                    all_urls_in_content = url_pattern.findall(content)
                    urls_with_domain = [url for url in all_urls_in_content if domain_pattern.search(url)]
                    author = data['account']['url']
                    user_identifier = username_extract(author)

                    if domain_pattern.search(content):
                        no_of_mbfc_post += 1
                        encoded_line = line.encode('utf-8')
                        write_to_author_file(user_identifier, encoded_line)

                    # Write to the json file with the name of the file.
                    record = {
                        "post_id": data.get('id', 'N/A'),
                        "post_url": data.get('url', 'N/A'),
                        "post_uri": data.get('uri', 'N/A'),
                        "acc_url": author,
                        "username": username_extract(author),
                        "no_all_urls": len(all_urls_in_content),
                        "no_mbfc_domains": len(urls_with_domain)
                    }

                    write_author_summary(record, user_identifier)

        end_time = time.time()
        time_diff = end_time - start_time
        logging.info(f"End of extracting data from file - {gz_file} - time taken :{time_diff}")
    return no_of_mbfc_post, no_of_posts

def username_extract(url_parsed_str: str) -> str:
    """
    Extract the domain and username from the given URL string.

    :param url_parsed_str: The URL to parse.
    :return: A user identifier string
    """
    try:
        parsed_url = urlparse(url_parsed_str)
        domain = parsed_url.netloc
        username = parsed_url.path.split('/')[-1].strip("@")
        return f"{domain}@{username}"
    except Exception as e:
        logging.error(f"Exception occurred when extracting username - {e}")
        return None


def write_to_author_file(author: str, line: bytes) -> None:
    """
    Write the author file with specific content.

    :param author: Name of the author.
    :param line: Line passed.
    :return: None.
    """
    author_file_path = os.path.join(base_folder, "authors", f"mbfc_posts_{author}.json.gz")
    logging.info(f"writing to {author_file_path}")
    try:
        if not os.path.exists(os.path.dirname(author_file_path)):
            os.makedirs(os.path.dirname(author_file_path))
        with gzip.open(author_file_path, 'ab') as f:
            f.write(line)
    except Exception as e:
        logging.error(f"Failed to append to {author_file_path}, error: {e}")

def write_author_summary(record: Dict, author: str) -> None:
    """
    Write summary data to a JSON file with each record on a new line.

    :param data: Dictionary containing the summary data.
    :param author: Author identifier used to name the output file.
    :param base_folder: Base directory where the JSON file will be saved.
    """
    try:
        # Define the file path
        file_path = os.path.join(base_folder, "author_summary", f"author_{author}.json")

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Write the record to the file on a new line
        with open(file_path, 'a', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False)
            f.write('\n')  # Write a newline character after each JSON object
    except Exception as e:
        logging.error(f"Error occurred while saving data - {e}")

if __name__ == '__main__':
    script_start_time = time.time()
    domain_list = read_csv_file(mbfc_file_path)
    all_gz_files = list_gz_files(data_folder)

    no_of_mbfc_post,no_of_posts  = get_authors_with_mbfc_links(all_gz_files, domain_list)

    logging.info("#############################################")
    # Count how many users have
    authors_dir = os.path.join(base_folder, "authors")
    all_author_files = list_gz_files(authors_dir)
    logging.info(f"Number of collected MBFC authors(files) - {len(all_author_files)}")
    logging.info(f"Number of MBFC posts - {no_of_mbfc_post}")
    logging.info(f"Time taken to process files - {time.time() -script_start_time}")






