"""
Purpose:
    The script is used to go through each of the Mastodon files and

Inputs:
    - MBFC List
    - Streamer files
Output:
    None

Author: Pasan Kamburugamuwa
"""


import pandas as pd
import gzip, os, re, json
import yaml
import logging
import requests
from collections import defaultdict

#Read the MBFC list and store the data
df = pd.read_excel("MBFC List.xlsx")

#Remove the https, http and / from each item.
MBFC_SET = set(df['actual_URL'])


with open('config.yml') as f:
    config = yaml.safe_load(f)

#Base folder to save the extected mbfc-posts
base_folder = config['BASE_FOLDER']

#loggers
log_dir = config['LOG_DIR']
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_dir_path = os.path.join(log_dir, 'extracting_posts.log')
logging.basicConfig(filename=log_dir_path, level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s:%(message)s')

#Clean the Urls
def clean_urls(url):
    """
        Clean the MBFC urls
    :param :
        MBFC url
    :return:
        cleaned url
    """
    clean_url = re.sub(r'^https?://(www\.)?', '', url)
    # Remove any trailing slash
    clean_url = clean_url.rstrip('/')
    return clean_url

def make_common(file_name):
    """
        Get the common file name for each Mastodon instance
    :param file_name:
    :return:
    """
    match = re.match(r'([^_]*)_', file_name)
    if match:
        return match.group(1)
    return file_name

df['cleaned_URL'] = df['actual_URL'].apply(clean_urls)

# Convert the cleaned URLs to a set
MBFC_SET = set(df['cleaned_URL'])

# Step 01
# Extract the posts which having MBFC links
#############################################
logging.info("============Start extracting posts with MBFC links=============")
# Iterate through each file in the folder.
for month in config['MONTHS']:
    #Get the month folder
    month_folder = os.path.join(config['DATA_DIR'], month)

    #Go through each date folder
    for date in os.listdir(month_folder):
        #Go through each mastodon instances data files
        if re.match(r'\d{4}-\d{2}-\d{2}', date):
            dates_files = os.path.join(month_folder, date)
            for file in os.listdir(dates_files):
                lines_to_write = []
                file_path = os.path.join(dates_files, file)
                logging.info(f"Getting the file to read - {file_path}")
                with gzip.open(file_path, 'rt') as gzip_file:
                    for line in gzip_file:
                        included_mbfc = False
                        #Going through each word of a line
                        for word in line.split():
                            if word in MBFC_SET:
                                included_mbfc = True
                                break

                        if included_mbfc:
                            lines_to_write.append(line)

                extracted_data_file_path = os.path.join(base_folder, f"mbfc-posts", f"{make_common(file)}.json")

                # Create the directory if they not exist
                os.makedirs(os.path.dirname(extracted_data_file_path), exist_ok=True)
                with open(extracted_data_file_path, 'a') as f:
                    f.writelines(lines_to_write)
                logging.info(f"Successfully extracted MBFC posts from the file - {file_path}")

# Step 02
# Extract the users from posts and assign the home account id for each post.
##############################################
logging.info("============Start extract users from posts and assign home account id=============")
# Get the list extracted MBFC post dir
mbfc_posts_dir = config['MDFC_POST_DIR']

accounts_folder = os.path.join(base_folder, "accounts")
os.makedirs(accounts_folder, exist_ok=True)

# Iterate over each file in the specified directory
for file in os.listdir(config['MDFC_POST_DIR']):
    post_file = os.path.join(config['MDFC_POST_DIR'], file)

    # Define the output file path
    extracted_account_file_path = os.path.join(accounts_folder, f"{file}")

    with open(extracted_account_file_path, 'a') as outfile:
        # Open and read each file
        with open(post_file) as f:
            for line in f:
                try:
                    # Parse each line as JSON
                    data = json.loads(line)

                    mastodon_instance_url = re.sub(r"@[^/]+", "", data['account']['url'])

                    account_name = file.replace(".json", "")

                    # This will avoid redundant calling the get home account id for in house received accounts.
                    if account_name in mastodon_instance_url:
                        account = {
                            "post_id": data['id'],
                            "post_uri": data['uri'],
                            "account": data['account'],
                            "home_acc_id": data['account']['id']
                        }
                    else:
                        # Account lookup API endpoint to get the account id.
                        endpoint = f'{mastodon_instance_url}/api/v1/accounts/lookup'
                        params = {"acct": data['account']['username']}
                        response = requests.get(endpoint, params=params)
                        if response.status_code == 200:
                            account = {
                                "post_id": data['id'],
                                "post_uri": data['uri'],
                                "account": data['account'],
                                "home_acc_id": response.json()['id']
                            }
                    # Write the "acct" value to the output file
                    outfile.writelines(json.dumps(account) + '\n')
                except (json.JSONDecodeError, KeyError, IndexError):
                    # Handle possible errors in parsing or missing fields
                    logging.error(f"Error processing line: {line}")
    logging.info(f"Successfully extracted accounts from the file - {post_file}")


#Step 03
#Remove duplicates and get accounts having 10 or more than 10 posts, get all accounts to a file.
logging.info("============Start remove duplicates and get accounts having more than 10 posts shared=============")
# Get the list of accounts folder
account_ids_dir = config['ACCOUNT_DIR']

#Account follower directory
accounts_followers_dir = os.path.join(base_folder, "removed_duplicates")
os.makedirs(accounts_followers_dir, exist_ok=True)

# Dictionary to keep track of post counts per (mastodon_instance_url, home_account_id, post_id) tuple
post_counts = defaultdict(int)

# Count posts for each (mastodon_instance_url, home_account_id, post_id) combination
for acc_file in os.listdir(account_ids_dir):
    home_acc_file = os.path.join(account_ids_dir, acc_file)

    with open(home_acc_file) as f:
        for line in f:
            data = json.loads(line)
            mastodon_instance_url = re.sub(r"@[^/]+", "", data['account']['url'])
            home_account_id = data['home_acc_id']
            post_id = data['post_id']
            unique_key = (mastodon_instance_url, home_account_id, post_id)
            post_counts[unique_key] += 1

# Filter and remove account duplicates based on (mastodon_instance_url, home_account_id)
unique_accounts = set()
removed_duplicates_accounts_file_path = os.path.join(accounts_followers_dir, "removed_duplicates_accounts.json")

with open(removed_duplicates_accounts_file_path, 'a') as outfile:
    for acc_file in os.listdir(account_ids_dir):
        home_acc_file = os.path.join(account_ids_dir, acc_file)

        with open(home_acc_file) as f:
            for line in f:
                data = json.loads(line)
                mastodon_instance_url = re.sub(r"@[^/]+", "", data['account']['url'])
                home_account_id = data['home_acc_id']
                post_id = data['post_id']
                unique_key = (mastodon_instance_url, home_account_id, post_id)

                # Check if user has 2 or more posts and is not a duplicate
                if post_counts[unique_key] >= 2 and (mastodon_instance_url, home_account_id) not in unique_accounts:
                    unique_accounts.add((mastodon_instance_url, home_account_id))
                    data_updated = {
                        "account": data["account"],
                        "home_acc_id": data["home_acc_id"]
                    }
                    outfile.write(json.dumps(data_updated) + '\n')
# Step 04
# Get followers for each account
############################################
logging.info("============Start get followers for each account=============")
# Get the list of accounts folder
removed_duplicates_account_ids_dir = config['REMOVED_DUPLICATE_ACCOUNT_DIR']

#Account follower directory
accounts_followers_dir = os.path.join(base_folder, "followers")
os.makedirs(accounts_followers_dir, exist_ok=True)

# Handle pagination to get all followers.
def get_all_followers(base_url, account_id, limit=40, max_id=None, since_id=None, min_id=None):

    all_followers = []
    endpoint = f'{base_url}{account_id}/followers'

    params = {'limit': limit}

    if max_id:
        params['max_id'] = max_id
    if since_id:
        params['since_id'] = since_id
    if min_id:
        params['min_id'] = min_id

    while True:
        response = requests.get(endpoint, params=params)

        if response.status_code != 200:
            logging.error(f"Failed to retrieve followers: {response.status_code}")
            break

        followers = response.json()
        if not followers:
            break

        all_followers.extend(followers)

        # Check for pagination in Link header
        if 'Link' in response.headers:
            links = response.headers['Link']
            next_link = None
            for link in links.split(','):
                if 'rel="next"' in link:
                    next_link = link.split(';')[0].strip('<> ')
                    break
            if next_link:
                # Extract query parameters from next_link
                params = {param.split('=')[0]: param.split('=')[1] for param in next_link.split('?')[1].split('&')}
            else:
                break
        else:
            break

    return all_followers



removed_duplicates_file = os.path.join(removed_duplicates_account_ids_dir, "removed_duplicates_accounts.json")
# Define the output file path
account_followers_file_path = os.path.join(accounts_followers_dir, f"account_followers.json")
with open(account_followers_file_path, 'a') as outfile:
    # Open and read each file
    with open(removed_duplicates_file) as f:
        for line in f:
            data = json.loads(line)
            mastodon_instance_url = re.sub(r"@[^/]+", "", data['account']['url'])
            home_account_id = data['home_acc_id']
            base_url = f'{mastodon_instance_url}api/v1/accounts/'
            all_followers = get_all_followers(base_url, home_account_id)
            account_followers = {"followers": all_followers}
            data.update(account_followers)
            outfile.write(json.dumps(data) + '\n')


