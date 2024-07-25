import os
import glob
import gzip
import json
import requests
from urllib.parse import urlparse
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import skew

base_dir = "/Users/pkamburu/codingpractice"

def load_author_accounts():
    """
    Load author follower summary
    """
    # Get the filter author file
    author_accounts = []
    author_follower_summary = os.path.join(base_dir, "all_data.json")
    with open(author_follower_summary, 'r') as file:
        for line in file:
            try:
                # Parse the JSON object from the line
                author_data = json.loads(line)
                author_accounts.append(author_data)
            except json.JSONDecodeError:
                print(f"Error decoding JSON from line: {line}")
    return author_accounts


def plot_skewness(author_data_list):
    """
    Plot the skewness of the tot_followings_count field.
    """
    # Convert to DataFrame
    df = pd.DataFrame(author_data_list)

    # Ensure tot_followings_count is numeric and drop NaNs
    df['total_followers_count'] = pd.to_numeric(df['total_followers_count'], errors='coerce')
    df = df.dropna(subset=['total_followers_count'])

    # Compute skewness
    skewness = skew(df['total_followers_count'])

    plt.figure(figsize=(10, 6))
    plt.hist(df['total_followers_count'], bins=30, edgecolor='black')
    plt.title(f"Distribution of total_followers_count (Skewness: {skewness:.2f})")
    plt.xlabel('Total Followers Count')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


def get_followers(all_authors):
    """
    Get the followers for each user
    """

    for author in all_authors:
        if author['tot_followings_count'] >= 5000:
            username = author['username']
            domain = author['domain']
            get_home_acc_id_api = f'https://{domain}/api/v1/accounts/lookup'
            params = {"acct": username}

            response = requests.get(get_home_acc_id_api, params=params)
            if response.status_code == 200:
                account_id = response.json()['id']
                all_followers = get_all_followers_endpoint(domain, account_id)

                # Calculate total followers count directly
                total_followers_count = len(all_followers)

                # Update author dictionary
                author['total_followers_count'] = total_followers_count

                # Write updated author to file
                all_data_dir = os.path.join(base_dir, "all_data.json")
                with open(all_data_dir, 'a') as f:
                    f.write(json.dumps(author) + '\n')

def get_all_followers_endpoint(instance, account_id, limit=80):
    """
    Call get followings API to collect all followers for each user

    Parameters:
    Input:
    instance : Mastodon instance name
    account_id : Mastodon account name
    limit : number of elements received per call

    Returns:
    Account followings
    """
    all_followings = []
    endpoint = f'https://{instance}/api/v1/accounts/{account_id}/followers'

    params = {'limit': limit}

    while True:
        response = requests.get(endpoint, params=params)

        if response.status_code != 200:
            break

        try:
            followings = response.json()
        except requests.exceptions.JSONDecodeError:
            break

        if not followings:
            break

        all_followings.extend(followings)

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
    return all_followings

all_followers_data_list = load_author_accounts()
# get_followers(author_data_list)
plot_skewness(all_followers_data_list)
