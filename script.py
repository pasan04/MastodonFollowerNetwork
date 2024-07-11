"""
Purpose:
    The script is used to go through each of the Mastodon files and

Inputs:
    None

Output:
    None

Author: Pasan Kamburugamuwa
"""


import pandas as pd
import gzip, os, re, json
import yaml

#Read the MBFC list and store the data
df = pd.read_excel("MBFC List.xlsx")

#Remove the https, http and / from each item.
MBFC_SET = set(df['actual_URL'])

with open('config.yml') as f:
    config = yaml.safe_load(f)

#Clean the Urls
def clean_urls(url):
    clean_url = re.sub(r'^https?://(www\.)?', '', url)
    # Remove any trailing slash
    clean_url = clean_url.rstrip('/')
    return clean_url

def make_common(file_name):
    match = re.match(r'([^_]*)_', file_name)
    if match:
        return match.group(1)
    return file_name

df['cleaned_URL'] = df['actual_URL'].apply(clean_urls)

# Convert the cleaned URLs to a set
MBFC_SET = set(df['cleaned_URL'])

print(MBFC_SET)
# #Iterate through each file in the folder
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

                extracted_data_file_path = os.path.join(month_folder, f"mbfc-posts", f"{make_common(file)}.json")

                # Create the directory if they not exist
                os.makedirs(os.path.dirname(extracted_data_file_path), exist_ok=True)
                with open(extracted_data_file_path, 'w') as f:
                    f.writelines(lines_to_write)



