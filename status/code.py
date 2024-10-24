from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from tqdm import tqdm
from asyncio import Semaphore, gather, run, wait_for, sleep
import aiofiles
from aiohttp.client import ClientSession
import sys
from urllib.parse import urlencode, urljoin
from requests_html import HTML
import argparse
from uuid import uuid4
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, help="Starting page")
parser.add_argument("--limit", type=int, help="End page")
args = parser.parse_args()

AZURE_CONNECTION_STRING = ""
AZURE_CONTAINER_NAME = ""
MAX_TASKS = 20  # Max number of workers
MAX_TIME = sys.maxsize
DELAY = 2  # Delay between requests to avoid being flagged
BASE_URL = 'https://peraturan.bpk.go.id'
UU_TABLE = pd.DataFrame({'name': [], 'description': []}).set_index('name')
STATUS_TABLE = pd.DataFrame({'status_id': [],  'name_1': [], 'name_2': []}).set_index('status_id')

async def fetch_page(session, current_page, query_params):
    query_params['p'] = current_page
    params_str = urlencode(query_params)
    url = f"{BASE_URL}/Search?{params_str}"

    print(f"Fetching page {current_page} from {url}")

    # Add delay to avoid overwhelming the server
    await sleep(DELAY)

    async with session.get(url) as response:
        if response.status != 200:
            print(f"Failed to fetch page {current_page}: {response.status}")
            return []

        text = await response.text()

        # Parse the HTML using requests_html's HTML parser
        r_html = HTML(html=text)
        elements = r_html.find('.flex-grow-1')[1:]

        results = {'uu_rows': dict(),  'status_rows': dict()}

        for el in elements:
          name_element = el.find('.fw-semibold', first=True).text
          description_element = el.find('a', first=True).text
          results['uu_rows'][name_element] = description_element
          status_elements = el.find('li')
          for status_el in status_elements:
            status_description_element = status_el.find('span', first=True).text
            status_name_element = status_el.find('a', first=True)
            if status_name_element is not None:
              status_name_element = status_name_element.text
            else:
              status_name_element = status_description_element
            results['uu_rows'][status_name_element] = status_description_element
            results['status_rows'][uuid4()] = [name_element, status_name_element]

        print(f"Found {len(results['uu_rows'])} laws on page {current_page}")
        return results

async def scrape_pages(current_page, limit, max_page):
    tasks = []
    sem = Semaphore(MAX_TASKS)
    query_params = {
        'tentang': "",
        'nomor': "",
        'p': current_page,
    }

    async with ClientSession() as session:
        for i in range(current_page, limit + 1):
            if i > max_page:
                break
            # Wrap each page fetch in a task with a semaphore
            task = fetch_page_limited(session, i, query_params, sem)
            tasks.append(# Wait max seconds for each download
                wait_for(
                    task,
                    timeout=MAX_TIME,
                )
            )

        # Gather results from all tasks
        results = await gather(*tasks)

    # Flatten the results since each task returns a list of links
    global UU_TABLE
    global STATUS_TABLE

    uu_rows = dict()
    status_rows = dict()

    for result in results:
      uu_rows.update(result['uu_rows'])
      status_rows.update(result['status_rows'])

    for name in uu_rows:
      UU_TABLE.loc[name] = uu_rows[name]

    for status_id in status_rows:
      STATUS_TABLE.loc[status_id] = status_rows[status_id]

async def fetch_page_limited(session, current_page, query_params, sem):
    async with sem:
        return await fetch_page(session, current_page, query_params)

def get_container_client():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    return container_client

def push_to_azure(file, filename):
    container_client = get_container_client()
    blob_client = container_client.get_blob_client(f"src/db/{filename}")
    if blob_client.exists():
        return
    blob_client.upload_blob(file)

async def main():
    id = uuid4().hex
    uu_dest_file = f'uu-{id}.csv'
    status_dest_file = f'status-{id}.csv'
    current_page = args.offset
    max_page = float('inf')
    limit = args.limit
    directory_path = os.getcwd()

    await scrape_pages(current_page, limit, max_page)
    uu_csv = UU_TABLE.to_csv (index_label="name", encoding = "utf-8")
    status_csv = STATUS_TABLE.to_csv (index_label="status_id", encoding = "utf-8")
    push_to_azure(uu_csv, uu_dest_file)
    push_to_azure(status_csv, status_dest_file)

if __name__ == "__main__":
    run(main())