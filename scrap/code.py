from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import fitz
from tqdm import tqdm
from asyncio import Semaphore, gather, run, wait_for, sleep
import aiofiles
from aiohttp.client import ClientSession
import sys
from urllib.parse import urlencode, urljoin
from requests_html import HTML
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--offset", type=int, help="Starting page")
parser.add_argument("--limit", type=int, help="End page")
args = parser.parse_args()

AZURE_CONNECTION_STRING = ""
AZURE_CONTAINER_NAME = ""
MAX_TASKS = 20
MAX_TIME = sys.maxsize
DELAY_BETWEEN_REQUESTS = 5
DELAY = 2  # Delay between requests to avoid being flagged
BASE_URL = 'https://peraturan.bpk.go.id'
PDF_LINKS = []  # List to store all PDF links

def preprocess(directory_path):
    directory_files = os.listdir(directory_path)
    for _, file in enumerate(tqdm(directory_files, desc=f"Processing PDFs", total=len(directory_files)-2)):
        if file.endswith(".pdf") or file.endswith(".PDF") or file.endswith(".Pdf"):
            dest_file = os.path.join(directory_path, file)
            try:
                with fitz.open(dest_file) as pdf:
                    text = get_texts(pdf)
                filename = file[:-3] + 'txt'
                push_to_azure(text, filename)
                print(filename)
                os.remove(dest_file)
            except:
                pass

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
        pdf_elements = r_html.find('a', containing='.pdf')

        # Manually resolve relative URLs with the base URL
        page_pdf_links = [urljoin(BASE_URL, el.attrs.get('href')) for el in pdf_elements if el.attrs.get('href')]

        print(f"Found {len(page_pdf_links)} PDFs on page {current_page}")
        return page_pdf_links

async def scrape_pages(current_page, limit, max_page):
    tasks = []
    sem = Semaphore(MAX_TASKS)
    query_params = {
        'keywords': "",
        'tentang': "",
        'nomor': "",
        'p': current_page,
    }

    async with ClientSession(trust_env=True) as session:
        for i in range(current_page, limit + 1):
            if i > max_page:
                break
            # Wrap each page fetch in a task with a semaphore
            task = fetch_page_limited(session, i, query_params, sem)
            tasks.append(task)

        # Gather results from all tasks
        all_pdf_links = await gather(*tasks)

    # Flatten the results since each task returns a list of links
    global PDF_LINKS
    PDF_LINKS = [link for sublist in all_pdf_links for link in sublist]

async def fetch_page_limited(session, current_page, query_params, sem):
    async with sem:
        return await fetch_page(session, current_page, query_params)

async def download(pdf_list):
    tasks = []
    sem = Semaphore(MAX_TASKS)

    async with ClientSession(trust_env=True) as sess:
        for pdf_url in pdf_list:
            dest_file = pdf_url.split("/")[-1]

            if len(dest_file) > 255:
                dest_file = dest_file[:250] + ".pdf"
                
            tasks.append(
                # Wait max seconds for each download
                wait_for(
                    download_one(pdf_url, sess, sem, dest_file),
                    timeout=MAX_TIME,
                )
            )

        return await gather(*tasks)

async def download_one(url, sess, sem, dest_file):
    async with sem:
        print(f"Downloading {url}")

        await sleep(DELAY_BETWEEN_REQUESTS)
        async with sess.get(url) as res:
            content = await res.read()

        # Check everything went well
        if res.status != 200:
            print(f"Download failed: {res.status}")
            return

        async with aiofiles.open(dest_file, "+wb") as f:
            await f.write(content)
            # No need to use close(f) when using with statement

def get_texts(pdf):
    text = ""
    for page in pdf:
        text += page.get_text().replace("\n", "")
    text = " ".join(text.split())
    return text.encode()

def get_container_client():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    return container_client

def push_to_azure(file, filename):
    container_client = get_container_client()
    blob_client = container_client.get_blob_client(f"src/ocr/{filename}")
    if blob_client.exists():
        return
    blob_client.upload_blob(file)

async def main():
    current_page = args.offset
    max_page = float('inf')
    limit = args.limit
    directory_path = os.getcwd()

    #await scrape_pages(current_page, limit, max_page)
    #await download(PDF_LINKS)
    preprocess(directory_path)

if __name__ == "__main__":
    run(main())