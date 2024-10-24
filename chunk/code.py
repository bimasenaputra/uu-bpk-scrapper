from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import re
from tqdm import tqdm

AZURE_CONNECTION_STRING = ""
AZURE_CONTAINER_NAME = ""

def chunk(blob):
  for _, blob in enumerate(tqdm(blob_list, desc=f'Chunking texts')):
    complete_text = pull_from_azure(blob)
    # For saving the results for each chapters
    chapters = dict()
    matches = re.finditer(r'(?i)MEMUTUSKAN\s*:', complete_text)
    matches_start_index = [chunk.start(0) for chunk in matches]
    print(len(matches_start_index))
    for i in range(len(matches_start_index)):
      # Track which chapter is currently processed
      max_num = 0
      match_begin_index = matches_start_index[i]
      if i + 1 < len(matches_start_index):
        match_end_index = matches_start_index[i+1]
        chunk = complete_text[match_begin_index:match_end_index-1]
      else:
        chunk = complete_text[match_begin_index:]

      # TODO: Feed the chunk to a LLM to get how many chapters there are & their contents
      # Doing it without LLM is impossible because there's no indentation in the parsed documents
      # Which is the only cue to differentiate an actual chapter to the content of another chapter

    for chapter_num, chapter_text in chapters.items():
      push_to_azure(chapter_text.encode(), blob, chapter_num)

def get_container_client():
  blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
  container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
  return container_client

def get_blob_list():
  container_client = get_container_client()
  blob_client = container_client.get_blob_client(f"src/ocr")
  blob_list = container_client.list_blobs()
  blob_list = [blob.split("/")[-1] for blob in blob_list]
  return blob_list

def push_to_azure(file, filename, chapter):
  container_client = get_container_client()
  blob_client = container_client.get_blob_client(f"src/chunk/{filename[:-4]}/{chapter}.txt")
  blob_client.upload_blob(file)

def pull_from_azure(filename):
  container_client = get_container_client()
  blob_client = container_client.get_blob_client(f"src/ocr/{filename}")
  content = ''
  if blob_client.exists():
    blob_txt_content = blob_client.download_blob().readall()
    content = blob_txt_content.decode('utf-8')
  return content

def main():
    chunk(get_blob_list())

if __name__ == "__main__":
    main()