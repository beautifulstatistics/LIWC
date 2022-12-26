import zipfile
import os
import requests
import dask

dask.config.set(scheduler='processes')

def download_raw():
    url = "https://datahub.hku.hk/ndownloader/articles/16674565/versions/1"
    r = requests.get(url, allow_redirects=True)
    with open("./data/raw_data.zip", 'wb') as fd:
        fd.write(r.content)
    
    print("Download Complete")

def extract_files():
    with zipfile.ZipFile('./data/raw_data.zip', 'r') as archive:
        for file in archive.namelist():
            if file.startswith('week'):
                archive.extract(file,'./data/weeks')
                print(file)

    for i in range(1,53):
        file = f'./data/weeks/week{i}.zip'
        with zipfile.ZipFile(file, 'r') as archive:
            archive.extractall('./data/weeks')

    print('CSV Week Files Extracted')

def clean_up():
    for root, _, files in os.walk("./data/", topdown=False):
        for name in files:
            if name.startswith('week') or name.startswith('user') or name.startswith('README') or name.startswith('random'):
                os.unlink(os.path.join(root, name))
                print(f'Deleted {name}')

def main():
    download_raw()
    extract_files()


if __name__ == '__main__':
    main()