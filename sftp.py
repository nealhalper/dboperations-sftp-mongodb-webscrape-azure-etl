import os
import time
import requests
import paramiko
import fnmatch
import logging
from dotenv import load_dotenv
from config import CSV_FILES, BASE_URL
from datetime import datetime

load_dotenv()
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT'))
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASS = os.getenv('SFTP_PASS')
BASE_URL = os.getenv('BASE_URL')
DATE_STR = datetime.now().strftime('%Y-%m-%d')
BASE_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'sftp', DATE_STR)
os.makedirs(BASE_DATA_DIR, exist_ok=True)
LOG_FILE = os.path.join(BASE_DATA_DIR, 'transfer.log')
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def sftp_makedirs(sftp, remote_directory):
    dirs = []
    while remote_directory not in ('/', ''):
        dirs.append(remote_directory)
        remote_directory = os.path.dirname(remote_directory)
    for d in reversed(dirs):
        try:
            sftp.stat(d)
        except IOError:
            try:
                sftp.mkdir(d)
            except Exception:
                pass

def setup_sftp_connection(hostname, username, password, port=2222, max_retries=3, timeout=10):
    for attempt in range(1, max_retries + 1):
        try:
            transport = paramiko.Transport((hostname, port))
            transport.connect(username=username, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            return sftp, transport
        except Exception as e:
            print(f"SFTP connection attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(2)
    return None, None

def discover_files(sftp, pattern, remote_dir='.'):
    sftp_makedirs(sftp, remote_dir)
    try:
        sftp.chdir(remote_dir)
    except Exception as e:
        logging.error(f"FAILED: Could not change to remote_dir {remote_dir}: {e}")
        print(f"FAILED: Could not change to remote_dir {remote_dir}: {e}")
        return []
    files = [f for f in sftp.listdir() if fnmatch.fnmatch(f, pattern) and not f.endswith('.part')]
    print(f"Discovered files in '{remote_dir}' matching '{pattern}': {files}")
    return files

def download_and_organize(sftp, remote_files, local_dir, remote_dir='.'):
    successful, failed = [], []
    for remote_fname in remote_files:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name, ext = os.path.splitext(remote_fname)
        existing = [f for f in os.listdir(local_dir) if f.startswith(f"{name}_") and f.endswith(ext)]
        if existing:
            print(f"{remote_fname} already exists locally as {existing[0]}. Skipping download.")
            logging.info(f"SKIP: {remote_fname} already exists locally as {existing[0]}.")
            successful.append(existing[0])
            continue
        local_fname_ts = f"{name}_{timestamp}{ext}"
        local_path = os.path.join(local_dir, local_fname_ts)
        part_path = local_path + '.part'
        sftp.chdir(remote_dir)
        try:
            print(f"Downloading {remote_fname} from SFTP as {local_fname_ts} ...")
            sftp.get(remote_fname, part_path)
            os.rename(part_path, local_path)
            logging.info(f"SUCCESS: Downloaded {remote_fname} as {local_fname_ts} from SFTP.")
            successful.append(local_fname_ts)
        except Exception as e:
            logging.error(f"FAILED: Download error for {remote_fname}: {e}")
            if os.path.exists(part_path):
                os.remove(part_path)
            failed.append(remote_fname)
    return successful, failed

def log_transfer_results(successful, failed):
    print("\nTransfer Summary:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    logging.info(f"TRANSFER SUMMARY: Successful: {successful} | Failed: {failed}")


def download_csvs_from_baseurl():
    successful, failed = [], []
    for fname in CSV_FILES:
        url = f"{BASE_URL}/{fname}"
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name, ext = os.path.splitext(fname)
        existing = [f for f in os.listdir(BASE_DATA_DIR) if f.startswith(f"{name}_") and f.endswith(ext)]
        if existing:
            print(f"{fname} already exists locally as {existing[0]}. Skipping download.")
            logging.info(f"SKIP: {fname} already exists locally as {existing[0]}.")
            successful.append(existing[0])
            continue
        fname_ts = f"{name}_{timestamp}{ext}"
        local_path = os.path.join(BASE_DATA_DIR, fname_ts)
        part_path = local_path + '.part'
        try:
            print(f"Downloading {url} ...")
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            with open(part_path, 'wb') as f:
                f.write(resp.content)
            os.rename(part_path, local_path)
            logging.info(f"SUCCESS: Downloaded {fname} as {fname_ts} from BASE_URL.")
            successful.append(fname_ts)
        except Exception as e:
            logging.error(f"FAILED: Download error for {fname} from BASE_URL: {e}")
            if os.path.exists(part_path):
                os.remove(part_path)
            failed.append(fname)
    return successful, failed

def main():
    dl_success, dl_failed = download_csvs_from_baseurl()
    log_transfer_results(dl_success, dl_failed)

    sftp, transport = setup_sftp_connection(
        SFTP_HOST, SFTP_USER, SFTP_PASS, port=SFTP_PORT
    )
    try:
        remote_files = discover_files(sftp, '*.csv')
        successful, failed = download_and_organize(sftp, remote_files, BASE_DATA_DIR)
        log_transfer_results(successful, failed)
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

if __name__ == "__main__":
    main()
