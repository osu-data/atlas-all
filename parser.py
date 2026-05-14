import os
import hashlib
import zipfile
import shutil
import requests
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === CONFIG ===
CLIENT_ID = os.getenv("OSU_CLIENT_ID")
CLIENT_SECRET = os.getenv("OSU_CLIENT_SECRET")
GH_TOKEN = os.getenv("MY_GITHUB_TOKEN")
GH_USER = "osu-data"

MAX_THREADS = 5 
GLOBAL_LIMIT = 1000 

REPO_MAP = {0: "atlas-circles", 1: "atlas-taiko", 2: "atlas-catch", 3: "atlas-mania"}
ALL_DATA_REPO = "atlas-all"
PROGRESS_FILE = "processed_sets.txt"

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retries))

MIRRORS = ["https://api.nerinyan.moe/d/{}", "https://beatconnect.io/b/{}/?novideo=1", "https://kitsu.moe/d/{}"]
HEADERS = {"User-Agent": "OsuDataArchiver/1.0"}

data_buffer = {}
full_queue = {}
buffer_lock = threading.Lock()
processed_counter = 0

def git_setup():
    os.system('git config --global user.name "kiroffYT"')
    os.system('git config --global user.email "kirhit135@gmail.com"')

def clone_repo(repo_name):
    if not os.path.exists(repo_name):
        url = f"https://{GH_TOKEN}@github.com/{GH_USER}/{repo_name}.git"
        os.system(f"git clone {url} > /dev/null 2>&1")

def commit_and_push_all():
    print(f"\n[*] [{datetime.now().strftime('%H:%M:%S')}] Syncing to GitHub...")
    with buffer_lock:
        for repo_name, files in data_buffer.items():
            clone_repo(repo_name)
            for filename, new_entries in files.items():
                file_path = os.path.join(repo_name, filename)
                existing_hashes = set()
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = f.read().strip().rstrip(',')
                            if content:
                                current_list = json.loads(f"[{content}]")
                                existing_hashes = {x['beatmap_sha256'] for x in current_list}
                    except: pass
                unique_entries = [d for d in new_entries if d['beatmap_sha256'] not in existing_hashes]
                if unique_entries:
                    with open(file_path, 'a', encoding='utf-8') as f:
                        for entry in unique_entries:
                            f.write(json.dumps(entry, ensure_ascii=False) + ",\n")
            if os.path.exists(repo_name):
                cwd = os.getcwd()
                os.chdir(repo_name)
                os.system("git add .")
                if os.popen("git status --porcelain").read().strip():
                    os.system(f'git commit -m "Update: {datetime.now().strftime("%Y-%m-%d %H:%M")} [skip ci]"')
                    os.system("git pull --rebase -X ours origin main")
                    os.system("git push origin main")
                os.chdir(cwd)

def get_osu_token():
    data = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET, 'grant_type': 'client_credentials', 'scope': 'public'}
    try:
        r = session.post('https://osu.ppy.sh/oauth/token', data=data, timeout=10)
        return r.json().get('access_token')
    except: return None

def get_hashes(file_path):
    h_sha = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""): h_sha.update(chunk)
        return h_sha.hexdigest()
    except: return None

def fetch_api_worker(m, s, token):
    """Worker for API"""
    headers = {'Authorization': f'Bearer {token}'}
    local_count = 0
    for nsfw in [0, 1]:
        cursor = None
        page = 1
        while True:
            params = {'m': m, 's': s, 'nsfw': nsfw, 'cursor_string': cursor}
            try:
                r = session.get("https://osu.ppy.sh/api/v2/beatmapsets/search", params=params, headers=headers, timeout=15)
                if r.status_code != 200: break
                data = r.json()
                bsets = data.get('beatmapsets', [])
                if not bsets: break
                
                with buffer_lock:
                    for bset in bsets:
                        sid = str(bset['id'])
                        if sid not in full_queue:
                            full_queue[sid] = {'mode': m, 'file': f"m{m}_{s}.json", 'is_f': bset.get('is_featured_artist', False)}
                            local_count += 1
                
                cursor = data.get('cursor_string')
                if not cursor: break
                # Лог каждые 10 страниц, чтобы не спамить
                if page % 10 == 0:
                    print(f"    [Scanner] Mode {m} | {s} | NSFW:{nsfw} | Page {page}...", flush=True)
                page += 1
                time.sleep(0.05)
            except: break
    return local_count

def process_single_set(set_id, api_info):
    osz_name = f"temp_{set_id}.osz"
    extract_path = f"unpack_{set_id}"
    results = []
    downloaded = False
    for mirror in MIRRORS:
        try:
            r = session.get(mirror.format(set_id), headers=HEADERS, stream=True, timeout=(10, 45))
            if r.status_code == 200:
                with open(osz_name, 'wb') as f: shutil.copyfileobj(r.raw, f)
                if zipfile.is_zipfile(osz_name):
                    downloaded = True; break
        except: continue
    if not downloaded:
        if os.path.exists(osz_name): os.remove(osz_name)
        return None
    try:
        with zipfile.ZipFile(osz_name, 'r') as zip_ref: zip_ref.extractall(extract_path)
        res_hashes = {"bg": [], "audio": [], "video": []}
        osu_files = []
        for root, _, files in os.walk(extract_path):
            for file in files:
                fp = os.path.join(root, file); ext = os.path.splitext(file)[1].lower(); h = get_hashes(fp)
                if not h: continue
                if ext == '.mp4': res_hashes["video"].append(h)
                elif ext in ['.png', '.jpg', '.jpeg']: res_hashes["bg"].append(h)
                elif ext in ['.mp3', '.ogg']: res_hashes["audio"].append(h)
                elif ext == '.osu': osu_files.append(fp)
        for fp in osu_files:
            try:
                b_id = 0
                with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    if f'Mode: {api_info["mode"]}' in "".join(lines[:100]):
                        for line in lines:
                            if line.startswith("BeatmapID:"):
                                try: b_id = int(line.split(":")[1].strip()); break
                                except: pass
                        results.append({
                            "beatmapset_id": int(set_id), "beatmap_id": b_id,
                            "beatmap_sha256": get_hashes(fp), "is_featured": api_info['is_f'],
                            "resources": res_hashes, "checked_at": datetime.now().isoformat()
                        })
            except: continue
    finally:
        if os.path.exists(osz_name): os.remove(osz_name)
        if os.path.exists(extract_path): shutil.rmtree(extract_path, ignore_errors=True)
    return results

def thread_worker(sid, info, total_to_do):
    global processed_counter
    try:
        data = process_single_set(sid, info)
        if data:
            with buffer_lock:
                target_repos = [REPO_MAP[info['mode']], ALL_DATA_REPO]
                for r_name in target_repos:
                    if r_name not in data_buffer: data_buffer[r_name] = {}
                    if info['file'] not in data_buffer[r_name]: data_buffer[r_name][info['file']] = []
                    data_buffer[r_name][info['file']].extend(data)
                with open(PROGRESS_FILE, 'a') as f: f.write(f"{sid}\n")
                processed_counter += 1
                print(f"[{processed_counter}/{GLOBAL_LIMIT}] OK: {sid} (Left: {total_to_do - processed_counter})", flush=True)
    except Exception as e: print(f"[!] Error {sid}: {e}", flush=True)

def main():
    git_setup()
    token = get_osu_token()
    if not token: return
    
    print("[*] Starting Parallel Deep Scan...", flush=True)
    modes = [0, 1, 2, 3]
    statuses = ['ranked', 'approved', 'qualified', 'loved', 'pending', 'wip', 'graveyard']
    
    with ThreadPoolExecutor(max_workers=8) as search_executor:
        futures = [search_executor.submit(fetch_api_worker, m, s, token) for m in modes for s in statuses]
        for future in as_completed(futures): pass # Ждем завершения всех

    print(f"\n[*] Scan complete. Found: {len(full_queue)}")
    processed = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f: processed = {l.strip() for l in f}

    to_do = {k: v for k, v in full_queue.items() if k not in processed}
    print(f"[*] New for download: {len(to_do)}\n" + "-"*30)

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for sid, info in to_do.items():
            if processed_counter >= GLOBAL_LIMIT: break
            executor.submit(thread_worker, sid, info, len(to_do))
    
    if data_buffer: commit_and_push_all()
    print(f"[*] Finished. Saved {processed_counter} sets.")

if __name__ == "__main__":
    main()
