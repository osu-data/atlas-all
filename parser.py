import os
import hashlib
import zipfile
import shutil
import requests
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CLIENT_ID = os.getenv("OSU_CLIENT_ID")
CLIENT_SECRET = os.getenv("OSU_CLIENT_SECRET")
GH_TOKEN = os.getenv("MY_GITHUB_TOKEN")
GH_USER = "osu-data"

MAX_THREADS = 10
GLOBAL_LIMIT = 500

REPO_MAP = {
    0: "atlas-circles",
    1: "atlas-taiko",
    2: "atlas-catch",
    3: "atlas-mania"
}
ALL_DATA_REPO = "atlas-all"

PROGRESS_FILE = "processed_sets.txt"

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(pool_connections=MAX_THREADS, pool_maxsize=MAX_THREADS, max_retries=retries))

MIRRORS = ["https://api.nerinyan.moe/d/{}", "https://beatconnect.io/b/{}/?novideo=1"]
HEADERS = {"User-Agent": "OsuDataArchiver/1.0"}

data_buffer = {}
buffer_lock = threading.Lock()
processed_counter = 0

def git_setup():
    os.system('git config --global user.name "kiroffYT"')
    os.system('git config --global user.email "kirhit135@gmail.com"')

def clone_repo(repo_name):
    if not os.path.exists(repo_name):
        url = f"https://{GH_TOKEN}@github.com/{GH_USER}/{repo_name}.git"
        os.system(f"git clone {url}")

def commit_and_push_all():
    """Github push, in final"""
    print("\n[*] Adding changes into repositories...")
    
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
                    except Exception as e:
                        print(f"[!] Ошибка чтения {filename}: {e}")

                unique_entries = [d for d in new_entries if d['beatmap_sha256'] not in existing_hashes]
                
                if unique_entries:
                    with open(file_path, 'a', encoding='utf-8') as f:
                        for entry in unique_entries:
                            f.write(json.dumps(entry, ensure_ascii=False) + ",\n")

            cwd = os.getcwd()
            if os.path.exists(repo_name):
                os.chdir(repo_name)
                os.system("git add .")
                os.system(f'git commit -m "Update osu! data: {datetime.now().strftime("%Y-%m-%d %H:%M")}"')
                os.system("git push")
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
            for chunk in iter(lambda: f.read(1048576), b""):
                h_sha.update(chunk)
        return h_sha.hexdigest()
    except: return None

def process_single_set(set_id, api_info):
    osz_name = f"temp_{set_id}.osz"
    extract_path = f"unpack_{set_id}"
    results = []
    
    downloaded = False
    for mirror in MIRRORS:
        try:
            r = session.get(mirror.format(set_id), headers=HEADERS, stream=True, timeout=30)
            if r.status_code == 200:
                with open(osz_name, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                if zipfile.is_zipfile(osz_name):
                    downloaded = True
                    break
        except: continue

    if not downloaded:
        if os.path.exists(osz_name): os.remove(osz_name)
        return None

    try:
        with zipfile.ZipFile(osz_name, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        res_hashes = {"bg": [], "audio": [], "video": []}
        osu_files = []

        for root, _, files in os.walk(extract_path):
            for file in files:
                fp = os.path.join(root, file)
                ext = os.path.splitext(file)[1].lower()
                h = get_hashes(fp)
                if not h: continue
                
                if ext == '.mp4': res_hashes["video"].append(h)
                elif ext in ['.png', '.jpg', '.jpeg']: res_hashes["bg"].append(h)
                elif ext in ['.mp3', '.ogg']: res_hashes["audio"].append(h)
                elif ext == '.osu': osu_files.append(fp)

        for fp in osu_files:
            try:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(5000)
                    if f'Mode: {api_info["mode"]}' in content:
                        sha_map = get_hashes(fp)
                        results.append({
                            "beatmapset_id": int(set_id),
                            "beatmap_sha256": sha_map,
                            "is_featured": api_info['is_f'],
                            "resources": res_hashes,
                            "checked_at": datetime.now().isoformat()
                        })
            except: continue
    finally:
        if os.path.exists(osz_name): os.remove(osz_name)
        if os.path.exists(extract_path): shutil.rmtree(extract_path, ignore_errors=True)

    return results

def add_to_buffer(data, filename, mode):
    with buffer_lock:
        target_repos = [REPO_MAP[mode], ALL_DATA_REPO]
        for r_name in target_repos:
            if r_name not in data_buffer:
                data_buffer[r_name] = {}
            if filename not in data_buffer[r_name]:
                data_buffer[r_name][filename] = []
            data_buffer[r_name][filename].extend(data)

def thread_worker(sid, info):
    global processed_counter

    if processed_counter >= GLOBAL_LIMIT:
        return

    try:
        data = process_single_set(sid, info)
        if data:
            with buffer_lock:
                if processed_counter >= GLOBAL_LIMIT:
                    return
                
                add_to_buffer(data, info['file'], info['mode'])
                with open(PROGRESS_FILE, 'a') as f: f.write(f"{sid}\n")
                processed_counter += 1
                print(f"[OK] {sid} | Collected: {processed_counter}/{GLOBAL_LIMIT}")
    except Exception as e:
        print(f"[ERR] {sid}: {e}")

def main():
    git_setup()
    token = get_osu_token()
    if not token: 
        print("[!] Failed to get osu! token.")
        return

    modes = [0, 1, 2, 3]
    statuses = ['ranked', 'approved', 'qualified', 'loved', 'pending', 'wip', 'graveyard']
    full_queue = {}

    print("[*] osu!scan is starting...")
    for m in modes:
        for s in statuses:
            for nsfw in [0, 1]:
                for extra in [[], ['video'], ['storyboard'], ['video', 'storyboard']]:
                    extra_tag = 'v_s' if len(extra)==2 else (extra[0] if extra else 'none')
                    fname = f"m{m}_{s}_nsfw{nsfw}_{extra_tag}.json"
                    
                    params = {'m': m, 's': s, 'nsfw': nsfw}
                    for e in extra: params['extra[]'] = e
                    
                    try:
                        r = session.get("https://osu.ppy.sh/api/v2/beatmapsets/search", 
                                        params=params, headers={'Authorization': f'Bearer {token}'}, timeout=10)
                        for bset in r.json().get('beatmapsets', []):
                            sid = str(bset['id'])
                            if sid not in full_queue:
                                full_queue[sid] = {'mode': m, 'file': fname, 'is_f': bset.get('is_featured_artist', False)}
                    except: continue

    processed = set()
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f: processed = {l.strip() for l in f}

    to_do = {k: v for k, v in full_queue.items() if k not in processed}
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for sid, info in to_do.items():
            if processed_counter >= GLOBAL_LIMIT:
                break
            executor.submit(thread_worker, sid, info)

    if data_buffer:
        commit_and_push_all()
    else:
        print("[!] No new data to commit.")

if __name__ == "__main__":
    main()
