import os
import requests
from bs4 import BeautifulSoup
import base64
import re
import urllib.parse
import json
from datetime import datetime, timedelta
import pytz
from playwright.sync_api import sync_playwright
import gc
from contextlib import suppress
from requests.adapters import HTTPAdapter

OUTPUT_FILE = 'output/extracted_ids.txt'
ROUTE_STATE_FILE = 'output/decoded_routes.jsonl'
STATUS_FILE = 'output/scrape_status.json'
LOCK_FILE = 'output/scrape_job.lock'
REFRESH_INTERVAL_RUNS = 2


def str2long(s, w):
    v = []
    for i in range(0, len(s), 4):
        v0 = s[i]
        v1 = s[i + 1] if i + 1 < len(s) else 0
        v2 = s[i + 2] if i + 2 < len(s) else 0
        v3 = s[i + 3] if i + 3 < len(s) else 0
        v.append(v0 | (v1 << 8) | (v2 << 16) | (v3 << 24))
    if w:
        v.append(len(s))
    return v


def long2str(v, w):
    vl = len(v)
    if vl == 0:
        return b""
    n = (vl - 1) << 2
    if w:
        m = v[-1]
        if (m < n - 3) or (m > n):
            return None
        n = m
    s = bytearray()
    for i in range(vl):
        s.append(v[i] & 0xff)
        s.append((v[i] >> 8) & 0xff)
        s.append((v[i] >> 16) & 0xff)
        s.append((v[i] >> 24) & 0xff)
    return bytes(s[:n]) if w else bytes(s)


def xxtea_decrypt(data, key):
    if not data:
        return b""
    v = str2long(data, False)
    k = str2long(key, False)
    if len(k) < 4:
        k.extend([0] * (4 - len(k)))
    n = len(v) - 1
    if n < 1:
        return b""

    z = v[n]
    y = v[0]
    delta = 0x9E3779B9
    q = 6 + 52 // (n + 1)
    sum_val = (q * delta) & 0xffffffff

    while sum_val != 0:
        e = (sum_val >> 2) & 3
        for p in range(n, 0, -1):
            z = v[p - 1]
            mx = (((z >> 5) ^ (y << 2)) + ((y >> 3) ^ (z << 4))) ^ ((sum_val ^ y) + (k[(p & 3) ^ e] ^ z))
            y = v[p] = (v[p] - mx) & 0xffffffff
        z = v[n]
        mx = (((z >> 5) ^ (y << 2)) + ((y >> 3) ^ (z << 4))) ^ ((sum_val ^ y) + (k[(0 & 3) ^ e] ^ z))
        y = v[0] = (v[0] - mx) & 0xffffffff
        sum_val = (sum_val - delta) & 0xffffffff

    return long2str(v, True)


def decode_stream_from_id(raw_id):
    target_key = b"ABCDEFGHIJKLMNOPQRSTUVWX"
    try:
        decoded_id = urllib.parse.unquote(raw_id)
        pad = 4 - (len(decoded_id) % 4)
        if pad != 4:
            decoded_id += "=" * pad
        bin_data = base64.b64decode(decoded_id)
        decrypted_bytes = xxtea_decrypt(bin_data, target_key)
        if not decrypted_bytes:
            return None
        json_str = decrypted_bytes.decode('utf-8', errors='ignore')
        data = json.loads(json_str)
        return data.get("url")
    except Exception:
        return None


def get_keep_window(now):
    keep_start = now - timedelta(hours=7)
    keep_end = now + timedelta(hours=7)
    return keep_start, keep_end


def load_existing_records(now, tz):
    keep_start, keep_end = get_keep_window(now)
    records = []
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith('{'):
                    continue
                try:
                    item = json.loads(line)
                    item_match_time = item.get("match_time")
                    if not item_match_time:
                        continue
                    match_time = datetime.strptime(item_match_time, "%Y-%m-%d %H:%M:%S")
                    match_time = tz.localize(match_time)
                    if keep_start <= match_time <= keep_end and item.get("source_url") and item.get("stream_url"):
                        records.append(item)
                except Exception:
                    continue
    return records


def load_route_states(now, tz):
    keep_start, keep_end = get_keep_window(now)
    states = {}
    if os.path.exists(ROUTE_STATE_FILE):
        with open(ROUTE_STATE_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                    source_url = item.get("source_url")
                    match_time_str = item.get("match_time")
                    if not source_url or not match_time_str:
                        continue
                    match_time = tz.localize(datetime.strptime(match_time_str, "%Y-%m-%d %H:%M:%S"))
                    if keep_start <= match_time <= keep_end:
                        states[source_url] = item
                except Exception:
                    continue
    return states


def save_route_states(states):
    os.makedirs('output', exist_ok=True)
    with open(ROUTE_STATE_FILE, 'w', encoding='utf-8') as f:
        for item in states.values():
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


def append_route_event(route_state, stage, message, extra=None):
    events = route_state.setdefault("events", [])
    payload = {
        "time": route_state.get("last_checked_at"),
        "stage": stage,
        "message": message
    }
    if extra:
        payload["extra"] = extra
    events.append(payload)


def should_cleanup_events(route_state):
    cleanup_counter = route_state.get("event_cleanup_counter", 0) + 1
    if cleanup_counter >= 6:
        route_state["events"] = []
        route_state["event_cleanup_counter"] = 0
        return True
    route_state["event_cleanup_counter"] = cleanup_counter
    return False


def should_schedule_refresh(route_state):
    if not route_state.get("resolved") or not route_state.get("stream_url"):
        route_state["refresh_counter"] = 0
        return False

    refresh_counter = route_state.get("refresh_counter", 0) + 1
    if refresh_counter >= REFRESH_INTERVAL_RUNS:
        route_state["refresh_counter"] = 0
        return True

    route_state["refresh_counter"] = refresh_counter
    return False


def write_status(status, message="", extra=None):
    os.makedirs('output', exist_ok=True)
    payload = {
        "status": status,
        "message": message,
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
    if extra:
        payload.update(extra)
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False)


def scrape_job():
    tz = pytz.timezone('Asia/Shanghai')
    now = datetime.now(tz)
    last_run_time = now.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{last_run_time}] 开始执行抓取任务...")
    write_status("running", "scrape_job started", {"last_run_time": last_run_time})

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    try:
        with requests.Session() as session:
            session.mount('http://', HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0))
            session.mount('https://', HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=0))
            res = session.get('https://www.74001.tv', headers=headers, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, 'html.parser')

            match_infos = {}
            lower_bound = now - timedelta(hours=4)
            upper_bound = now + timedelta(hours=1)

            for a in soup.select('a.clearfix'):
                href = a.get('href')
                time_str = a.get('t-nzf-o')
                if href and '/bofang/' in href and time_str:
                    try:
                        if len(time_str) == 10:
                            time_str += " 00:00:00"
                        match_time = tz.localize(datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S'))

                        if lower_bound <= match_time <= upper_bound:
                            match_id = href.split('/')[-1]

                            em_tag = a.select_one('.eventtime em')
                            league = em_tag.text.strip() if em_tag else "未知联赛"

                            zhudui_tag = a.select_one('.zhudui p')
                            home = zhudui_tag.text.strip() if zhudui_tag else "未知主队"

                            kedui_tag = a.select_one('.kedui p')
                            away = kedui_tag.text.strip() if kedui_tag else "未知客队"

                            time_i_tag = a.select_one('.eventtime i')
                            display_time = time_i_tag.text.strip() if time_i_tag else match_time.strftime('%H:%M')

                            match_infos[match_id] = {
                                'match_time': match_time.strftime('%Y-%m-%d %H:%M:%S'),
                                'time': display_time,
                                'league': league,
                                'home': home,
                                'away': away
                            }
                    except Exception:
                        continue

            play_url_to_info = {}
            for match_id, info in match_infos.items():
                link = f"https://www.74001.tv/live/{match_id}"
                try:
                    res = session.get(link, headers=headers, timeout=10)
                    res.raise_for_status()
                    soup = BeautifulSoup(res.text, 'html.parser')
                    for dd in soup.select('dd[nz-g-c]'):
                        if dd.get('nz-g-ca'):
                            continue

                        b64_str = dd.get('nz-g-c')
                        if b64_str:
                            b64_str += "=" * ((4 - len(b64_str) % 4) % 4)
                            decoded = base64.b64decode(b64_str).decode('utf-8', errors='ignore')
                            m = re.search(r'ftp:\*\*(.*?)(?:::|$)', decoded)
                            if m:
                                raw_url = m.group(1)
                                url = 'http://' + raw_url.replace('!', '.').replace('&nbsp', 'com').replace('*', '/')
                                play_url_to_info[url] = info
                except Exception:
                    continue
    except Exception as e:
        write_status("failed", f"获取主页失败: {e}", {"last_run_time": last_run_time})
        print(f"获取主页失败: {e}")
        return

    existing_records = load_existing_records(now, tz)
    route_states = load_route_states(now, tz)
    for state in route_states.values():
        should_cleanup_events(state)

    for url, info in play_url_to_info.items():
        old = route_states.get(url, {})
        route_states[url] = {
            "source_url": url,
            "match_time": info["match_time"],
            "time": info["time"],
            "league": info["league"],
            "home": info["home"],
            "away": info["away"],
            "resolved": old.get("resolved", False),
            "id": old.get("id"),
            "stream_url": old.get("stream_url"),
            "refresh_counter": old.get("refresh_counter", 0),
            "last_stage": old.get("last_stage", "initialized"),
            "last_error": old.get("last_error"),
            "last_checked_at": old.get("last_checked_at"),
            "attempt_count": old.get("attempt_count", 0),
            "last_request_count": old.get("last_request_count", 0),
            "last_seen_paps_url": old.get("last_seen_paps_url"),
            "event_cleanup_counter": old.get("event_cleanup_counter", 0),
            "events": old.get("events", [])
        }
        should_cleanup_events(route_states[url])
        append_route_event(route_states[url], "initialized", "线路进入本轮调度队列")

    refresh_candidates = set()
    for source_url, state in route_states.items():
        if should_schedule_refresh(state):
            refresh_candidates.add(source_url)

    success_by_source_url = {
        source_url for source_url, state in route_states.items()
        if state.get("resolved") and state.get("stream_url")
    } - refresh_candidates

    final_data = []
    for source_url in success_by_source_url:
        state = route_states[source_url]
        if state.get("id") and state.get("stream_url"):
            final_data.append({
                'id': state["id"],
                'source_url': source_url,
                'stream_url': state["stream_url"],
                'match_time': state["match_time"],
                'time': state["time"],
                'league': state["league"],
                'home': state["home"],
                'away': state["away"]
            })
    final_data_index = {item["source_url"]: idx for idx, item in enumerate(final_data)}

    for item in existing_records:
        if item["source_url"] not in success_by_source_url:
            final_data_index[item["source_url"]] = len(final_data)
            final_data.append(item)
            success_by_source_url.add(item["source_url"])

    seen_ids = set(item.get("id") for item in final_data if item.get("id"))
    seen_source_urls = set(success_by_source_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--js-flags="--max-old-space-size=512"',
                '--disable-background-networking',
                '--disable-background-timer-throttling'
            ]
        )

        try:
            for url, info in play_url_to_info.items():
                if url in success_by_source_url:
                    route_states[url]["last_stage"] = "cached_success"
                    route_states[url]["last_error"] = None
                    route_states[url]["last_checked_at"] = last_run_time
                    append_route_event(route_states[url], "cached_success", "命中缓存成功线路，本轮跳过Playwright抓取")
                    continue

                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
                    viewport={'width': 1280, 'height': 720},
                    extra_http_headers={
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Referer': 'https://www.74001.tv/'
                    }
                )
                page = context.new_page()
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                request_count = 0
                requests_list = []

                def handle_request(request):
                    nonlocal request_count
                    request_count += 1
                    requests_list.append(request.url)
                    # 定时清理请求缓存，避免长页面导致列表持续膨胀
                    if len(requests_list) >= 400:
                        del requests_list[:-80]

                try:
                    route_states[url]["attempt_count"] = route_states[url].get("attempt_count", 0) + 1
                    route_states[url]["last_stage"] = "page_loading"
                    route_states[url]["last_error"] = None
                    route_states[url]["last_checked_at"] = last_run_time
                    append_route_event(route_states[url], "page_loading", "开始使用Playwright加载线路页面")

                    page.on("request", handle_request)
                    page.goto(url, wait_until='domcontentloaded', timeout=20000)

                    try:
                        with page.expect_request(lambda req: "paps.html?id=" in req.url, timeout=10000):
                            pass
                    except Exception:
                        page.wait_for_timeout(2000)

                    route_states[url]["last_request_count"] = request_count
                    route_states[url]["last_stage"] = "searching_paps_id"
                    append_route_event(route_states[url], "searching_paps_id", "页面加载完成，开始扫描请求列表中的播放器ID", {"request_count": request_count})

                    matched_paps_url = next((req for req in requests_list if "paps.html?id=" in req), None)
                    if matched_paps_url:
                        route_states[url]["last_seen_paps_url"] = matched_paps_url
                        append_route_event(route_states[url], "paps_found", "检测到播放器ID请求", {"paps_url": matched_paps_url})
                        extracted_id = matched_paps_url.split('paps.html?id=')[-1]
                        can_replace = url in refresh_candidates
                        if (can_replace or extracted_id not in seen_ids) and (can_replace or url not in seen_source_urls):
                            stream_url = decode_stream_from_id(extracted_id)
                            if stream_url:
                                route_states[url]["resolved"] = True
                                route_states[url]["id"] = extracted_id
                                route_states[url]["stream_url"] = stream_url
                                route_states[url]["refresh_counter"] = 0
                                route_states[url]["last_stage"] = "resolved"
                                route_states[url]["last_error"] = None
                                append_route_event(route_states[url], "resolved", "线路解密成功，已得到stream_url", {"stream_url": stream_url})
                                new_item = {
                                    'id': extracted_id,
                                    'source_url': url,
                                    'stream_url': stream_url,
                                    'match_time': info['match_time'],
                                    'time': info['time'],
                                    'league': info['league'],
                                    'home': info['home'],
                                    'away': info['away']
                                }
                                if can_replace and url in final_data_index:
                                    final_data[final_data_index[url]] = new_item
                                else:
                                    final_data_index[url] = len(final_data)
                                    final_data.append(new_item)
                                seen_ids.add(extracted_id)
                                seen_source_urls.add(url)
                            else:
                                route_states[url]["last_stage"] = "decode_failed"
                                route_states[url]["last_error"] = "提取到ID，但解密后未得到stream_url"
                                append_route_event(route_states[url], "decode_failed", "提取到ID但解密失败，未获得可用stream_url")
                        else:
                            route_states[url]["last_stage"] = "skipped_duplicate"
                            route_states[url]["last_error"] = "提取到的ID或线路已存在，跳过覆盖"
                            append_route_event(route_states[url], "skipped_duplicate", "线路或ID已存在，按去重规则跳过覆盖", {"can_replace": can_replace})
                    else:
                        route_states[url]["last_stage"] = "missing_paps_id"
                        route_states[url]["last_error"] = "页面请求中未发现 paps.html?id=..."
                        append_route_event(
                            route_states[url],
                            "missing_paps_id",
                            "未在请求列表中发现播放器ID请求，可能是线路失效/前端策略变更",
                            {"request_count": request_count, "sample_requests": requests_list[:5]}
                        )
                except Exception as e:
                    route_states[url]["last_stage"] = "page_error"
                    route_states[url]["last_error"] = str(e)
                    route_states[url]["last_checked_at"] = last_run_time
                    append_route_event(route_states[url], "page_error", "页面处理异常", {"error": str(e)})
                finally:
                    with suppress(Exception):
                        page.remove_listener("request", handle_request)
                    requests_list.clear()
                    with suppress(Exception):
                        page.close()
                    with suppress(Exception):
                        context.close()
        finally:
            browser.close()

    os.makedirs('output', exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for item in final_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    save_route_states(route_states)

    print(f"任务完成，共保存 {len(final_data)} 个记录。")
    write_status("success", f"任务完成，共保存 {len(final_data)} 个记录。", {"last_run_time": last_run_time, "record_count": len(final_data)})
    gc.collect()


def main():
    os.makedirs('output', exist_ok=True)
    lock_fd = None
    try:
        lock_fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode())
    except FileExistsError:
        print("已有抓取任务在执行，跳过本次触发。")
        write_status("skipped", "已有抓取任务在执行", {"pid": os.getpid()})
        return

    try:
        scrape_job()
    except Exception as e:
        write_status("failed", str(e), {"pid": os.getpid()})
        raise
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        with suppress(FileNotFoundError):
            os.remove(LOCK_FILE)


if __name__ == "__main__":
    main()
