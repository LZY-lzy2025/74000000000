import os
import subprocess
import base64
import urllib.parse
import json
from flask import Flask, jsonify, Response

app = Flask(__name__)
OUTPUT_FILE = 'output/extracted_ids.txt'
STATUS_FILE = 'output/scrape_status.json'
SCRAPER_SCRIPT = 'scraper.py'


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


def generate_playlist(fmt="m3u", mode="clean"):
    if not os.path.exists(OUTPUT_FILE):
        return "请稍后再试，爬虫尚未生成数据"

    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if fmt == "m3u":
        content = "#EXTM3U\n"
    else:
        content = "74体育,#genre#\n"

    index = 1
    for line in lines:
        try:
            if line.startswith('{'):
                item = json.loads(line)
                channel_name = f"{item['time']} {item['home']}VS{item['away']}".replace(" ", "")
                group_title = "74体育"
                stream_url = item.get("stream_url")
            else:
                channel_name = f"74体育 {index}"
                group_title = "74体育"
                stream_url = decode_stream_from_id(line)

            if stream_url:
                if mode == "plus":
                    stream_url = f"{stream_url}|Referer="

                if fmt == "m3u":
                    content += f'#EXTINF:-1 group-title="{group_title}",{channel_name}\n{stream_url}\n'
                else:
                    content += f'{channel_name},{stream_url}\n'
                index += 1
        except Exception:
            continue

    return content


def read_status():
    if not os.path.exists(STATUS_FILE):
        return {"status": "idle", "message": "尚未触发抓取任务"}
    with open(STATUS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


@app.route('/')
def index():
    return jsonify({
        "status": "running",
        "scrape_status": read_status(),
        "endpoints": ["/trigger", "/ids", "/m3u", "/m3u_plus", "/txt", "/txt_plus"]
    })


@app.route('/trigger')
def trigger_scrape():
    python_bin = os.environ.get("PYTHON_BIN", "python")
    subprocess.Popen(
        [python_bin, SCRAPER_SCRIPT],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    return jsonify({
        "ok": True,
        "message": "scraper.py 已启动，请稍后查看 / 或 /m3u",
        "trigger_mode": "cloudflare-worker-cron"
    })


@app.route('/ids')
def get_ids():
    if not os.path.exists(OUTPUT_FILE):
        return Response("请稍后再试，爬虫尚未生成数据", mimetype='text/plain; charset=utf-8')
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        return Response(f.read(), mimetype='text/plain; charset=utf-8', headers={"Access-Control-Allow-Origin": "*"})


@app.route('/m3u')
def get_m3u_clean():
    return Response(generate_playlist("m3u", "clean"), mimetype='text/plain; charset=utf-8', headers={"Access-Control-Allow-Origin": "*"})


@app.route('/m3u_plus')
def get_m3u_plus():
    return Response(generate_playlist("m3u", "plus"), mimetype='text/plain; charset=utf-8', headers={"Access-Control-Allow-Origin": "*"})


@app.route('/txt')
def get_txt_clean():
    return Response(generate_playlist("txt", "clean"), mimetype='text/plain; charset=utf-8', headers={"Access-Control-Allow-Origin": "*"})


@app.route('/txt_plus')
def get_txt_plus():
    return Response(generate_playlist("txt", "plus"), mimetype='text/plain; charset=utf-8', headers={"Access-Control-Allow-Origin": "*"})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, use_reloader=False)
