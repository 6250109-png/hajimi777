import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional

import requests

from common.Logger import logger
from common.config import Config
from utils.file_manager import file_manager, checkpoint


class SyncUtils:
    """同步工具类，负责异步发送keys到外部应用"""

    def __init__(self):
        """初始化同步工具"""
        # --- 修改点：将 GEMINI 改为 GROK ---
        self.balancer_url = Config.GROK_BALANCER_URL.rstrip('/') if Config.GROK_BALANCER_URL else ""
        self.balancer_auth = Config.GROK_BALANCER_AUTH
        self.balancer_sync_enabled = Config.parse_bool(Config.GROK_BALANCER_SYNC_ENABLED)
        self.balancer_enabled = bool(self.balancer_url and self.balancer_auth and self.balancer_sync_enabled)

        # GPT Load Balancer 配置
        self.gpt_load_url = Config.GPT_LOAD_URL.rstrip('/') if Config.GPT_LOAD_URL else ""
        self.gpt_load_auth = Config.GPT_LOAD_AUTH
        # 解析多个group names (逗号分隔)
        self.gpt_load_group_names = [name.strip() for name in Config.GPT_LOAD_GROUP_NAME.split(',') if name.strip()] if Config.GPT_LOAD_GROUP_NAME else []
        self.gpt_load_sync_enabled = Config.parse_bool(Config.GPT_LOAD_SYNC_ENABLED)
        self.gpt_load_enabled = bool(self.gpt_load_url and self.gpt_load_auth and self.gpt_load_group_names and self.gpt_load_sync_enabled)

        # 创建线程池用于异步执行
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="SyncUtils")
        self.saving_checkpoint = False

        # 周期性发送控制
        self.batch_interval = 60
        self.batch_timer = None
        self.shutdown_flag = False

        # GPT Load Balancer group ID 缓存 (15分钟缓存)
        self.group_id_cache: Dict[str, int] = {}
        self.group_id_cache_time: Dict[str, float] = {}
        self.group_id_cache_ttl = 15 * 60  # 15分钟

        if not self.balancer_enabled:
            logger.warning("🚫 Grok Balancer sync disabled - URL or AUTH not configured")
        else:
            logger.info(f"🔗 Grok Balancer enabled - URL: {self.balancer_url}")

        if not self.gpt_load_enabled:
            logger.warning("🚫 GPT Load Balancer sync disabled - URL, AUTH, GROUP_NAME not configured or sync disabled")
        else:
            logger.info(f"🔗 GPT Load Balancer enabled - URL: {self.gpt_load_url}, Groups: {', '.join(self.gpt_load_group_names)}")

        # 启动周期性发送线程
        self._start_batch_sender()

    def add_keys_to_queue(self, keys: List[str]):
        """
        将keys同时添加到balancer和GPT load的发送队列
        """
        if not keys:
            return

        # Acquire lock for checkpoint saving
        while self.saving_checkpoint:
            logger.info(f"📥 Checkpoint is currently being saved, waiting before adding {len(keys)} key(s) to queues...")
            time.sleep(0.5)

        self.saving_checkpoint = True
        try:
            # Grok Balancer (原 Gemini Balancer)
            if self.balancer_enabled:
                initial_balancer_count = len(checkpoint.wait_send_balancer)
                checkpoint.wait_send_balancer.update(keys)
                new_balancer_count = len(checkpoint.wait_send_balancer)
                added_balancer_count = new_balancer_count - initial_balancer_count
                logger.info(f"📥 Added {added_balancer_count} key(s) to grok balancer queue (total: {new_balancer_count})")
            else:
                logger.info(f"🚫 Grok Balancer disabled, skipping {len(keys)} key(s) for grok balancer queue")

            # GPT Load Balancer
            if self.gpt_load_enabled:
                initial_gpt_count = len(checkpoint.wait_send_gpt_load)
                checkpoint.wait_send_gpt_load.update(keys)
                new_gpt_count = len(checkpoint.wait_send_gpt_load)
                added_gpt_count = new_gpt_count - initial_gpt_count
                logger.info(f"📥 Added {added_gpt_count} key(s) to GPT load balancer queue (total: {new_gpt_count})")
            else:
                logger.info(f"🚫 GPT Load Balancer disabled, skipping {len(keys)} key(s) for GPT load balancer queue")

            file_manager.save_checkpoint(checkpoint)
        finally:
            self.saving_checkpoint = False

    def _send_balancer_worker(self, keys: List[str]) -> str:
        """实际执行发送到balancer的工作函数"""
        try:
            logger.info(f"🔄 Sending {len(keys)} key(s) to grok balancer...")

            config_url = f"{self.balancer_url}/api/config"
            headers = {
                'Cookie': f'auth_token={self.balancer_auth}',
                'User-Agent': 'HajimiKing/1.0'
            }

            response = requests.get(config_url, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to get config: HTTP {response.status_code} - {response.text}")
                return "get_config_failed_not_200"

            config_data = response.json()
            current_api_keys = config_data.get('API_KEYS', [])

            existing_keys_set = set(current_api_keys)
            new_add_keys_set = set()
            for key in keys:
                if key not in existing_keys_set:
                    existing_keys_set.add(key)
                    new_add_keys_set.add(key)

            if len(new_add_keys_set) == 0:
                logger.info(f"ℹ️ All {len(keys)} key(s) already exist in grok balancer")
                return "ok"

            config_data['API_KEYS'] = list(existing_keys_set)
            logger.info(f"📝 Updating grok balancer config with {len(new_add_keys_set)} new key(s)...")

            update_headers = headers.copy()
            update_headers['Content-Type'] = 'application/json'

            update_response = requests.put(
                config_url,
                headers=update_headers,
                json=config_data,
                timeout=60
            )

            if update_response.status_code != 200:
                logger.error(f"Failed to update config: HTTP {update_response.status_code} - {update_response.text}")
                return "update_config_failed_not_200"

            # 验证添加结果
            updated_config = update_response.json()
            updated_api_keys = updated_config.get('API_KEYS', [])
            updated_keys_set = set(updated_api_keys)
            failed_to_add = [key for key in new_add_keys_set if key not in updated_keys_set]

            if failed_to_add:
                logger.error(f"❌ Failed to add {len(failed_to_add)} key(s)")
                send_result = {key: ("update_failed" if key in failed_to_add else "ok") for key in new_add_keys_set}
                file_manager.save_keys_send_result(list(new_add_keys_set), send_result)
                return "update_failed"

            logger.info(f"✅ All {len(new_add_keys_set)} new key(s) successfully added to grok balancer.")
            send_result = {key: "ok" for key in new_add_keys_set}
            file_manager.save_keys_send_result(list(new_add_keys_set), send_result)
            return "ok"

        except Exception as e:
            logger.error(f"❌ Failed to send keys to grok balancer: {str(e)}")
            return "exception"

    def _get_gpt_load_group_id(self, group_name: str) -> Optional[int]:
        """获取GPT Load Balancer group ID，带缓存功能"""
        current_time = time.time()
        if (group_name in self.group_id_cache and
            group_name in self.group_id_cache_time and
            current_time - self.group_id_cache_time[group_name] < self.group_id_cache_ttl):
            return self.group_id_cache[group_name]
        
        try:
            groups_url = f"{self.gpt_load_url}/api/groups"
            headers = {'Authorization': f'Bearer {self.gpt_load_auth}', 'User-Agent': 'HajimiKing/1.0'}
            response = requests.get(groups_url, headers=headers, timeout=30)
            if response.status_code != 200: return None

            groups_data = response.json()
            if groups_data.get('code') != 0: return None

            groups_list = groups_data.get('data', [])
            for group in groups_list:
                if group.get('name') == group_name:
                    group_id = group.get('id')
                    self.group_id_cache[group_name] = group_id
                    self.group_id_cache_time[group_name] = current_time
                    return group_id
            return None
        except Exception:
            return None

    def _send_gpt_load_worker(self, keys: List[str]) -> str:
        """发送到GPT load balancer的工作函数"""
        try:
            all_success = True
            failed_groups = []
            for group_name in self.gpt_load_group_names:
                group_id = self._get_gpt_load_group_id(group_name)
                if group_id is None:
                    failed_groups.append(group_name)
                    all_success = False
                    continue

                add_keys_url = f"{self.gpt_load_url}/api/keys/add-async"
                payload = {"group_id": group_id, "keys_text": ",".join(keys)}
                add_headers = {
                    'Authorization': f'Bearer {self.gpt_load_auth}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'HajimiKing/1.0'
                }
                add_response = requests.post(add_keys_url, headers=add_headers, json=payload, timeout=60)

                if add_response.status_code != 200 or add_response.json().get('code') != 0:
                    failed_groups.append(group_name)
                    all_success = False

            if all_success:
                send_result = {key: "ok" for key in keys}
                file_manager.save_keys_send_result(keys, send_result)
                return "ok"
            else:
                send_result = {key: f"partial_failure_{len(failed_groups)}_groups" for key in keys}
                file_manager.save_keys_send_result(keys, send_result)
                return "partial_failure"
        except Exception:
            return "exception"

    def _start_batch_sender(self) -> None:
        if self.shutdown_flag: return
        self.executor.submit(self._batch_send_worker)
        self.batch_timer = threading.Timer(self.batch_interval, self._start_batch_sender)
        self.batch_timer.daemon = True
        self.batch_timer.start()

    def _batch_send_worker(self) -> None:
        while self.saving_checkpoint:
            time.sleep(0.5)

        self.saving_checkpoint = True
        try:
            # 修改点：同步名称改为 grok balancer 相关日志
            if checkpoint.wait_send_balancer and self.balancer_enabled:
                balancer_keys = list(checkpoint.wait_send_balancer)
                result_code = self._send_balancer_worker(balancer_keys)
                if result_code == 'ok':
                    checkpoint.wait_send_balancer.clear()
                    logger.info(f"✅ Grok balancer queue processed successfully")

            if checkpoint.wait_send_gpt_load and self.gpt_load_enabled:
                gpt_load_keys = list(checkpoint.wait_send_gpt_load)
                result_code = self._send_gpt_load_worker(gpt_load_keys)
                if result_code == 'ok':
                    checkpoint.wait_send_gpt_load.clear()
                    logger.info(f"✅ GPT load balancer queue processed successfully")

            file_manager.save_checkpoint(checkpoint)
        except Exception as e:
            logger.error(f"❌ Batch send worker error: {e}")
        finally:
            self.saving_checkpoint = False

    def shutdown(self) -> None:
        self.shutdown_flag = True
        if self.batch_timer: self.batch_timer.cancel()
        self.executor.shutdown(wait=True)
        logger.info("🔚 SyncUtils shutdown complete")

sync_utils = SyncUtils()
