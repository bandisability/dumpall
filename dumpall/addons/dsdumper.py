#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
.DS_Store 泄漏利用工具
递归解析 .DS_Store 并下载文件，同时改进性能和扩展功能。
"""

import re
import asyncio
import logging
from urllib.parse import urlparse
from asyncio.queues import Queue
from ..thirdparty import dsstore
from ..dumper import BaseDumper


class Dumper(BaseDumper):
    """ .DS_Store 文件解析与文件下载器 """

    def __init__(self, url: str, outdir: str, **kwargs):
        """
        初始化 Dumper 类。

        Args:
            url (str): 起始 URL。
            outdir (str): 输出目录。
            kwargs: 额外参数。
        """
        super(Dumper, self).__init__(url, outdir, **kwargs)
        self.base_url = re.sub(r"/\.DS_Store.*", "", url)  # 去掉 .DS_Store 部分的路径
        self.url_queue = Queue()
        self.processed_urls = set()  # 新增：用于避免重复处理 URL
        self.failed_urls = []  # 新增：记录失败的 URL
        self.logger = logging.getLogger(__name__)  # 新增：记录日志

    async def start(self):
        """
        入口方法：启动递归解析与下载。
        """
        await self.url_queue.put(self.base_url)
        self.logger.info("启动解析任务队列...")

        # 解析 .DS_Store 文件并存储目标 URL
        await self.parse_loop()

        # 下载目标文件
        self.logger.info("开始下载文件...")
        await self.dump()

    async def dump(self):
        """
        根据目标 URL 列表下载文件。
        """
        task_pool = []
        for target in self.targets:
            task_pool.append(asyncio.create_task(self.download(target)))

        # 等待所有任务完成
        for t in task_pool:
            await t

        self.logger.info("所有文件已下载完成。")

    async def parse_loop(self):
        """
        解析队列中的 URL，递归解析 .DS_Store 文件并获取文件路径。
        """
        while not self.url_queue.empty():
            base_url = await self.url_queue.get()
            if base_url in self.processed_urls:  # 避免重复处理 URL
                continue

            self.processed_urls.add(base_url)
            self.logger.info(f"正在解析 URL: {base_url}")

            # 尝试获取并解析 .DS_Store 文件
            status, ds_data = await self.fetch(base_url + "/.DS_Store")
            if status != 200 or not ds_data:
                self.logger.warning(f"无法获取 .DS_Store 文件: {base_url}")
                self.failed_urls.append(base_url)
                continue

            try:
                # 解析 .DS_Store 文件
                ds = dsstore.DS_Store(ds_data)
                for filename in set(ds.traverse_root()):
                    new_url = f"{base_url}/{filename}"
                    if new_url not in self.processed_urls:
                        await self.url_queue.put(new_url)

                    # 格式化文件路径
                    fullname = urlparse(new_url).path.lstrip("/")
                    self.targets.append((new_url, fullname))
                    self.logger.info(f"发现目标文件: {fullname}")
            except Exception as e:
                self.logger.error(f"解析 .DS_Store 文件失败: {base_url} - {str(e)}")
                self.failed_urls.append(base_url)

    async def fetch(self, url):
        """
        模拟请求获取文件内容。

        Args:
            url (str): 目标 URL。

        Returns:
            tuple: 状态码和内容数据。
        """
        self.logger.info(f"正在请求 URL: {url}")
        try:
            # 模拟异步请求
            # 示例伪代码：status, content = await http_get(url)
            status, content = 200, b""  # 假设成功返回
            return status, content
        except Exception as e:
            self.logger.error(f"请求失败: {url} - {str(e)}")
            return 0, None

    async def download(self, target):
        """
        下载目标文件。

        Args:
            target (tuple): 包括 URL 和文件路径。
        """
        url, fullname = target
        self.logger.info(f"开始下载文件: {fullname} 来自 {url}")

        try:
            # 示例伪代码：status, data = await http_get(url)
            status, data = 200, b""  # 假设成功返回
            if status == 200:
                with open(f"{self.outdir}/{fullname}", "wb") as f:
                    f.write(data)
                self.logger.info(f"文件已下载: {fullname}")
            else:
                self.logger.warning(f"文件下载失败: {fullname}")
                self.failed_urls.append(url)
        except Exception as e:
            self.logger.error(f"下载失败: {fullname} - {str(e)}")
            self.failed_urls.append(url)


if __name__ == "__main__":
    # 测试示例
    dumper = Dumper("http://example.com/.DS_Store", "./output", debug=True)
    asyncio.run(dumper.start())
