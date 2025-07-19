import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import json
import os
import re
import pandas as pd
from datetime import datetime
import concurrent.futures
import time
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class WebsiteCrawler:
    def __init__(self, base_url, max_depth=5, max_workers=10):
        self.base_url = base_url
        self.max_depth = max_depth
        self.max_workers = max_workers
        self.file_structure = {}
        self.visited_folders = set()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        })
    
    def clean_name(self, name):
        """清理文件名中的非法字符"""
        return re.sub(r'[\\/*?:"<>|]', '_', name)
    
    def is_valid_folder(self, folder_name):
        """检查是否是有效的文件夹名（数字命名）"""
        return folder_name.isdigit()
    
    def is_valid_file(self, href):
        """检查是否是有效的文件链接"""
        # 排除排序链接和目录链接
        if href.startswith('?') or href.endswith('/'):
            return False
        
        # 排除特殊链接
        if href in ['../', './']:
            return False
        
        # 排除看起来像排序参数的链接
        if any(param in href for param in ['C=N', 'C=M', 'C=S', 'C=D']):
            return False
        
        return True
    
    def parse_directory(self, url, depth=0):
        """解析目录内容"""
        if depth > self.max_depth:
            return []
        
        if url in self.visited_folders:
            return []
        
        self.visited_folders.add(url)
        logging.info(f"正在处理: {url}")
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            items = []
            
            # 解析所有链接
            for link in soup.find_all('a'):
                href = link.get('href')
                if not href:
                    continue
                
                # 清理href
                href = href.strip()
                
                # 如果是文件夹
                if href.endswith('/'):
                    folder_name = href.rstrip('/')
                    if self.is_valid_folder(folder_name):
                        items.append({
                            "type": "folder",
                            "name": folder_name,
                            "url": urljoin(url, href),
                            "parent": url
                        })
                
                # 如果是文件
                elif self.is_valid_file(href):
                    # 尝试获取文件大小和日期
                    file_size = ""
                    file_date = ""
                    
                    # 查找包含大小的相邻单元格
                    size_cell = link.find_next_sibling('td')
                    if size_cell:
                        file_size = size_cell.get_text(strip=True)
                        # 尝试获取日期
                        date_cell = size_cell.find_next_sibling('td')
                        if date_cell:
                            file_date = date_cell.get_text(strip=True)
                    
                    items.append({
                        "type": "file",
                        "name": self.clean_name(href),
                        "url": urljoin(url, href),
                        "size": file_size,
                        "date": file_date,
                        "parent": url
                    })
            
            return items
        
        except Exception as e:
            logging.error(f"处理目录失败: {url} - {str(e)}")
            return []
    
    def crawl_folder(self, folder_url, depth=0):
        """爬取单个文件夹及其子文件夹"""
        if depth > self.max_depth:
            return
        
        folder_id = folder_url.rstrip('/').split('/')[-1]
        if not folder_id.isdigit():
            return
        
        # 如果已经处理过，直接返回
        if folder_id in self.file_structure:
            return
        
        # 获取文件夹内容
        items = self.parse_directory(folder_url, depth)
        if not items:
            return
        
        # 初始化文件夹结构
        self.file_structure[folder_id] = {
            "url": folder_url,
            "files": [],
            "subfolders": []
        }
        
        # 处理文件夹内容
        for item in items:
            if item["type"] == "folder":
                # 记录子文件夹
                self.file_structure[folder_id]["subfolders"].append(item["name"])
                
                # 递归爬取子文件夹
                self.crawl_folder(item["url"], depth + 1)
            else:
                # 记录文件
                self.file_structure[folder_id]["files"].append({
                    "original_name": item["name"],
                    "url": item["url"],
                    "size": item["size"],
                    "date": item["date"]
                })
    
    def crawl(self):
        """开始爬取整个网站"""
        logging.info(f"开始爬取网站: {self.base_url}")
        start_time = time.time()
        
        # 使用线程池加速爬取
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 先爬取前1000个文件夹
            futures = []
            for i in range(1, 6315):
                folder_url = urljoin(self.base_url, f"{i}/")
                futures.append(executor.submit(self.crawl_folder, folder_url))
            
            # 等待所有任务完成
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"爬取过程中出错: {str(e)}")
        
        elapsed = time.time() - start_time
        logging.info(f"爬取完成! 共处理 {len(self.file_structure)} 个文件夹, 耗时 {elapsed:.2f} 秒")
    
    def generate_excel(self, output_file):
        """生成Excel文件用于重命名规划"""
        # 准备数据
        data = []
        
        for folder_id, folder_data in self.file_structure.items():
            # 跳过空文件夹
            if not folder_data["files"]:
                continue
                
            for file_info in folder_data["files"]:
                data.append({
                    "文件夹ID": folder_id,
                    "原始文件名": file_info["original_name"],
                    "新文件名": "",  # 留空供用户填写
                    "文件大小": file_info["size"],
                    "修改日期": file_info["date"],
                    "文件URL": file_info["url"],
                    "文件夹URL": folder_data["url"]
                })
        
        # 如果没有找到文件，返回空DataFrame
        if not data:
            logging.warning("没有找到任何文件!")
            return pd.DataFrame()
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 设置列顺序
        column_order = ["文件夹ID", "原始文件名", "新文件名", "文件大小", "修改日期", "文件URL", "文件夹URL"]
        df = df[column_order]
        
        # 保存到Excel
        try:
            df.to_excel(output_file, index=False)
            logging.info(f"Excel文件已生成: {os.path.abspath(output_file)}")
        except Exception as e:
            logging.error(f"生成Excel失败: {str(e)}")
            logging.info("请尝试安装 openpyxl: pip install openpyxl")
        
        return df
    
    def save_to_json(self, output_file):
        """保存文件结构到JSON"""
        # 过滤掉空文件夹
        filtered_structure = {
            folder_id: folder_data 
            for folder_id, folder_data in self.file_structure.items()
            if folder_data["files"]  # 只保留有文件的文件夹
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_structure, f, indent=2, ensure_ascii=False)
        logging.info(f"JSON文件已生成: {os.path.abspath(output_file)}")
        
        return filtered_structure

def main():
    # 配置参数
    BASE_URL = "http://ftp.airnet.ne.jp/pub/pc/freem/"
    EXCEL_FILE = "freem_file_rename_plan.xlsx"
    JSON_FILE = "freem_file_structure.json"
    
    # 创建爬虫实例
    crawler = WebsiteCrawler(BASE_URL, max_depth=2, max_workers=20)
    
    # 开始爬取
    crawler.crawl()
    
    # 保存结果
    file_structure = crawler.save_to_json(JSON_FILE)
    df = crawler.generate_excel(EXCEL_FILE)
    
    # 打印摘要信息
    total_folders = len(file_structure)
    total_files = sum(len(folder["files"]) for folder in file_structure.values())
    
    logging.info("\n" + "=" * 60)
    logging.info(f"爬取摘要:")
    logging.info(f"文件夹数量: {total_folders}")
    logging.info(f"文件总数: {total_files}")
    
    if not df.empty:
        logging.info(f"Excel文件包含 {len(df)} 行记录")
    
    logging.info("=" * 60)
    
    # 使用说明
    logging.info("\n下一步操作:")
    if not df.empty:
        logging.info(f"1. 打开 {EXCEL_FILE}")
        logging.info("2. 在'新文件名'列填写您想要的文件名")
        logging.info("3. 保存Excel文件")
        logging.info("4. 使用重命名脚本处理文件")
    else:
        logging.info("没有找到任何文件，请检查爬取过程是否有错误")

if __name__ == "__main__":
    main()