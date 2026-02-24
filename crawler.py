import os
import time
import random
import requests
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from PyQt6.QtCore import QThread, pyqtSignal, QObject
from PyQt6.QtGui import QImageReader, QImage
from urllib.parse import urljoin
from utils import format_filename, sanitize_filename
import datetime

class CrawlerSignals(QObject):
    log = pyqtSignal(str, str) # message, level (info, error, success)
    status_update = pyqtSignal(str, str, str, str, str) # url, status (running, done, error), title, date_str, local_path
    redirected = pyqtSignal(str, str) # old_url, new_url
    progress = pyqtSignal(str, int, int)
    image_downloaded = pyqtSignal(str, str)
    bandwidth_update = pyqtSignal(int)
    finished = pyqtSignal()

class CrawlerWorker(QThread):
    def __init__(self, start_url, config):
        super().__init__()
        self.url_queue = [start_url]
        self.visited_urls = set()
        self.config = config
        self.signals = CrawlerSignals()
        self.is_running = True
        self.session = requests.Session()
        self.total_bytes_downloaded = 0
        
        # Configure Retry Strategy for Network Stability
        retries = Retry(
            total=5,
            backoff_factor=2, # Increased backoff: 2s, 4s, 8s, 16s, 32s
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
    def run(self):
        try:
            mode = self.config.get('mode', 'next') # next, prev, free
            
            while self.is_running and self.url_queue:
                current_url = self.url_queue.pop(0)
                if current_url in self.visited_urls:
                    continue
                    
                self.visited_urls.add(current_url)
                self.process_page(current_url, mode)
                
                # Anti-scraping delay
                p_min, p_max = self.config.get('page_delay', (2.0, 5.0))
                if p_max < p_min: p_max = p_min
                delay = random.uniform(p_min, p_max)
                self.signals.log.emit(f"Waiting {delay:.1f}s before next page...", "info")
                time.sleep(delay)
        except Exception as e:
            self.signals.log.emit(f"爬虫线程发生未捕获异常: {str(e)}", "error")
            import traceback
            traceback.print_exc()
        finally:
            self.signals.finished.emit()
        
    def stop(self):
        self.is_running = False

    def get_headers(self):
        # 使用固定的桌面端 User-Agent，避免移动端页面结构差异导致解析失败
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        }

    def process_page(self, url, mode):
        self.signals.log.emit(f"Analyzing [v1.0.1] {url}...", "info")
        self.signals.status_update.emit(url, "running", "Analyzing...", "", "")
        
        try:
            # 回滚：请求页面时不带 Referer，模拟直接访问
            resp = self.session.get(url, headers=self.get_headers(), timeout=15)
            
            # Check for redirection
            if resp.url != url:
                self.signals.log.emit(f"Redirected: {url} -> {resp.url}", "info")
                self.signals.redirected.emit(url, resp.url)
                url = resp.url # Update local url variable for subsequent operations
            
            # Encoding detection
            # 优先从 meta 标签获取编码，否则默认 utf-8
            # 有些网站 meta 写 gbk 但实际是 utf-8，反之亦然。
            # 这里先尝试检测 meta，增加检测范围
            content_preview = resp.content[:2000].lower()
            if b'charset=gbk' in content_preview or b'charset="gbk"' in content_preview:
                resp.encoding = 'gbk'
            elif b'charset=utf-8' in content_preview or b'charset="utf-8"' in content_preview:
                resp.encoding = 'utf-8'
            else:
                # Fallback to auto detection or utf-8
                resp.encoding = resp.apparent_encoding if resp.apparent_encoding else 'utf-8'
            
            self.signals.log.emit(f"Encoding detected: {resp.encoding}", "info")

            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract Info
            # Priority: span#subject_tpc -> title tag
            subject_span = soup.find('span', id='subject_tpc')
            if subject_span:
                title = subject_span.get_text().strip()
            else:
                title = soup.title.string.strip() if soup.title else "Unknown Title"
            
            # Date
            page_date = None
            date_str = ""
            
            # Strategy 1: Find any element with "发表于" in text
            # This is more robust than class matching
            target_elements = soup.find_all(lambda tag: tag.name in ['span', 'div', 'p'] and '发表于' in tag.get_text())
            
            # Strategy 1.5: Direct string search in soup (sometimes lambda misses things)
            if not target_elements:
                 text_nodes = soup.find_all(string=re.compile("发表于"))
                 for node in text_nodes:
                     if node.parent not in target_elements:
                         target_elements.append(node.parent)

            for el in target_elements:
                # Check title attribute first (high confidence)
                title_attr = el.get('title')
                if title_attr:
                     match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})', title_attr)
                     if match:
                        date_str = match.group(1)
                        try:
                            page_date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                            self.signals.log.emit(f"Date found in title attr: {date_str}", "info")
                            break
                        except:
                            pass
                
                # Check text content
                text = el.get_text()
                match = re.search(r'发表于[:：]?\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})', text)
                if match:
                    date_str = match.group(1)
                    try:
                        page_date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        self.signals.log.emit(f"Date found in text: {date_str}", "info")
                        break
                    except:
                        pass
            
            # Strategy 2: If still not found, search for raw date pattern in the whole text (last resort)
            if not page_date:
                self.signals.log.emit("Date not found in DOM, trying regex on raw text...", "warning")
                
                # Save failed HTML for debugging
                debug_file = f"debug_failed_date_{int(time.time())}.html"
                try:
                    with open(debug_file, "w", encoding=resp.encoding, errors='replace') as f:
                        f.write(resp.text)
                    self.signals.log.emit(f"Saved failed page to {debug_file}", "warning")
                except Exception as e:
                    self.signals.log.emit(f"Failed to save debug file: {e}", "error")

                # Try matching date pattern directly without "发表于" prefix
                # Pattern: YYYY-MM-DD HH:mm
                date_pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})'
                
                # Limit search to first 30k chars
                body_text = soup.get_text()[:30000]
                matches = re.findall(date_pattern, body_text)
                
                if matches:
                    self.signals.log.emit(f"Found {len(matches)} potential dates, using the first one.", "info")
                    date_str = matches[0]
                    try:
                        page_date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                        self.signals.log.emit(f"Date found by raw date regex: {date_str}", "info")
                    except:
                        pass
                else:
                    # Fallback to searching in raw HTML (in case BeautifulSoup parsing failed)
                    raw_matches = re.findall(date_pattern, resp.text[:30000])
                    if raw_matches:
                         date_str = raw_matches[0]
                         try:
                            page_date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M')
                            self.signals.log.emit(f"Date found by raw HTML regex: {date_str}", "info")
                         except:
                            pass

            date_display = page_date.strftime('%Y-%m-%d %H:%M') if page_date else 'No Date'
            
            # Determine Local Path (approximate, since we don't have filename yet)
            # Use the folder where images will be saved
            naming_pattern = self.config.get('naming_pattern', '{page.title}/{filename}')
            # We construct a dummy filename to get the directory
            dummy_path = format_filename(url, title, page_date, "dummy.jpg", 0, naming_pattern)
            local_save_dir = os.path.dirname(os.path.join(self.config.get('save_dir', 'downloads'), dummy_path))
            
            self.signals.status_update.emit(url, "running", title, date_display, local_save_dir)
            
            # Find Images
            content_div = soup.find('div', id='read_tpc') or soup.find('div', class_='tpc_content')
            img_tags = []
            if content_div:
                img_tags = content_div.find_all('img')
                self.signals.log.emit(f"Found content div, {len(img_tags)} images inside.", "info")
            else:
                # Fallback: Find all images that look like content
                self.signals.log.emit("Content div not found, falling back to all images.", "warning")
                all_imgs = soup.find_all('img')
                img_tags = all_imgs
            
            # Filter Image URLs
            image_urls = []
            allowed_formats = self.config.get('formats', ['.jpg', '.png', '.jpeg', '.gif', '.webp', '.bmp', '.svg', '.ico', '.tiff', '.avif'])
            
            for img in img_tags:
                # Support lazy loading (data-src)
                src = img.get('data-src') or img.get('src')
                if not src:
                    continue

                # Handle relative URLs
                abs_url = urljoin(url, src)
                
                # Check extension (ignoring query params)
                try:
                    path_part = abs_url.split('?')[0]
                    ext = os.path.splitext(path_part)[1].lower()
                    if not ext: ext = '.jpg'
                    
                    if ext in allowed_formats or '*' in allowed_formats:
                        if abs_url not in image_urls:
                            image_urls.append(abs_url)
                    else:
                        # Log first 5 rejections for debug
                        if len(image_urls) == 0 and len(img_tags) > 0: 
                             self.signals.log.emit(f"Debug: Rejected {abs_url} (Ext: {ext})", "warning")
                except Exception as e:
                    self.signals.log.emit(f"Debug: Error checking {abs_url}: {e}", "error")
            
            self.signals.log.emit(f"Found {len(image_urls)} valid images to download.", "info")
            if not image_urls:
                self.signals.log.emit("No valid images found matching configuration. Check allowed formats.", "warning")
                self.signals.status_update.emit(url, "warning", title, date_display, local_save_dir)
                # Do not return here, continue to navigation
            else:
                # Download Images
                total_images = len(image_urls)
                downloaded_count = 0
                
                # 使用 ThreadPoolExecutor 实现并发下载
                # 设置 max_workers=5 以达到加速效果（原单线程，现5线程，理论速度提升显著）
                # 注意：ThreadPoolExecutor 已在文件头部导入
                with ThreadPoolExecutor(max_workers=5) as executor:
                    # 提交任务
                    future_to_url = {
                        executor.submit(self.download_image, img_url, url, title, page_date, idx): img_url
                        for idx, img_url in enumerate(image_urls)
                    }
                    
                    completed_count = 0
                    for future in as_completed(future_to_url):
                        if not self.is_running: 
                            executor.shutdown(wait=False)
                            break
                        
                        completed_count += 1
                        # Update progress (using completed count, not index)
                        self.signals.progress.emit(url, completed_count, total_images)
                        
                        try:
                            success = future.result()
                            if success:
                                downloaded_count += 1
                        except Exception as exc:
                            self.signals.log.emit(f"Download exception: {exc}", "error")

                # Status Update
                if downloaded_count == total_images:
                    self.signals.status_update.emit(url, "success", title, date_display, local_save_dir)
                elif downloaded_count > 0:
                    self.signals.status_update.emit(url, "warning", title, date_display, local_save_dir)
                else:
                    self.signals.status_update.emit(url, "error", title, date_display, local_save_dir)

            # Navigation
            if mode == 'free':
                # Logic: Try Next, then Prev
                next_link = soup.find('a', string=lambda t: t and '下一主题' in t)
                if next_link:
                    next_url = urljoin(url, next_link.get('href'))
                    if 'job.php' in next_url:
                        try:
                            self.signals.log.emit(f"Resolving redirect: {next_url}", "info")
                            # Remove stream=True to access text for HTML redirect check
                            r = self.session.get(next_url, headers=self.get_headers(), timeout=10)
                            resolved_url = r.url
                            
                            # Check for HTML meta refresh or JS redirect if URL is still job.php
                            if 'job.php' in resolved_url:
                                content = r.text
                                # Meta refresh: <meta http-equiv="refresh" content="0;url=read.php?tid=...">
                                meta_match = re.search(r'url=([^"\'>]+)', content, re.IGNORECASE)
                                if meta_match:
                                    resolved_url = urljoin(next_url, meta_match.group(1))
                                else:
                                    # JS location: location.href = 'read.php?tid=...';
                                    js_match = re.search(r"location\.href\s*=\s*['\"](.*?)['\"]", content)
                                    if js_match:
                                        resolved_url = urljoin(next_url, js_match.group(1))
                            
                            next_url = resolved_url
                            r.close()
                            self.signals.log.emit(f"Resolved to: {next_url}", "info")
                        except Exception as e:
                            self.signals.log.emit(f"Redirect resolution failed: {e}", "warning")
                    
                    if 'job.php' not in next_url:
                        self.url_queue.append(next_url)
                        self.signals.log.emit(f"Free Explore: Found next topic {next_url}", "info")
                    else:
                        self.signals.log.emit(f"Free Explore: Skipping failed redirect {next_url}", "warning")
                else:
                    self.signals.log.emit("Free Explore: No next topic, trying previous...", "warning")
                    prev_link = soup.find('a', string=lambda t: t and '上一主题' in t)
                    if prev_link:
                        prev_url = urljoin(url, prev_link.get('href'))
                        if 'job.php' in prev_url:
                            try:
                                r = self.session.get(prev_url, headers=self.get_headers(), timeout=10)
                                resolved_url = r.url
                                if 'job.php' in resolved_url:
                                    content = r.text
                                    meta_match = re.search(r'url=([^"\'>]+)', content, re.IGNORECASE)
                                    if meta_match: resolved_url = urljoin(prev_url, meta_match.group(1))
                                    else:
                                        js_match = re.search(r"location\.href\s*=\s*['\"](.*?)['\"]", content)
                                        if js_match: resolved_url = urljoin(prev_url, js_match.group(1))
                                prev_url = resolved_url
                                r.close()
                            except: pass
                        
                        if 'job.php' not in prev_url:
                            self.url_queue.append(prev_url)
                        else:
                            self.signals.log.emit(f"Free Explore: Skipping failed redirect {prev_url}", "warning")
            
            elif mode == 'next':
                next_link = soup.find('a', string=lambda t: t and '下一主题' in t)
                if next_link:
                    next_url = urljoin(url, next_link.get('href'))
                    if 'job.php' in next_url:
                        try:
                            self.signals.log.emit(f"Resolving redirect: {next_url}", "info")
                            r = self.session.get(next_url, headers=self.get_headers(), timeout=10)
                            resolved_url = r.url
                            if 'job.php' in resolved_url:
                                content = r.text
                                meta_match = re.search(r'url=([^"\'>]+)', content, re.IGNORECASE)
                                if meta_match: resolved_url = urljoin(next_url, meta_match.group(1))
                                else:
                                    js_match = re.search(r"location\.href\s*=\s*['\"](.*?)['\"]", content)
                                    if js_match: resolved_url = urljoin(next_url, js_match.group(1))
                            next_url = resolved_url
                            r.close()
                            self.signals.log.emit(f"Resolved to: {next_url}", "info")
                        except Exception as e:
                            self.signals.log.emit(f"Redirect resolution failed: {e}", "warning")
                            
                    if 'job.php' not in next_url:
                        self.url_queue.append(next_url)
                    else:
                        self.signals.log.emit(f"Skipping failed redirect: {next_url}", "warning")
                else:
                    self.signals.log.emit("No next topic found.", "warning")

            elif mode == 'prev':
                prev_link = soup.find('a', string=lambda t: t and '上一主题' in t)
                if prev_link:
                    prev_url = urljoin(url, prev_link.get('href'))
                    if 'job.php' in prev_url:
                        try:
                            self.signals.log.emit(f"Resolving redirect: {prev_url}", "info")
                            r = self.session.get(prev_url, headers=self.get_headers(), timeout=10)
                            resolved_url = r.url
                            if 'job.php' in resolved_url:
                                content = r.text
                                meta_match = re.search(r'url=([^"\'>]+)', content, re.IGNORECASE)
                                if meta_match: resolved_url = urljoin(prev_url, meta_match.group(1))
                                else:
                                    js_match = re.search(r"location\.href\s*=\s*['\"](.*?)['\"]", content)
                                    if js_match: resolved_url = urljoin(prev_url, js_match.group(1))
                            prev_url = resolved_url
                            r.close()
                            self.signals.log.emit(f"Resolved to: {prev_url}", "info")
                        except Exception as e:
                            self.signals.log.emit(f"Redirect resolution failed: {e}", "warning")
                            
                    if 'job.php' not in prev_url:
                        self.url_queue.append(prev_url)
                    else:
                        self.signals.log.emit(f"Skipping failed redirect: {prev_url}", "warning")
                else:
                    self.signals.log.emit("No previous topic found.", "warning")

        except Exception as e:
            self.signals.log.emit(f"Error processing {url}: {str(e)}", "error")
            self.signals.status_update.emit(url, "error", "Failed", "", "")

    def download_image(self, img_url, page_url, page_title, page_date, index):
        max_retries = int(self.config.get('img_retries', 3))
        # 使用页面 URL 作为 Referer，但手动添加，不依赖 get_headers 参数
        headers = self.get_headers()
        headers['Referer'] = page_url
        
        for attempt in range(max_retries):
            try:
                # Filename Generation
                # Remove query params for filename clean
                clean_url = img_url.split('?')[0]
                original_filename = os.path.basename(clean_url)
                # Check ext again for filename
                if not os.path.splitext(original_filename)[1]:
                     original_filename += '.jpg'

                naming_pattern = self.config.get('naming_pattern', '{page.title}/{filename}')
                save_path_template = format_filename(page_url, page_title, page_date, original_filename, index, naming_pattern)
                
                full_save_path = os.path.join(self.config.get('save_dir', 'downloads'), save_path_template)
                
                # Ensure dir exists
                os.makedirs(os.path.dirname(full_save_path), exist_ok=True)
                
                if os.path.exists(full_save_path):
                    self.signals.log.emit(f"File exists: {full_save_path}", "info")
                    return True

                self.signals.log.emit(f"Downloading {img_url} (Attempt {attempt+1}/{max_retries})...", "info")
                timeout = float(self.config.get('img_timeout', 30.0))
                resp = self.session.get(img_url, headers=headers, timeout=timeout, stream=True)
                
                if resp.status_code != 200:
                    raise Exception(f"HTTP {resp.status_code}")

                content = b""
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        content += chunk
                        self.total_bytes_downloaded += len(chunk)
                        self.signals.bandwidth_update.emit(self.total_bytes_downloaded)
                
                min_w, min_h = self.config.get('min_resolution', (0, 0))
                if min_w > 0 or min_h > 0:
                    image = QImage()
                    # Load from bytes directly
                    if image.loadFromData(content):
                        if image.width() < min_w or image.height() < min_h:
                            self.signals.log.emit(f"Skipped {img_url}: Resolution {image.width()}x{image.height()} < {min_w}x{min_h}", "warning")
                            return True # Treated as success (skipped)
                    else:
                        self.signals.log.emit(f"Warning: Could not check resolution for {img_url}", "warning")

                # Save to Disk
                with open(full_save_path, 'wb') as f:
                    f.write(content)
                
                self.signals.log.emit(f"Saved: {save_path_template}", "success")
                self.signals.image_downloaded.emit(img_url, full_save_path)
                
                # Download delay
                i_min, i_max = self.config.get('img_delay', (0.1, 0.5))
                if i_max < i_min: i_max = i_min
                time.sleep(random.uniform(i_min, i_max))
                
                return True

            except Exception as e:
                self.signals.log.emit(f"Failed to download {img_url} (Attempt {attempt+1}): {str(e)}", "warning")
                time.sleep(1 + attempt) # Backoff
        
        return False
