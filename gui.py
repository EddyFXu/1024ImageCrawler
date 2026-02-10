import sys
import os
import json
import traceback
import time
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QPushButton, QRadioButton, QButtonGroup, 
    QCheckBox, QComboBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QGroupBox, QScrollArea, QSplitter, QTextEdit, QTabWidget,
    QGridLayout, QMessageBox, QDoubleSpinBox, QSizePolicy, QGraphicsBlurEffect, QStackedLayout
)
from PyQt6.QtCore import Qt, QSize, QTimer, QPointF, QUrl
from PyQt6.QtGui import QPixmap, QIcon, QColor, QAction, QPainter, QPen, QBrush, QPainterPath, QDesktopServices
from crawler import CrawlerWorker
from utils import get_app_path, get_resource_path

VERSION = "v1.0.8"

# Global exception hook to capture crashes in compiled exe
def exception_hook(exctype, value, tb):
    error_msg = "".join(traceback.format_exception(exctype, value, tb))
    # Write to a file in the same directory as the executable
    log_path = os.path.join(get_app_path(), "crash.log")
    try:
        with open(log_path, "w") as f:
            f.write(error_msg)
    except:
        pass # If we can't write, we can't do much
        
    # Also try to show a message box if QApplication is still alive
    if QApplication.instance():
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setText("程序发生严重错误 (Crash)")
        msg.setInformativeText(f"错误信息已保存至: {log_path}\n\n{str(value)}")
        msg.setDetailedText(error_msg)
        msg.exec()
    
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = exception_hook

# Config file should be stored in the app directory (or user data dir, but app dir is fine for portable)
CONFIG_FILE = os.path.join(get_app_path(), "config.json")

class ImagePreviewWidget(QWidget):
    def __init__(self):
        super().__init__()
        # Main layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Image Label
        self.image_label = QLabel("无预览")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setStyleSheet("background-color: #f0f0f0; border: 1px solid #ccc;")
        self.image_label.setMinimumSize(200, 200)
        self.main_layout.addWidget(self.image_label)
        
        # Blur effect
        self.blur_effect = QGraphicsBlurEffect()
        self.blur_effect.setBlurRadius(0) # Start clear
        self.image_label.setGraphicsEffect(self.blur_effect)
        
        # Toggle Button (Floating)
        self.toggle_btn = QPushButton(self)
        self.toggle_btn.setFixedSize(28, 28)
        # Use generated icons instead of text
        self.icon_eye = self.create_eye_icon(crossed=False)
        self.icon_crossed = self.create_eye_icon(crossed=True)
        self.toggle_btn.setIcon(self.icon_eye)
        self.toggle_btn.setIconSize(QSize(24, 24))
        
        self.toggle_btn.setStyleSheet("""
            QPushButton {
                background-color: rgba(255, 255, 255, 120);
                border: 1px solid rgba(255, 255, 255, 180);
                border-radius: 14px;
            }
            QPushButton:hover {
                background-color: rgba(255, 255, 255, 200);
                border: 1px solid rgba(255, 255, 255, 255);
            }
        """)
        self.toggle_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.toggle_btn.clicked.connect(self.toggle_privacy)
        self.toggle_btn.setToolTip("切换隐私模式 (模糊显示)")
        
        self.is_private = False
        self.current_raw_pixmap = None
        self.current_path = None
        
    def create_eye_icon(self, crossed=False):
        pixmap = QPixmap(32, 32)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Color: Lighter Gray for a softer look
        color = QColor("#9E9E9E")
        pen = QPen(color)
        pen.setWidth(2)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        pen.setJoinStyle(Qt.PenJoinStyle.RoundJoin)
        painter.setPen(pen)
        
        # Draw Eye Outline (Almond shape) - Maximized for 32x32 canvas
        path = QPainterPath()
        # Start left (2, 16)
        path.moveTo(2, 16)
        # Top curve to (30, 16) via (16, 2)
        path.quadTo(16, 2, 30, 16) 
        # Bottom curve back to (2, 16) via (16, 30)
        path.quadTo(16, 30, 2, 16) 
        painter.drawPath(path)
        
        # Draw Pupil
        painter.setBrush(QBrush(color))
        painter.drawEllipse(QPointF(16, 16), 5, 5)
        
        if crossed:
            # Draw Slash
            pen.setWidth(3)
            pen.setColor(QColor("#9E9E9E")) 
            painter.setPen(pen)
            # From top-left to bottom-right, fitting the new size
            painter.drawLine(4, 4, 28, 28)
            
        painter.end()
        return QIcon(pixmap)
        
    def resizeEvent(self, event):
        self.toggle_btn.move(self.width() - 38, 10)
        self.update_display()
        super().resizeEvent(event)
        
    def toggle_privacy(self):
        self.is_private = not self.is_private
        if self.is_private:
            self.blur_effect.setBlurRadius(30)
            self.toggle_btn.setIcon(self.icon_crossed)
        else:
            self.blur_effect.setBlurRadius(0)
            self.toggle_btn.setIcon(self.icon_eye)
            
    def set_pixmap(self, pixmap, path=None):
        self.current_raw_pixmap = pixmap
        self.current_path = path
        self.update_display()

    def update_display(self):
        if self.current_raw_pixmap and not self.current_raw_pixmap.isNull():
            w = self.image_label.width()
            h = self.image_label.height()
            if w > 0 and h > 0:
                scaled = self.current_raw_pixmap.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                self.image_label.setPixmap(scaled)
        else:
            self.image_label.setText("无预览")
            
    def mouseDoubleClickEvent(self, event):
        if self.current_path and os.path.exists(self.current_path):
            try:
                # Use os.startfile on Windows for better compatibility with default viewers
                os.startfile(self.current_path)
            except Exception as e:
                print(f"Error opening file: {e}")
                # Fallback
                try:
                    QDesktopServices.openUrl(QUrl.fromLocalFile(self.current_path))
                except:
                    pass
        super().mouseDoubleClickEvent(event)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"1024图片爬虫工具 {VERSION}")
        self.resize(1200, 800)
        
        # Set Window Icon
        logo_path = get_resource_path("logo.png")
        if os.path.exists(logo_path):
            self.setWindowIcon(QIcon(logo_path))
            
        # Status Bar
        self.status_bar = self.statusBar()
        
        self.worker = None
        self.downloaded_images = []
        self.last_success_url = ""
        self.total_bytes_downloaded = 0
        self.last_bytes_value = 0
        self.last_bandwidth_time = time.time()
        
        self.total_tasks_count = 0
        self.total_images_count = 0
        
        self.init_ui()
        self.load_config()
        
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        
        # --- Left Panel: Controls ---
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_panel.setFixedWidth(350)
        
        # 1. Target URL
        url_group = QGroupBox("目标地址")
        url_layout = QVBoxLayout()
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("输入初始页面URL...")
        url_layout.addWidget(self.url_input)
        
        # Last URL Info
        last_url_layout = QHBoxLayout()
        self.lbl_last_url = QLabel("上次成功: 无")
        self.lbl_last_url.setStyleSheet("color: gray; font-size: 11px;")
        self.lbl_last_url.setWordWrap(False)
        self.lbl_last_url.setMaximumWidth(250)
        
        btn_restore = QPushButton("恢复")
        btn_restore.setFixedSize(40, 20)
        btn_restore.setStyleSheet("font-size: 11px; padding: 0px;")
        btn_restore.clicked.connect(self.restore_last_url)
        
        last_url_layout.addWidget(self.lbl_last_url)
        last_url_layout.addWidget(btn_restore)
        url_layout.addLayout(last_url_layout)
        
        url_group.setLayout(url_layout)
        left_layout.addWidget(url_group)
        
        # 2. Navigation Mode
        nav_group = QGroupBox("探索模式")
        nav_layout = QVBoxLayout()
        self.nav_bg = QButtonGroup()
        
        self.radio_next = QRadioButton("自动下一主题")
        self.radio_next.setChecked(True)
        self.nav_bg.addButton(self.radio_next, 0)
        nav_layout.addWidget(self.radio_next)
        
        self.radio_prev = QRadioButton("自动上一主题")
        self.nav_bg.addButton(self.radio_prev, 1)
        nav_layout.addWidget(self.radio_prev)
        
        self.radio_auto = QRadioButton("自由探索 (实验性)")
        self.nav_bg.addButton(self.radio_auto, 2)
        nav_layout.addWidget(self.radio_auto)
        
        nav_group.setLayout(nav_layout)
        left_layout.addWidget(nav_group)
        
        # 3. Filters
        filter_group = QGroupBox("图片过滤")
        filter_layout = QVBoxLayout()
        
        # Resolution
        res_layout = QHBoxLayout()
        res_layout.addWidget(QLabel("最小分辨率:"))
        self.res_combo = QComboBox()
        self.res_combo.addItems(["不限制", "1920x1080", "1280x720", "1024x768", "800x600", "500x500", "自定义"])
        self.res_combo.setEditable(True)
        res_layout.addWidget(self.res_combo)
        filter_layout.addLayout(res_layout)
        
        # Formats
        fmt_layout = QGridLayout()
        self.fmt_checks = {}
        formats = ["jpg", "png", "webp", "gif", "svg"]
        for i, fmt in enumerate(formats):
            cb = QCheckBox(fmt)
            cb.setChecked(True)
            self.fmt_checks[fmt] = cb
            fmt_layout.addWidget(cb, i // 3, i % 3)
        filter_layout.addLayout(fmt_layout)
        
        filter_group.setLayout(filter_layout)
        left_layout.addWidget(filter_group)
        
        # 4. Settings
        set_group = QGroupBox("保存设置")
        set_layout = QVBoxLayout()
        
        # Path
        path_layout = QHBoxLayout()
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("保存目录...")
        btn_browse = QPushButton("...")
        btn_browse.setFixedWidth(30)
        btn_browse.clicked.connect(self.browse_dir)
        path_layout.addWidget(self.path_input)
        path_layout.addWidget(btn_browse)
        set_layout.addWidget(QLabel("保存路径:"))
        set_layout.addLayout(path_layout)
        
        # Naming
        name_layout = QHBoxLayout()
        self.naming_input = QLineEdit()
        self.naming_input.setPlaceholderText("{page.title}/{filename}")
        self.naming_input.setText("{page.title}/{filename}")
        
        btn_help = QPushButton("?")
        btn_help.setFixedSize(20, 20)
        btn_help.clicked.connect(self.show_naming_help)
        
        name_layout.addWidget(self.naming_input)
        name_layout.addWidget(btn_help)
        
        set_layout.addWidget(QLabel("重命名规则:"))
        set_layout.addLayout(name_layout)
        
        set_group.setLayout(set_layout)
        left_layout.addWidget(set_group)
        
        # 5. Delays
        delay_group = QGroupBox("延迟设置 (秒)")
        delay_layout = QGridLayout()
        
        delay_layout.addWidget(QLabel("页面间隔:"), 0, 0)
        self.page_delay_min = QDoubleSpinBox()
        self.page_delay_min.setRange(0.1, 60.0)
        self.page_delay_min.setValue(2.0)
        delay_layout.addWidget(self.page_delay_min, 0, 1)
        delay_layout.addWidget(QLabel("-"), 0, 2)
        self.page_delay_max = QDoubleSpinBox()
        self.page_delay_max.setRange(0.1, 60.0)
        self.page_delay_max.setValue(5.0)
        delay_layout.addWidget(self.page_delay_max, 0, 3)
        
        delay_layout.addWidget(QLabel("图片间隔:"), 1, 0)
        self.img_delay_min = QDoubleSpinBox()
        self.img_delay_min.setRange(0.0, 10.0)
        self.img_delay_min.setValue(0.1)
        delay_layout.addWidget(self.img_delay_min, 1, 1)
        delay_layout.addWidget(QLabel("-"), 1, 2)
        self.img_delay_max = QDoubleSpinBox()
        self.img_delay_max.setRange(0.0, 10.0)
        self.img_delay_max.setValue(0.5)
        delay_layout.addWidget(self.img_delay_max, 1, 3)
        
        delay_group.setLayout(delay_layout)
        left_layout.addWidget(delay_group)
        
        # 6. Action Buttons
        btn_layout = QHBoxLayout()
        self.btn_start = QPushButton("开始爬取")
        self.btn_start.setFixedHeight(40)
        self.btn_start.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold;")
        self.btn_start.clicked.connect(self.start_crawler)
        
        self.btn_stop = QPushButton("停止")
        self.btn_stop.setFixedHeight(40)
        self.btn_stop.setStyleSheet("background-color: #f44336; color: white; font-weight: bold;")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.stop_crawler)
        
        btn_layout.addWidget(self.btn_start)
        btn_layout.addWidget(self.btn_stop)
        left_layout.addLayout(btn_layout)
        
        left_layout.addStretch()
        main_layout.addWidget(left_panel)
        
        # --- Right Panel: Content ---
        right_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # 1. Task List (Top)
        task_group = QGroupBox("任务列表")
        task_layout = QVBoxLayout()
        self.task_table = QTableWidget()
        self.task_table.setColumnCount(5)
        self.task_table.setHorizontalHeaderLabels(["状态", "地址", "标题", "时间", "操作"])
        self.task_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.task_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self.task_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.task_table.setColumnWidth(4, 60)
        self.task_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.task_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        task_layout.addWidget(self.task_table)
        task_group.setLayout(task_layout)
        right_splitter.addWidget(task_group)
        
        # 2. Gallery (Middle)
        gallery_group = QGroupBox("已下载图片")
        gallery_layout = QHBoxLayout() # Use HBox for better space usage in middle section
        
        # Preview Area (Left side of gallery)
        self.preview_widget = ImagePreviewWidget()
        gallery_layout.addWidget(self.preview_widget, 1)
        
        # Image List (Right side of gallery)
        self.image_list = QTableWidget()
        self.image_list.setColumnCount(2)
        self.image_list.setHorizontalHeaderLabels(["文件名", "路径"])
        self.image_list.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.image_list.itemClicked.connect(self.on_image_selected)
        gallery_layout.addWidget(self.image_list, 1)
        
        gallery_group.setLayout(gallery_layout)
        right_splitter.addWidget(gallery_group)
        
        # 3. Log (Bottom)
        log_group = QGroupBox("运行日志")
        log_layout = QVBoxLayout()
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        # Bandwidth Speed
        speed_layout = QGridLayout()
        self.bandwidth_label = QLabel("当前带宽: 0 KB/s")
        speed_layout.addWidget(self.bandwidth_label, 0, 0)
        log_layout.addLayout(speed_layout)
        
        log_group.setLayout(log_layout)
        right_splitter.addWidget(log_group)
        
        # Set splitter proportions (Task:Gallery:Log = 2:6:2)
        right_splitter.setStretchFactor(0, 2)
        right_splitter.setStretchFactor(1, 6)
        right_splitter.setStretchFactor(2, 2)
        
        main_layout.addWidget(right_splitter)
        
    def load_config(self):
        if not os.path.exists(CONFIG_FILE):
            return
            
        try:
            with open(CONFIG_FILE, 'r') as f:
                data = json.load(f)
                
            self.url_input.setText(data.get('url', ''))
            self.path_input.setText(data.get('save_dir', ''))
            self.naming_input.setText(data.get('naming', '{page.title}/{filename}'))
            self.res_combo.setCurrentText(data.get('res', '不限制'))
            
            formats = data.get('formats', [])
            if formats:
                # Normalize formats to no-dot for matching keys
                formats_no_dot = [f.lstrip('.') for f in formats]
                for fmt, cb in self.fmt_checks.items():
                    cb.setChecked(fmt in formats_no_dot)
            
            p_min, p_max = data.get('page_delay', (2.0, 5.0))
            self.page_delay_min.setValue(p_min)
            self.page_delay_max.setValue(p_max)
            
            i_min, i_max = data.get('img_delay', (0.1, 0.5))
            self.img_delay_min.setValue(i_min)
            self.img_delay_max.setValue(i_max)
            
            self.last_success_url = data.get('last_success_url', '')
            if self.last_success_url:
                self.lbl_last_url.setText(f"上次成功: {self.last_success_url}")
                self.lbl_last_url.setToolTip(self.last_success_url)
                
        except Exception as e:
            self.log(f"加载配置失败: {e}", "error")

    def restore_last_url(self):
        if self.last_success_url:
            self.url_input.setText(self.last_success_url)
            self.log(f"已恢复上次成功URL: {self.last_success_url}", "success")
        else:
            QMessageBox.information(self, "提示", "没有记录到上次成功的URL")

    def browse_dir(self):
        path = QFileDialog.getExistingDirectory(self, "选择保存目录")
        if path:
            self.path_input.setText(path)

    def show_naming_help(self):
        msg = """
        支持的命名参数：
        {filename}: 原始文件名
        {no.10001}: 自增序号 (从10001开始)
        {origin_serial}: 原始链接中的序号
        {page.title}: 帖子标题
        {page.date}: 帖子日期 (YYYY-MM-DD)
        {YYYY}, {MM}, {DD}: 年月日
        {HH}, {mm}, {ss}: 时分秒
        """
        QMessageBox.information(self, "重命名规则帮助", msg)

    def start_crawler(self):
        url = self.url_input.text().strip()
        if not url:
            QMessageBox.warning(self, "提示", "请输入目标地址")
            return
            
        save_dir = self.path_input.text().strip()
        if not save_dir:
            QMessageBox.warning(self, "提示", "请选择保存目录")
            return
            
        # Collect config
        config = {
            'save_dir': save_dir,
            'naming': self.naming_input.text().strip(),
            'mode': ['next', 'prev', 'free'][self.nav_bg.checkedId()],
            'min_res': self.parse_resolution(),
            'formats': [('.' + f if not f.startswith('.') else f) for f, cb in self.fmt_checks.items() if cb.isChecked()],
            'page_delay': (self.page_delay_min.value(), self.page_delay_max.value()),
            'img_delay': (self.img_delay_min.value(), self.img_delay_max.value())
        }
        
        self.save_config()
        self.downloaded_images = []
        self.image_list.setRowCount(0)
        self.task_table.setRowCount(0)
        self.total_tasks_count = 0
        self.total_images_count = 0
        
        self.worker = CrawlerWorker(url, config)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.status_update.connect(self.update_task_status)
        self.worker.signals.redirected.connect(self.update_redirected_url)
        self.worker.signals.image_downloaded.connect(self.add_image_to_gallery)
        self.worker.signals.finished.connect(self.on_finished)
        self.worker.signals.bandwidth_update.connect(self.on_bandwidth_update)
        
        self.worker.start()
        
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.log("爬虫已启动...", "info")

    def log(self, message, level="info"):
        color = "black"
        if level == "error": color = "red"
        elif level == "success": color = "green"
        elif level == "warning": color = "orange"
        
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.append(f'<span style="color:{color}">[{timestamp}] {message}</span>')
        
        # Scroll to bottom
        sb = self.log_text.verticalScrollBar()
        sb.setValue(sb.maximum())

    def stop_crawler(self):
        if self.worker:
            self.worker.stop()
            self.log("正在停止...", "warning")
            self.btn_stop.setEnabled(False)

    def on_finished(self):
        self.log("爬虫任务结束", "success")
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        self.bandwidth_label.setText("当前带宽: 0 KB/s")

    def on_bandwidth_update(self, total_bytes):
        self.total_bytes_downloaded = total_bytes
        now = time.time()
        elapsed = now - self.last_bandwidth_time
        if elapsed <= 0:
            return
        delta = self.total_bytes_downloaded - self.last_bytes_value
        if delta < 0:
            delta = 0
        rate = delta / elapsed
        self.last_bytes_value = self.total_bytes_downloaded
        self.last_bandwidth_time = now
        if rate < 1024:
            text = f"当前带宽: {rate:.1f} B/s"
        elif rate < 1024 * 1024:
            text = f"当前带宽: {rate / 1024:.1f} KB/s"
        else:
            text = f"当前带宽: {rate / (1024 * 1024):.2f} MB/s"
        self.bandwidth_label.setText(text)

    def update_redirected_url(self, old_url, new_url):
        rows = self.task_table.rowCount()
        for r in range(rows):
            if self.task_table.item(r, 1).text() == old_url:
                self.task_table.setItem(r, 1, QTableWidgetItem(new_url))
                break

    def update_task_status(self, url, status, title, date_str, local_path=""):
        # Check if URL exists in table
        rows = self.task_table.rowCount()
        found_row = -1
        for r in range(rows):
            if self.task_table.item(r, 1).text() == url:
                found_row = r
                break
        
        if found_row == -1:
            # Insert at top (reverse order)
            self.task_table.insertRow(0)
            found_row = 0
            self.task_table.setItem(0, 1, QTableWidgetItem(url))
            
            # Set custom row number (cumulative count)
            self.total_tasks_count += 1
            self.task_table.setVerticalHeaderItem(0, QTableWidgetItem(str(self.total_tasks_count)))
            
            # Add Open Folder Button
            btn_open = QPushButton("📂")
            btn_open.setToolTip("打开文件夹")
            btn_open.setFixedSize(30, 24)
            btn_open.setStyleSheet("padding: 2px;")
            # Use lambda with default argument to capture current path, but wait, local_path might change?
            # Actually local_path is sent with updates. We should store it in the item data.
            # But here we can just connect it to a method that reads the data.
            btn_open.clicked.connect(lambda: self.open_task_folder(url))
            
            # Center the button
            widget = QWidget()
            layout = QHBoxLayout(widget)
            layout.setContentsMargins(0,0,0,0)
            layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(btn_open)
            self.task_table.setCellWidget(0, 4, widget)

        # Store local_path in the URL item for retrieval
        if local_path:
             self.task_table.item(found_row, 1).setData(Qt.ItemDataRole.UserRole, local_path)
            
        # Update status icon
        icon_label = QLabel()
        if status == "running":
            # Just text or a simple indicator, can't easily do spinner without gif
            self.task_table.setItem(found_row, 0, QTableWidgetItem("Running"))
            self.task_table.item(found_row, 0).setBackground(QColor("#e3f2fd"))
        elif status == "success" or status == "done":
            self.task_table.setItem(found_row, 0, QTableWidgetItem("Success"))
            self.task_table.item(found_row, 0).setBackground(QColor("#e8f5e9"))
            self.task_table.item(found_row, 0).setForeground(QColor("green"))
            
            # Save success url
            self.last_success_url = url
            self.lbl_last_url.setText(f"上次成功: {url}")
            self.lbl_last_url.setToolTip(url)
            self.save_config()
            
        elif status == "warning":
            self.task_table.setItem(found_row, 0, QTableWidgetItem("Warning"))
            self.task_table.item(found_row, 0).setBackground(QColor("#fff3e0"))
            self.task_table.item(found_row, 0).setForeground(QColor("#ef6c00"))
            
        elif status == "error":
            self.task_table.setItem(found_row, 0, QTableWidgetItem("Error"))
            self.task_table.item(found_row, 0).setBackground(QColor("#ffebee"))
            self.task_table.item(found_row, 0).setForeground(QColor("red"))
            
        # Update title and date
        self.task_table.setItem(found_row, 2, QTableWidgetItem(title))
        self.task_table.setItem(found_row, 3, QTableWidgetItem(date_str))

    def open_task_folder(self, url):
        # Find row by url
        rows = self.task_table.rowCount()
        for r in range(rows):
            item = self.task_table.item(r, 1)
            if item.text() == url:
                local_path = item.data(Qt.ItemDataRole.UserRole)
                if local_path and os.path.exists(local_path):
                    try:
                        QDesktopServices.openUrl(QUrl.fromLocalFile(local_path))
                    except Exception as e:
                        print(f"Error opening folder: {e}")
                else:
                    # Try to guess path if not stored (fallback)
                    # But actually we don't have enough info here to guess accurately without title/date
                    # So we just warn
                    pass
                break

    def add_image_to_gallery(self, url, path):
        # Insert at top (reverse order)
        self.image_list.insertRow(0)
        self.image_list.setItem(0, 0, QTableWidgetItem(os.path.basename(path)))
        self.image_list.setItem(0, 1, QTableWidgetItem(path))
        
        # Set custom row number (cumulative count)
        self.total_images_count += 1
        self.image_list.setVerticalHeaderItem(0, QTableWidgetItem(str(self.total_images_count)))
        
        # Auto preview last (which is now first)
        self.show_preview(path)

    def on_image_selected(self, item):
        row = item.row()
        path = self.image_list.item(row, 1).text()
        self.show_preview(path)

    def show_preview(self, path):
        if os.path.exists(path):
            pixmap = QPixmap(path)
            self.preview_widget.set_pixmap(pixmap, path)
        else:
            self.preview_widget.set_pixmap(None)

    def parse_resolution(self):
        text = self.res_combo.currentText()
        if text == "不限制": return (0, 0)
        if text == "自定义": return (0, 0) # Handle specifically?
        
        try:
            parts = text.lower().split('x')
            if len(parts) == 2:
                return (int(parts[0]), int(parts[1]))
        except:
            pass
        return (0, 0)

    def save_config(self):
        data = {
            'url': self.url_input.text(),
            'save_dir': self.path_input.text(),
            'naming': self.naming_input.text(),
            'res': self.res_combo.currentText(),
            'formats': [f for f, cb in self.fmt_checks.items() if cb.isChecked()],
            'page_delay': (self.page_delay_min.value(), self.page_delay_max.value()),
            'img_delay': (self.img_delay_min.value(), self.img_delay_max.value()),
            'last_success_url': self.last_success_url
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(data, f)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
