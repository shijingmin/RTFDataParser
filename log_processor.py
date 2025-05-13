import logging
import queue
from logging import Handler
import datetime


class UILogHandler(Handler):
    """专用于UI的日志处理器"""

    def __init__(self, text_widget=None):
        super().__init__()
        self.text_widget = text_widget
        self.formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    def attach_ui(self, text_widget):
        """绑定UI组件"""
        self.text_widget = text_widget
        # 配置颜色标签
        self.text_widget.tag_config("DEBUG", foreground="gray")
        self.text_widget.tag_config("INFO", foreground="black")
        self.text_widget.tag_config("WARNING", foreground="orange")
        self.text_widget.tag_config("ERROR", foreground="red")
        self.text_widget.tag_config("CRITICAL", foreground="white", background="red")

    def emit(self, record):
        """输出日志到UI"""
        if self.text_widget:
            msg = self.format(record)
            self.text_widget.configure(state="normal")
            self.text_widget.insert("end", msg + "\n", record.levelname)
            self.text_widget.configure(state="disabled")
            self.text_widget.see("end")


class LogManager:
    """日志管理器（单例模式）"""
    _instance = None


    def __init__(self):
        # 添加日志队列
        self.log_queue = queue.Queue()
        self._init_logger()

    def get_log_queue(self):
        return self.log_queue

    # 修改日志记录方式
    def log(self, level, message):
        """线程安全的日志记录"""
        self.log_queue.put((level, message))

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._init_logger()
        return cls._instance

    def _init_logger(self):
        """初始化日志系统"""
        self.logger = logging.getLogger("AppLogger")
        self.logger.setLevel(logging.DEBUG)

        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)

        # UI处理器延迟绑定
        self.ui_handler = UILogHandler()
        self.logger.addHandler(self.ui_handler)

    def bind_ui(self, text_widget):
        """绑定UI组件"""
        self.ui_handler.attach_ui(text_widget)

    def get_logger(self):
        """获取日志记录器"""
        return self.logger

    def set_log_level(self, level):
        """设置日志级别"""
        level = getattr(logging, level.upper(), logging.INFO)
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
        self.get_logger().debug(f"日志级别设置为：{logging.getLevelName(level)}")