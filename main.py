import queue
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from log_processor import LogManager
from rtf_parser import RTFParser


class AppUI:
    def __init__(self, root):
        self.root = root
        self.logger = LogManager()
        self.root.bind("<<TaskDone>>", self.on_task_done)
        self.setup_ui()
        self.setup_logging()

    def setup_ui(self):
        """界面布局"""
        self.root.title("RTF解析工具")
        self.root.geometry("800x600")

        # 主布局使用grid管理
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)  # 日志区域可扩展

        # 第一行：目录选择部分
        # 第一行：目录选择和操作按钮
        top_frame = ttk.Frame(self.root)
        top_frame.grid(row=0, column=0, padx=10, pady=5, sticky="ew")

        # 目录输入框（左侧）
        self.dir_entry = ttk.Entry(top_frame)
        self.dir_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))

        # 按钮组（右侧）
        btn_group = ttk.Frame(top_frame)
        btn_group.pack(side="right")

        # 选择目录按钮
        ttk.Button(
            btn_group,
            text="选择目录",
            command=self.select_directory,
            width=10
        ).pack(side="left", padx=2)

        # 开始解析按钮
        self.parse_btn = ttk.Button(
            btn_group,
            text="开始解析",
            command=self.start_parsing,
            width=10
        )
        self.parse_btn.pack(side="left", padx=5)

        # 第二行：控制面板
        control_frame = ttk.Frame(self.root)
        control_frame.grid(row=2, column=0, padx=10, pady=5, sticky="w")

        # 日志级别选择器
        self.log_level = tk.StringVar(value="INFO")
        level_combo = ttk.Combobox(
            control_frame,
            textvariable=self.log_level,
            values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            width=8,
            state="readonly"
        )
        level_combo.pack(side="left", padx=5)
        level_combo.bind("<<ComboboxSelected>>", self.update_log_level)

        ttk.Button(
            control_frame,
            text="清空日志",
            command=self.clear_logs
        ).pack(side="left", padx=5)
        # 第四行：日志显示区域
        log_frame = ttk.Frame(self.root)
        log_frame.grid(row=3, column=0, padx=10, pady=5, sticky="nsew")

        self.log_text = tk.Text(log_frame, wrap="word")
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self.log_text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.root.after(100, self.process_log_queue)

    # 新增方法
    def select_directory(self):
        """选择目录"""
        directory = filedialog.askdirectory()
        if directory:
            self.dir_entry.delete(0, tk.END)
            self.dir_entry.insert(0, directory)
            self.logger.get_logger().info(f"已选择目录：{directory}")

    def start_parsing(self):
        """启动解析"""
        directory = self.dir_entry.get()
        if not directory:
            messagebox.showwarning("提示", "请先选择目录")
            return

        # 禁用按钮防止重复点击
        self.parse_btn.configure(state="disabled")
        self.root.title("RTF解析工具 - 运行中...")

        # 创建后台任务线程
        threading.Thread(
            target=self.run_parsing_task,
            args=(directory,),
            daemon=True
        ).start()

    def setup_logging(self):
        """绑定日志到UI"""
        self.logger.bind_ui(self.log_text)
        self.logger.set_log_level(self.log_level.get())

    def update_log_level(self, event=None):
        """更新日志级别"""
        new_level = self.log_level.get()
        self.logger.set_log_level(new_level)
        self.logger.get_logger().info(f"日志级别已变更为：{new_level}")

    def clear_logs(self):
        """清空日志内容"""
        self.log_text.configure(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state="disabled")
        self.logger.get_logger().info("日志已清空")


    def process_log_queue(self):
        """处理日志队列"""
        try:
            while True:
                # 非阻塞获取日志
                level, msg = self.logger.log_queue.get_nowait()
                logger = self.logger.get_logger()
                getattr(logger, level.lower())(msg)
        except queue.Empty:
            pass
        finally:
            # 每100ms检查一次
            self.root.after(100, self.process_log_queue)


    def run_parsing_task(self, directory):
        """后台任务线程执行的方法"""
        """后台任务线程执行的方法"""
        try:
            # 初始化解析器时传递停止事件
            self.parser = RTFParser(
                log_queue=self.logger.get_log_queue(),
                stop_event=threading.Event()
            )
            self.parser.process_files(directory)
        except Exception as e:
            self.logger.log("ERROR", f"任务异常终止: {str(e)}")
        finally:
            # 确保事件必定触发
            self.root.event_generate("<<TaskDone>>", when="tail")
            self.logger.log("DEBUG", "已发送任务完成信号")

    def on_task_done(self, event):
        """任务完成回调（修复版本）"""
        try:
            # 确保按钮存在且未被销毁
            if self.parse_btn.winfo_exists():
                self.parse_btn.configure(state="normal")
            else:
                print("按钮组件已销毁，无法恢复状态")

            # 强制刷新界面
            self.root.update_idletasks()
            self.root.title("RTF解析工具")
            self.logger.log("INFO", "界面状态已恢复，可以解析下一组文件")

        except Exception as e:
            self.logger.log("ERROR", f"状态恢复失败: {str(e)}")
            print(f"Critical Error: {str(e)}")

    def force_stop(self):
        """强制停止任务"""
        if hasattr(self, 'parser'):
            self.parser._stop_event.set()  # 使用传入的事件对象
        self.on_task_done(None)

if __name__ == "__main__":
    root = tk.Tk()
    app = AppUI(root)
    root.mainloop()