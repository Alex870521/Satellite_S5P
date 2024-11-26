import logging
import tkinter as tk
from tkinter import messagebox, ttk
from tkcalendar import DateEntry
import threading

# 導入你的處理模組
from src.api.sentinel_api import S5PFetcher
from src.config import setup_directory_structure


class SatelliteApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("衛星數據處理器")
        self.root.geometry("800x900")

        # 設置日誌
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.create_gui()

    def create_gui(self):
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # ===== 認證區域 =====
        auth_frame = ttk.LabelFrame(main_frame, text="認證", padding="10")
        auth_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(auth_frame, text="帳號:").grid(row=0, column=0, padx=5, pady=5)
        self.username = ttk.Entry(auth_frame)
        self.username.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(auth_frame, text="密碼:").grid(row=1, column=0, padx=5, pady=5)
        self.password = ttk.Entry(auth_frame, show="*")
        self.password.grid(row=1, column=1, padx=5, pady=5)

        # ===== 日期選擇區域 =====
        date_frame = ttk.LabelFrame(main_frame, text="日期範圍", padding="10")
        date_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(date_frame, text="開始日期:").grid(row=0, column=0, padx=5, pady=5)
        self.start_date = DateEntry(date_frame, width=12)
        self.start_date.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(date_frame, text="結束日期:").grid(row=1, column=0, padx=5, pady=5)
        self.end_date = DateEntry(date_frame, width=12)
        self.end_date.grid(row=1, column=1, padx=5, pady=5)

        # ===== 數據模式選擇 =====
        mode_frame = ttk.LabelFrame(main_frame, text="數據模式", padding="10")
        mode_frame.pack(fill=tk.X, pady=(0, 10))

        self.data_mode = tk.StringVar(value="all")
        ttk.Radiobutton(mode_frame, text="即時數據", value="realtime", variable=self.data_mode).pack(anchor=tk.W)
        ttk.Radiobutton(mode_frame, text="離線數據", value="offline", variable=self.data_mode).pack(anchor=tk.W)
        ttk.Radiobutton(mode_frame, text="全部數據", value="all", variable=self.data_mode).pack(anchor=tk.W)

        # ===== 數據類型選擇 =====
        type_frame = ttk.LabelFrame(main_frame, text="數據類型", padding="10")
        type_frame.pack(fill=tk.X, pady=(0, 10))

        self.data_types = {
            'aerosol': 'Aerosol Index',
            'co': 'Carbon Monoxide (CO)',
            'cloud': 'Cloud',
            'hcho': 'Formaldehyde (HCHO)',
            'ch4': 'Methane (CH4)',
            'no2': 'Nitrogen Dioxide (NO2)',
            'o3': 'Ozone (O3)',
            'so2': 'Sulfur Dioxide (SO2)'
        }

        self.selected_types = {}
        for key, value in self.data_types.items():
            var = tk.BooleanVar()
            self.selected_types[key] = var
            ttk.Checkbutton(type_frame, text=value, variable=var).pack(anchor=tk.W)

        # ===== 執行按鈕 =====
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=10)

        self.status_label = ttk.Label(btn_frame, text="就緒")
        self.status_label.pack(side=tk.LEFT)

        self.run_button = ttk.Button(btn_frame, text="開始處理", command=self.start_processing)
        self.run_button.pack(side=tk.RIGHT)

        # ===== 日誌區域 =====
        log_frame = ttk.LabelFrame(main_frame, text="處理日誌", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, height=15, width=70)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def log_message(self, message):
        """添加日誌消息"""
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.root.update()

    def start_processing(self):
        """開始處理數據"""
        # 驗證輸入
        if not self.username.get() or not self.password.get():
            messagebox.showerror("錯誤", "請輸入帳號和密碼")
            return

        selected_data = [key for key, var in self.selected_types.items() if var.get()]
        if not selected_data:
            messagebox.showerror("錯誤", "請至少選擇一種數據類型")
            return

        # 禁用按鈕
        self.run_button.configure(state='disabled')
        self.status_label.configure(text="處理中...")

        # 在新線程中執行處理
        thread = threading.Thread(target=self.process_data, args=(selected_data,))
        thread.daemon = True
        thread.start()

    def process_data(self, selected_data):
        """處理數據的主要邏輯"""
        try:
            start_str = self.start_date.get_date().strftime('%Y-%m-%d')
            end_str = self.end_date.get_date().strftime('%Y-%m-%d')
            data_mode = self.data_mode.get()

            self.log_message(f"開始處理數據：{start_str} 到 {end_str}")
            setup_directory_structure(start_str, end_str)

            fetcher = S5PFetcher(max_workers=3)

            for data_type in selected_data:
                self.log_message(f"處理 {self.data_types[data_type]}...")

                try:
                    fetch_method = getattr(fetcher, f'fetch_{data_type}_data')
                    products = fetch_method(
                        start_date=start_str,
                        end_date=end_str,
                        boundary=(118, 124, 20, 27),
                        limit=None,
                        data_mode=data_mode
                    )

                    if products:
                        self.log_message(f"找到 {len(products)} 個數據文件")
                        self.log_message("開始下載數據...")
                        fetcher.parallel_download(products)
                        self.log_message("數據下載完成")

                        self.log_message("開始處理數據...")
                        processor_class = globals()[f"{data_type.upper()}Processor"]
                        processor = processor_class(
                            interpolation_method='kdtree',
                            resolution=0.02,
                            mask_value=0.5
                        )
                        processor.process_each_data(
                            start_str,
                            end_str,
                            use_taiwan_mask=False,
                            file_class=data_mode
                        )
                        self.log_message("數據處理完成")
                    else:
                        self.log_message(f"找不到符合條件的 {self.data_types[data_type]} 數據")

                except Exception as e:
                    self.log_message(f"處理 {data_type} 時發生錯誤: {str(e)}")
                    continue

            self.root.after(0, lambda: messagebox.showinfo("完成", "所有數據處理完成！"))

        except Exception as e:
            self.log_message(f"錯誤: {str(e)}")
            self.root.after(0, lambda: messagebox.showerror("錯誤", str(e)))

        finally:
            self.root.after(0, lambda: self.status_label.configure(text="就緒"))
            self.root.after(0, lambda: self.run_button.configure(state='normal'))

    def run(self):
        """運行應用程式"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.root.mainloop()

    def on_closing(self):
        """關閉應用程式"""
        if messagebox.askokcancel("確認", "確定要關閉程式嗎？"):
            self.root.destroy()


if __name__ == "__main__":
    app = SatelliteApp()
    app.run()