import time
from rich.console import Console
from rich.prompt import Confirm
from rich.panel import Panel
from rich.table import Table
from rich.align import Align


# 定義常數
PANEL_WIDTH = 100

console = Console(force_terminal=True, color_system="auto", width=PANEL_WIDTH)  # 使用您想要的寬度


def rich_print(message: str | Table,
               width: int = PANEL_WIDTH,
               confirm: bool = False,
               title: str = None) -> bool | None:
    """統一的訊息顯示函數

    Parameters
    ----------
    message : str | Table
        要顯示的訊息或表格
    width : int
        面板寬度
    confirm : bool
        是否需要確認
    title : str
        面板標題，可選
    """
    if confirm:
        return Confirm.ask(
            f"[bold cyan]{message}[/bold cyan]",
            default=True,
            show_default=True
        )

    # 如果輸入是表格，則使用不同的格式化方式
    if isinstance(message, Table):
        content = Align.center(message)
    else:
        content = Align.center(f"[bold cyan]{message}[/bold cyan]")

    console.print(Panel(
        content,
        title=title,
        width=width,
        expand=True,
        border_style="bright_blue",
        padding=(0, 0)
    ))


class DisplayManager:
    def __init__(self):
        self.console = console
        self.panel_width = PANEL_WIDTH
        self.panel_style = "bright_blue"
        self.panel_padding = (1, 0)

    def create_centered_panel(self, content, title, subtitle=None):
        """創建置中的面板"""
        centered_content = Align.center(content)
        return Panel(
            centered_content,
            title=title,
            width=self.panel_width,
            expand=True,
            border_style=self.panel_style,
            padding=self.panel_padding,
            subtitle=subtitle
        )

    def display_products(self, products):
        """顯示產品資訊表格"""
        table = Table(title="Product Information")

        # 設定欄位
        columns = [
            ("No.", "right", "cyan"),
            ("Time", "left", "magenta"),
            ("Name", "left", "blue"),
            ("Size", "right", "green")
        ]

        for file_name, justify, style in columns:
            table.add_column(file_name, justify=justify, style=style)

        # 添加資料行
        for i, product in enumerate(products, 1):
            time_str = product.get('ContentDate', {}).get('Start', 'N/A')[:19]
            file_name = product.get('Name', 'N/A')
            size = product.get('ContentLength', 0)
            size_str = f"{size / 1024 / 1024:.2f} MB"

            # 處理過長的名稱
            name_short_cut = f"{file_name[:35]}...{file_name[-15:]}" if len(file_name) > 53 else file_name

            table.add_row(str(i), time_str, name_short_cut, size_str)

        # 顯示面板
        panel = self.create_centered_panel(table, f"Found {len(products)} Products")
        self.console.print(panel)

    def display_download_summary(self, stats):
        """顯示下載統計摘要"""
        table = Table(title="Download Summary", width=60, padding=(0, 1), expand=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        # 計算基本統計
        total_files = sum(stats[key] for key in ['success', 'failed', 'skipped'])
        elapsed_time = time.time() - stats['start_time']

        # 準備顯示資料
        metrics = [
            ("Total Files", str(total_files)),
            ("Successfully Downloaded", str(stats['success'])),
            ("Failed Downloads", str(stats['failed'])),
            ("Skipped Files", str(stats['skipped'])),
            ("Total Size", f"{stats['total_size'] / 1024 / 1024:.2f} MB"),
            ("Actual Download Size", f"{stats['actual_download_size'] / 1024 / 1024:.2f} MB"),
            ("Spend Time", f"{elapsed_time:.2f}s")
        ]

        # 如果有經過時間，添加速度資訊
        if elapsed_time > 0:
            avg_speed = stats['actual_download_size'] / elapsed_time
            metrics.append(("Average Speed", f"{avg_speed / 1024 / 1024:.2f} MB/s"))

        # 添加所有指標到表格
        for metric, value in metrics:
            table.add_row(metric, value)

        # 顯示面板
        panel = self.create_centered_panel(table, "Download Results")
        self.console.print(panel)

    def display_product_info(self, nc_info):
        """顯示下載統計摘要"""
        table = Table(title="Information", width=40, padding=(0, 1), expand=False)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right", style="green")

        # 準備顯示資料
        metrics = [
            ("Data Time", str(nc_info['time'])),
            ("Data Shape", str(nc_info['shape'])),
            ("Latitude", str(nc_info['latitude'])),
            ("Longitude", str(nc_info['longitude'])),
        ]

        # 添加所有指標到表格
        for metric, value in metrics:
            table.add_row(metric, value)

        # 顯示面板
        file_name_short_cut = f"{nc_info['file_name'][:35]}...{nc_info['file_name'][-15:]}"
        panel = self.create_centered_panel(table, f"Processing: {file_name_short_cut}", "繪製插值後的數據圖...")
        self.console.print("\n", panel)