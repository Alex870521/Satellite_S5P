import logging
from zoneinfo import ZoneInfo

from rich.console import Console
from rich.logging import RichHandler
from abc import ABC, abstractmethod
from datetime import datetime
from src.config.settings import BASE_DIR, REGIONS


class SatelliteHub(ABC):
    name = 'SatelliteHub'

    def __init__(self, base_dir=BASE_DIR, region='taiwan'):
        # Set up directory structure
        self.base_dir = base_dir
        if region not in REGIONS:
            raise ValueError(f"Unknown region '{region}'. Known: {sorted(REGIONS)}")
        # The geographic crop region. 'taiwan' keeps the historical layout
        # (processed/); any other region writes to processed_<region>/ and uses
        # that region's GridFrame bounds (resolution is left untouched).
        self.region = region
        self.region_bounds = REGIONS[region]
        self._setup_common_dirs()

        # Set up logging
        self.logger = self._setup_logger()

        # Initialize client
        self.client = self.authentication()

    def _setup_logger(self):
        """Set up logger with rich color support"""

        # Create a custom Console object with larger width limit
        console = Console(width=180, highlight=False)  # Set wider console width

        # Use the class name attribute as the logger identifier
        logger_name = self.name

        # Create logger
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        # Clear existing handlers
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)

        # Create rich console handler
        console_handler = RichHandler(
            console=console,  # Use custom Console
            rich_tracebacks=True,
            markup=True,  # Enable markup parsing!
            show_level=True,
            show_path=False,
            enable_link_path=False,  # Disable path links to save space
            omit_repeated_times=True,  # Reduce repeated timestamps
            log_time_format="[%Y-%m-%d %H:%M:%S]",  # Custom time format
        )
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)

        # Add file handler（logs 一定會用到 → 在這裡才建立,而非 _setup_common_dirs 預建）
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(self.logs_dir / f"{logger_name}_{datetime.now().strftime('%Y%m')}.log")
        file_handler.setLevel(logging.DEBUG)

        # File uses plain text format (without rich markup)
        file_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        # Allow logs to propagate to parent (changed from False to True)
        logger.propagate = True

        return logger

    def _setup_common_dirs(self):
        """Set up basic directory structure common to all satellite APIs"""
        # Create base directory with API name
        self.main_dir = self.base_dir / self.name

        # Region-specific OUTPUTS get a _<region> suffix so the Taiwan archive and
        # wider crops coexist (web auto-detects each processed_* folder). Taiwan keeps
        # the historical bare names. raw/ is shared — the same granules feed all regions.
        def _r(base):
            return base if self.region == "taiwan" else f"{base}_{self.region}"

        # 只設定路徑屬性,不在這裡建立資料夾 —— 每個流程(download/process/figure/
        # geotiff)在實際寫入時才 mkdir(各寫入點都帶 parents=True)。這樣沒跑到的
        # 流程就不會留下空資料夾。logs 例外:logger 一啟動就會寫,於 _setup_logger 建。
        self.logs_dir = self.main_dir / "logs"
        self.raw_dir = self.main_dir / "raw"   # region-independent downloads
        self.processed_dir = self.main_dir / _r("processed")
        self.figure_dir = self.main_dir / _r("figure")

        if self.name == 'Sentinel-5P':
            self.geotiff_dir = self.main_dir / _r("geotiff")

    @abstractmethod
    def authentication(self):
        """Authenticate and return API client, must be implemented by subclasses"""
        pass

    @abstractmethod
    def fetch_data(self, **kwargs):
        """Fetch satellite data, must be implemented by subclasses"""
        pass

    @abstractmethod
    def download_data(self, **kwargs):
        """Generic method for downloading data, subclasses can override if needed"""
        pass  # Default implementation or can provide some common logic

    @abstractmethod
    def process_data(self, **kwargs):
        """Process downloaded data"""
        pass

    def run_pipeline(self, **kwargs):
        """One-call pipeline: fetch_data -> download_data -> process_data.

        This is the unified entry point shared by every hub. All keyword args are
        forwarded to ``fetch_data`` (each provider's query params differ). Hubs
        with a non-standard flow override this:
          - GEMSHub: streaming download->grid->delete (server-side area extract).
          - ERA5Hub: fetch stores the query internally, download() takes no
            products, and process_data needs ``stations``.

        Returns whatever ``fetch_data`` returned (product list), for inspection.
        """
        products = self.fetch_data(**kwargs)
        if not products:
            self.logger.warning("No products fetched — nothing to download or process.")
            return products
        self.download_data(products)
        self.process_data()
        return products

    def plot(self):
        """Plot data"""
        pass

    def animation(self):
        """Create animation"""
        pass

    def _setup_timezone(self, timezone):
        """Sets up timezone and calculates offset"""
        try:
            # Use provided timezone or attempt to get system timezone
            self.timezone = ZoneInfo(timezone) if timezone else ZoneInfo(str(datetime.now().astimezone().tzinfo))
        except:
            # If failed, use UTC
            self.timezone = ZoneInfo('UTC')

        # Calculate timezone offset (hours)
        self.tz_offset = datetime.now(self.timezone).utcoffset().total_seconds() / 3600

    def _normalize_time_inputs(self, start_date, end_date, set_timezone=True):
        """
        Normalize time range inputs, handle different formats, add timezone information,
        and ensure dates are not in the future

        Parameters:
            start_date (str or datetime): Start date/time
            end_date (str or datetime): End date/time
            set_timezone (bool): Whether to set timezone information for dates

        Returns:
            tuple: (normalized start time, normalized end time) as datetime objects
        """
        # Step 1: Convert all inputs to datetime objects
        if isinstance(start_date, str):
            try:
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except ValueError:
                # Try more lenient format parsing
                start_date = datetime.strptime(start_date, "%Y-%m-%d")

        if isinstance(end_date, str):
            try:
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except ValueError:
                # Try more lenient format parsing
                end_date = datetime.strptime(end_date, "%Y-%m-%d")

        # Step 2: Set default time values if needed
        # For end_date: if time part is zero, assume user wants end of day (23:59:59)
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0 and end_date.microsecond == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        # Step 3: Determine timezone if needed
        tz_info = None
        if set_timezone:
            if hasattr(self, 'timezone'):
                tz_info = self.timezone
            else:
                # Try to get system timezone
                try:
                    tz_info = datetime.now().astimezone().tzinfo
                except:
                    # If unable to get system timezone, use UTC
                    from datetime import timezone
                    tz_info = timezone.utc

        # Step 4: Get current time for validation (with matching timezone setting)
        now = datetime.now()

        # Step 5: Validate dates are not in the future
        if start_date > now:
            if hasattr(self, 'logger'):
                self.logger.warning(f"Start time {start_date} is in the future, setting to current time")
            start_date = now

        if end_date > now:
            if hasattr(self, 'logger'):
                self.logger.warning(f"End time {end_date} is in the future, setting to current time")
            end_date = now

        # Step 6: Ensure start time is not later than end time
        if start_date > end_date:
            if hasattr(self, 'logger'):
                self.logger.warning(f"Start time {start_date} is later than end time {end_date}, swapping times")
            start_date, end_date = end_date, start_date

        # Step 7: Handle timezone settings last
        if set_timezone:
            # Add timezone info if missing
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=tz_info)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=tz_info)
        else:
            # Remove timezone info if present
            if start_date.tzinfo is not None:
                start_date = start_date.replace(tzinfo=None)
            if end_date.tzinfo is not None:
                end_date = end_date.replace(tzinfo=None)

        return start_date, end_date
