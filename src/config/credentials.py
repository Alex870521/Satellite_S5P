"""
API 憑證驗證器
在 pipeline 啟動前檢查所有必要的 API key 是否存在、格式正確、且可用。
"""
import os
import re
import logging
import requests
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CredentialStatus:
    """單一憑證的檢查結果"""
    service: str
    ok: bool
    message: str
    required: bool = True


@dataclass
class CredentialReport:
    """所有憑證的檢查報告"""
    results: list[CredentialStatus] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        return all(r.ok for r in self.results if r.required)

    @property
    def required_failures(self) -> list[CredentialStatus]:
        return [r for r in self.results if r.required and not r.ok]

    @property
    def optional_failures(self) -> list[CredentialStatus]:
        return [r for r in self.results if not r.required and not r.ok]

    def print_report(self):
        """印出彩色狀態摘要"""
        logger.info("=" * 60)
        logger.info("API Credential Check Report")
        logger.info("=" * 60)

        for r in self.results:
            icon = "OK" if r.ok else "FAIL"
            tag = "" if r.required else " (optional)"
            logger.info(f"  [{icon}] {r.service}{tag}: {r.message}")

        logger.info("-" * 60)

        if self.all_ok:
            logger.info("All required credentials are valid.")
        else:
            for f in self.required_failures:
                logger.error(f"  BLOCKED: {f.service} - {f.message}")
            logger.error("Pipeline cannot start. Please fix the credentials above.")

        if self.optional_failures:
            for f in self.optional_failures:
                logger.warning(f"  SKIP: {f.service} - {f.message}")

        logger.info("=" * 60)


class CredentialValidator:
    """檢查 .env 中所有 API 憑證"""

    # 需要的環境變數定義
    REQUIRED_CREDENTIALS = {
        'copernicus': {
            'env_vars': ['COPERNICUS_USERNAME', 'COPERNICUS_PASSWORD'],
            'description': 'Copernicus (Sentinel-5P)',
        },
        'earthdata': {
            'env_vars': ['EARTHDATA_USERNAME', 'EARTHDATA_PASSWORD'],
            'description': 'NASA Earthdata (MODIS)',
        },
        'cdsapi': {
            'env_vars': ['CDSAPI_URL', 'CDSAPI_KEY'],
            'description': 'CDS API (ERA5)',
        },
    }

    OPTIONAL_CREDENTIALS = {
        'gems': {
            'env_vars': ['GEMS_API_KEY'],
            'description': 'GEMS (NIER/NESC Open-API)',
        },
        'himawari': {
            'env_vars': ['HIMAWARI_USERNAME', 'HIMAWARI_PASSWORD'],
            'description': 'Himawari',
        },
    }

    def __init__(self, health_check: bool = True):
        """
        Args:
            health_check: 是否實際打 API 驗證憑證有效（較慢但更準確）
        """
        self.health_check = health_check

    def validate_all(self) -> CredentialReport:
        """驗證所有憑證，回傳報告"""
        report = CredentialReport()

        # 必要憑證
        for key, cfg in self.REQUIRED_CREDENTIALS.items():
            status = self._check_credential(key, cfg, required=True)
            report.results.append(status)

        # 可選憑證
        for key, cfg in self.OPTIONAL_CREDENTIALS.items():
            status = self._check_credential(key, cfg, required=False)
            report.results.append(status)

        return report

    def _check_credential(self, key: str, cfg: dict, required: bool) -> CredentialStatus:
        """檢查單一服務的憑證"""
        service = cfg['description']
        env_vars = cfg['env_vars']

        # 1. 檢查環境變數是否存在
        missing = [v for v in env_vars if not os.getenv(v)]
        if missing:
            return CredentialStatus(
                service=service,
                ok=False,
                message=f"Missing env vars: {', '.join(missing)}",
                required=required,
            )

        # 2. 格式驗證
        format_error = self._validate_format(key)
        if format_error:
            return CredentialStatus(
                service=service,
                ok=False,
                message=format_error,
                required=required,
            )

        # 3. 健康檢查（實際打 API）
        if self.health_check:
            health_result = self._health_check(key)
            if health_result is not None:
                return CredentialStatus(
                    service=service,
                    ok=health_result[0],
                    message=health_result[1],
                    required=required,
                )

        return CredentialStatus(
            service=service,
            ok=True,
            message="Credentials present" + (" and verified" if self.health_check else ""),
            required=required,
        )

    def _validate_format(self, key: str) -> str | None:
        """格式驗證，回傳錯誤訊息或 None"""
        if key == 'cdsapi':
            cds_url = os.getenv('CDSAPI_URL', '')
            cds_key = os.getenv('CDSAPI_KEY', '')

            if not cds_url.startswith('http'):
                return f"CDSAPI_URL format invalid: should start with http(s)://"

            # CDS key 可能是 UUID 或 uid:key 格式
            uuid_pattern = re.compile(
                r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE
            )
            uid_key_pattern = re.compile(r'^\d+:[0-9a-f-]+$')

            if not uuid_pattern.match(cds_key) and not uid_key_pattern.match(cds_key):
                return f"CDSAPI_KEY format invalid: expected UUID or uid:key format"

        if key == 'copernicus':
            username = os.getenv('COPERNICUS_USERNAME', '')
            if '@' not in username and len(username) < 3:
                return f"COPERNICUS_USERNAME looks invalid: '{username}'"

        if key == 'earthdata':
            username = os.getenv('EARTHDATA_USERNAME', '')
            if len(username) < 3:
                return f"EARTHDATA_USERNAME too short: '{username}'"

        return None

    def _health_check(self, key: str) -> tuple[bool, str] | None:
        """實際打 API 驗證憑證，回傳 (ok, message) 或 None（不支援檢查的服務）"""
        try:
            if key == 'copernicus':
                return self._check_copernicus()
            elif key == 'earthdata':
                return self._check_earthdata()
            elif key == 'cdsapi':
                return self._check_cdsapi()
        except Exception as e:
            return (False, f"Health check error: {e}")

        return None  # 不支援健康檢查的服務

    def _check_copernicus(self) -> tuple[bool, str]:
        """驗證 Copernicus 帳密能否取得 OAuth token"""
        from src.config.settings import COPERNICUS_TOKEN_URL

        data = {
            'grant_type': 'password',
            'username': os.getenv('COPERNICUS_USERNAME'),
            'password': os.getenv('COPERNICUS_PASSWORD'),
            'client_id': 'cdse-public',
        }

        resp = requests.post(COPERNICUS_TOKEN_URL, data=data, timeout=15)

        if resp.status_code == 200:
            token_data = resp.json()
            expires_in = token_data.get('expires_in', 0)
            return (True, f"Token OK (expires in {expires_in}s)")
        elif resp.status_code == 401:
            return (False, "Authentication failed: invalid username or password")
        else:
            return (False, f"Token request failed: HTTP {resp.status_code}")

    def _check_earthdata(self) -> tuple[bool, str]:
        """驗證 NASA Earthdata 帳密"""
        username = os.getenv('EARTHDATA_USERNAME')
        password = os.getenv('EARTHDATA_PASSWORD')

        # earthaccess login test via URS endpoint (must be POST; GET returns 401)
        resp = requests.post(
            'https://urs.earthdata.nasa.gov/api/users/find_or_create_token',
            auth=(username, password),
            timeout=15,
        )

        if resp.status_code == 200:
            return (True, "Earthdata login OK")
        elif resp.status_code == 401:
            return (False, "Authentication failed: invalid username or password")
        else:
            # 403 等其他狀態碼可能代表帳戶問題
            return (False, f"Earthdata check failed: HTTP {resp.status_code}")

    def _check_cdsapi(self) -> tuple[bool, str]:
        """驗證 CDS API key（輕量級檢查）"""
        cds_url = os.getenv('CDSAPI_URL', '')
        cds_key = os.getenv('CDSAPI_KEY', '')

        # CDS API 沒有專用的 health endpoint，嘗試 GET /tasks
        # 新版 CDS (beta) 與舊版 URL 不同
        try:
            if 'cds-beta' in cds_url or 'cds.climate.copernicus.eu/api' in cds_url:
                test_url = cds_url.rstrip('/') + '/tasks'
                headers = {'PRIVATE-TOKEN': cds_key}
            else:
                test_url = cds_url.rstrip('/') + '/tasks'
                headers = {}

            resp = requests.get(
                test_url,
                headers=headers,
                auth=('', cds_key) if ':' not in cds_key else tuple(cds_key.split(':', 1)),
                timeout=15,
            )

            if resp.status_code in (200, 404):
                # 200 = OK, 404 = endpoint exists but no tasks (still valid auth)
                return (True, "CDS API key valid")
            elif resp.status_code == 401:
                return (False, "CDS API key rejected: unauthorized")
            elif resp.status_code == 403:
                return (False, "CDS API key expired or revoked")
            else:
                return (True, f"CDS API reachable (HTTP {resp.status_code})")

        except requests.exceptions.ConnectionError:
            return (False, f"Cannot connect to CDS API: {cds_url}")
        except requests.exceptions.Timeout:
            return (False, "CDS API timeout (may be temporarily unavailable)")


def check_credentials(health_check: bool = True) -> CredentialReport:
    """便捷函數：驗證所有憑證並印出報告"""
    validator = CredentialValidator(health_check=health_check)
    report = validator.validate_all()
    report.print_report()
    return report
