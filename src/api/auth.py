"""Copernicus API 認證處理"""
import os
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path

from src.config.settings import COPERNICUS_TOKEN_URL

logger = logging.getLogger(__name__)


class CopernicusAuth:
    def __init__(self):
        if not os.getenv('COPERNICUS_USERNAME') or not os.getenv('COPERNICUS_PASSWORD'):
            raise EnvironmentError(
                "Missing COPERNICUS credentials. Please set COPERNICUS_USERNAME and COPERNICUS_PASSWORD environment variables"
            )

        self.username = os.getenv('COPERNICUS_USERNAME')
        self.password = os.getenv('COPERNICUS_PASSWORD')
        self.token = None
        self.token_expiry = None

    def get_token(self):
        """獲取新的 access token"""
        try:
            data = {
                'grant_type': 'password',
                'username': self.username,
                'password': self.password,
                'client_id': 'cdse-public'
            }

            response = requests.post(COPERNICUS_TOKEN_URL, data=data, timeout=30)
            response.raise_for_status()

            token_data = response.json()
            self.token = token_data['access_token']
            self.token_expiry = datetime.now() + timedelta(seconds=token_data['expires_in'] - 60)
            # logger.info("Access token updated")
            return self.token
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting token: {e}")
            raise

    def ensure_valid_token(self):
        """確保 token 有效"""
        if not self.token or not self.token_expiry or datetime.now() >= self.token_expiry:
            return self.get_token()
        return self.token
