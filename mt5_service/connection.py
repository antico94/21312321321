# mt5_service/connection.py
import MetaTrader5 as mt5
from datetime import datetime
import pytz
import time
from container import container
from config.app_config import AppConfig, MT5Config
from log_service.logger import LoggingService


class MT5ConnectionService:
    """Service for managing the connection to MetaTrader5"""

    def __init__(self):
        """Initialize the MT5 connection service"""
        self._initialized = False
        self._config = None
        self._logging_service = None

    def initialize(self) -> bool:
        """
        Initialize the connection to MetaTrader5

        Returns:
            bool: True if initialization is successful, False otherwise

        Raises:
            Exception: If initialization fails
        """
        if self._initialized:
            return True

        try:
            # Get dependencies from container
            self._logging_service = container.resolve(LoggingService)
            app_config = container.resolve(AppConfig)
            self._config = app_config.mt5

            # Initialize logging
            if not self._logging_service:
                raise Exception("Logging service not available")

            # Log initialization attempt
            self._logging_service.log('INFO', 'mt5_connection',
                                      f"Initializing connection to MT5 server: {self._config.server}")

            # Initialize MT5
            initialized = mt5.initialize()
            if not initialized:
                error = mt5.last_error()
                raise Exception(f"Failed to initialize MT5: {error}")

            # Log MT5 version
            version_info = mt5.version()
            if version_info:
                version = f"{version_info[0]}.{version_info[1]}.{version_info[2]}"
                self._logging_service.log('INFO', 'mt5_connection', f"MT5 version: {version}")

            # Check terminal info
            terminal_info = mt5.terminal_info()
            if not terminal_info:
                raise Exception("Failed to get terminal info")

            # Login to MT5
            login_result = mt5.login(
                login=self._config.login,
                password=self._config.password,
                server=self._config.server,
                timeout=self._config.timeout
            )

            if not login_result:
                error = mt5.last_error()
                raise Exception(f"Failed to login to MT5: {error}")

            # Log successful login
            account_info = mt5.account_info()
            if account_info:
                self._logging_service.log(
                    'INFO',
                    'mt5_connection',
                    f"Logged in to MT5 account {account_info.login} ({account_info.name})"
                )

            self._initialized = True
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'mt5_connection', f"MT5 initialization error: {str(e)}")
            self.shutdown()
            raise

    def shutdown(self) -> None:
        """
        Shutdown the connection to MT5
        """
        if self._initialized:
            try:
                mt5.shutdown()
                self._logging_service.log('INFO', 'mt5_connection', "MT5 connection shutdown")
            except Exception as e:
                self._logging_service.log('ERROR', 'mt5_connection', f"MT5 shutdown error: {str(e)}")
            finally:
                self._initialized = False

    def is_connected(self) -> bool:
        """
        Check if the MT5 connection is established

        Returns:
            bool: True if connected, False otherwise
        """
        if not self._initialized:
            return False

        return mt5.terminal_info() is not None

    def ensure_connection(self) -> bool:
        """
        Ensure that the connection to MT5 is established

        Returns:
            bool: True if connection is established, False otherwise
        """
        if self.is_connected():
            return True

        try:
            return self.initialize()
        except Exception as e:
            self._logging_service.log('ERROR', 'mt5_connection', f"Failed to ensure connection: {str(e)}")
            return False


# Create an instance and register in container
mt5_connection_service = MT5ConnectionService()
container.register(MT5ConnectionService, mt5_connection_service)