# mt5_service/data_sync.py
import MetaTrader5 as mt5
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from container import container
from config.app_config import AppConfig, InstrumentConfig, TimeframeConfig
from log_service.logger import LoggingService
from mt5_service.connection import MT5ConnectionService
from mt5_service.data_fetcher import MT5DataFetcher, TimeframeMapping
from database.models import PriceBar


class MT5DataSyncService:
    """Service for synchronizing price data from MT5 in real-time"""

    def __init__(self):
        """Initialize the MT5 data sync service"""
        self._initialized = False
        self._connection_service = None
        self._data_fetcher = None
        self._logging_service = None
        self._app_config = None
        self._running = False
        self._sync_thread = None
        self._last_sync_times = {}  # {symbol: {timeframe: last_sync_time}}

    def initialize(self) -> bool:
        """
        Initialize the data sync service

        Returns:
            bool: True if initialization is successful, False otherwise

        Raises:
            Exception: If initialization fails
        """
        if self._initialized:
            return True

        try:
            # Get dependencies from container
            self._connection_service = container.resolve(MT5ConnectionService)
            self._data_fetcher = container.resolve(MT5DataFetcher)
            self._logging_service = container.resolve(LoggingService)
            self._app_config = container.resolve(AppConfig)

            # Ensure MT5 connection
            if not self._connection_service.ensure_connection():
                raise Exception("Failed to establish MT5 connection")

            # Ensure data fetcher is initialized
            if not self._data_fetcher._initialized:
                self._data_fetcher.initialize()

            # Fetch initial data
            self._data_fetcher.fetch_initial_data()

            # Initialize last sync times
            self._initialize_last_sync_times()

            self._initialized = True
            self._logging_service.log('INFO', 'data_sync', "MT5 data sync service initialized")
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'data_sync', f"MT5 data sync service initialization error: {str(e)}")
            raise

    def _initialize_last_sync_times(self) -> None:
        """
        Initialize the last sync times for all instruments and timeframes
        """
        try:
            # Get trading config
            trading_config = self._app_config.trading

            # Loop through configured instruments
            for symbol, instrument_config in trading_config.instruments.items():
                self._last_sync_times[symbol] = {}

                # Loop through configured timeframes
                for tf_name, _ in instrument_config.timeframes.items():
                    # Initialize last sync time to now
                    self._last_sync_times[symbol][tf_name] = datetime.now()

            self._logging_service.log(
                'INFO',
                'data_sync',
                f"Initialized last sync times for {len(self._last_sync_times)} instruments"
            )

        except Exception as e:
            self._logging_service.log('ERROR', 'data_sync', f"Failed to initialize last sync times: {str(e)}")
            raise

    def start(self) -> None:
        """
        Start the data sync service

        Raises:
            Exception: If starting fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # If already running, do nothing
            if self._running:
                self._logging_service.log('INFO', 'data_sync', "Data sync service is already running")
                return

            # Set running flag
            self._running = True

            # Create and start the sync thread
            self._sync_thread = threading.Thread(target=self._sync_loop)
            self._sync_thread.daemon = True
            self._sync_thread.start()

            self._logging_service.log('INFO', 'data_sync', "Data sync service started")

        except Exception as e:
            self._running = False
            self._logging_service.log('ERROR', 'data_sync', f"Failed to start data sync service: {str(e)}")
            raise

    def stop(self) -> None:
        """
        Stop the data sync service
        """
        if self._running:
            self._running = False

            # Wait for the sync thread to finish
            if self._sync_thread and self._sync_thread.is_alive():
                self._sync_thread.join(timeout=5.0)

            self._logging_service.log('INFO', 'data_sync', "Data sync service stopped")

    def _sync_loop(self) -> None:
        """
        Main sync loop that runs in a separate thread
        """
        try:
            sync_config = self._app_config.trading.sync_config

            while self._running:
                try:
                    # Ensure MT5 connection
                    if not self._connection_service.ensure_connection():
                        self._logging_service.log('WARNING', 'data_sync', "MT5 connection lost, reconnecting...")
                        time.sleep(sync_config.retry_delay_seconds)
                        continue

                    # Sync price data for all instruments and timeframes
                    self._sync_price_data()

                    # Sleep until next sync
                    time.sleep(sync_config.interval_seconds)

                except Exception as e:
                    self._logging_service.log('ERROR', 'data_sync', f"Error in sync loop: {str(e)}")
                    time.sleep(sync_config.retry_delay_seconds)

        except Exception as e:
            self._logging_service.log('ERROR', 'data_sync', f"Sync loop terminated with error: {str(e)}")
            self._running = False

    def _sync_price_data(self) -> None:
        """
        Sync price data for all instruments and timeframes
        """
        # Get trading config
        trading_config = self._app_config.trading

        # Loop through configured instruments
        for symbol, instrument_config in trading_config.instruments.items():
            # Loop through configured timeframes
            for tf_name, tf_config in instrument_config.timeframes.items():
                try:
                    # Get the latest bar
                    latest_bar = self._data_fetcher.fetch_latest_bar(symbol, tf_name)
                    if latest_bar is None:
                        continue

                    # Check if this is a new bar
                    last_sync_time = self._last_sync_times[symbol][tf_name]
                    if latest_bar.timestamp > last_sync_time:
                        # Store the bar in the database
                        self._store_new_bar(latest_bar)

                        # Update last sync time
                        self._last_sync_times[symbol][tf_name] = latest_bar.timestamp

                        self._logging_service.log(
                            'INFO',
                            'data_sync',
                            f"New {tf_name} bar for {symbol} at {latest_bar.timestamp}"
                        )

                except Exception as e:
                    self._logging_service.log(
                        'ERROR',
                        'data_sync',
                        f"Failed to sync {tf_name} data for {symbol}: {str(e)}"
                    )

    def _store_new_bar(self, price_bar: PriceBar) -> None:
        """
        Store a new price bar in the database

        Args:
            price_bar: The price bar to store

        Raises:
            Exception: If storing fails
        """
        try:
            # Add the bar to the database
            self._data_fetcher._db_session.add(price_bar)
            self._data_fetcher._db_session.commit()

        except Exception as e:
            self._data_fetcher._db_session.rollback()
            self._logging_service.log('ERROR', 'data_sync', f"Failed to store new bar: {str(e)}")
            raise


# Create an instance and register in container
mt5_data_sync_service = MT5DataSyncService()
container.register(MT5DataSyncService, mt5_data_sync_service)