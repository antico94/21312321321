# mt5_service/data_sync.py
import MetaTrader5 as mt5
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable

from sqlalchemy import and_

from container import container
from config.app_config import AppConfig, InstrumentConfig, TimeframeConfig
from log_service.logger import LoggingService
from mt5_service.connection import MT5ConnectionService
from mt5_service.data_fetcher import MT5DataFetcher, TimeframeMapping
from database.models import PriceBar, Instrument, Timeframe


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
        self._bar_listeners = []  # List of functions to call when a new bar is detected

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

    def add_bar_listener(self, listener: callable) -> None:
        """
        Add a listener for new bar events

        Args:
            listener: Function to call when a new bar is detected
                      Should accept (symbol, timeframe, bar) parameters
        """
        # Make sure we're initialized first
        if not self._initialized:
            self.initialize()

        self._bar_listeners.append(listener)

        if self._logging_service:  # Check that the logging service exists
            self._logging_service.log('INFO', 'data_sync',
                                      f"Added bar listener, total listeners: {len(self._bar_listeners)}")

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
                    # Get MT5 timeframe constant
                    mt5_timeframe = TimeframeMapping.NAME_TO_MT5.get(tf_name)
                    if mt5_timeframe is None:
                        self._logging_service.log('WARNING', 'data_sync', f"Unknown timeframe: {tf_name} for {symbol}")
                        continue

                    # Get the last sync time for this symbol and timeframe
                    last_sync_time = self._last_sync_times[symbol][tf_name]

                    # Fetch only the latest bar for this specific timeframe
                    bars = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, 1)

                    if bars is None or len(bars) == 0:
                        continue

                    # Get the latest bar
                    latest_bar_data = bars[0]
                    latest_bar_time = datetime.fromtimestamp(latest_bar_data['time'])

                    # If this is a new bar (timestamp is different than our last sync)
                    if latest_bar_time > last_sync_time:
                        # Before processing the new bar, update the previous completed bar
                        self._update_previous_bar(symbol, tf_name, mt5_timeframe)

                        # Now process the new bar
                        latest_bar = self._convert_mt5_bar_to_price_bar(symbol, tf_name, latest_bar_data)
                        if latest_bar:
                            # Store the bar in the database
                            self._store_new_bar(latest_bar)

                            # Update last sync time
                            self._last_sync_times[symbol][tf_name] = latest_bar_time

                            self._logging_service.log(
                                'INFO',
                                'data_sync',
                                f"New {tf_name} bar for {symbol} at {latest_bar_time}"
                            )

                except Exception as e:
                    self._logging_service.log(
                        'ERROR',
                        'data_sync',
                        f"Failed to sync {tf_name} data for {symbol}: {str(e)}"
                    )

    def _store_new_bar(self, price_bar: PriceBar) -> None:
        """
        Store a new price bar in the database or update if it already exists

        Args:
            price_bar: The price bar to store

        Raises:
            Exception: If storing fails
        """
        try:
            # Check if this bar already exists in the database
            existing_bar = self._data_fetcher._db_session.query(PriceBar).filter(
                and_(
                    PriceBar.instrument_id == price_bar.instrument_id,
                    PriceBar.timeframe_id == price_bar.timeframe_id,
                    PriceBar.timestamp == price_bar.timestamp
                )
            ).first()

            if existing_bar:
                # Update the existing bar instead of inserting a new one
                existing_bar.open = price_bar.open
                existing_bar.high = price_bar.high
                existing_bar.low = price_bar.low
                existing_bar.close = price_bar.close
                existing_bar.volume = price_bar.volume
                existing_bar.spread = price_bar.spread

                self._data_fetcher._db_session.commit()

                # Get symbol and timeframe for this bar to notify listeners
                instrument = self._data_fetcher._db_session.query(Instrument).filter(
                    Instrument.id == price_bar.instrument_id
                ).first()

                tf = self._data_fetcher._db_session.query(Timeframe).filter(
                    Timeframe.id == price_bar.timeframe_id
                ).first()

                if instrument and tf:
                    symbol = instrument.symbol
                    tf_name = tf.name

                    # Notify all registered bar listeners
                    for listener in self._bar_listeners:
                        try:
                            listener(symbol, tf_name, existing_bar)
                        except Exception as e:
                            if self._logging_service:
                                self._logging_service.log(
                                    'ERROR',
                                    'data_sync',
                                    f"Error in bar listener: {str(e)}"
                                )
                            else:
                                print(f"Error in bar listener: {str(e)}")
            else:
                # Add the new bar
                self._data_fetcher._db_session.add(price_bar)
                self._data_fetcher._db_session.commit()

                # Get symbol and timeframe for this bar to notify listeners
                instrument = self._data_fetcher._db_session.query(Instrument).filter(
                    Instrument.id == price_bar.instrument_id
                ).first()

                tf = self._data_fetcher._db_session.query(Timeframe).filter(
                    Timeframe.id == price_bar.timeframe_id
                ).first()

                if instrument and tf:
                    symbol = instrument.symbol
                    tf_name = tf.name

                    # Notify all registered bar listeners
                    for listener in self._bar_listeners:
                        try:
                            listener(symbol, tf_name, price_bar)
                        except Exception as e:
                            if self._logging_service:
                                self._logging_service.log(
                                    'ERROR',
                                    'data_sync',
                                    f"Error in bar listener: {str(e)}"
                                )
                            else:
                                print(f"Error in bar listener: {str(e)}")

        except Exception as e:
            self._data_fetcher._db_session.rollback()
            if self._logging_service:
                self._logging_service.log('ERROR', 'data_sync', f"Failed to store new bar: {str(e)}")
            else:
                print(f"Failed to store new bar: {str(e)}")
            raise

    def _update_bar_data(self, symbol: str, timeframe: str, bar_data: dict) -> None:
        """
        Update an existing price bar in the database

        Args:
            symbol: The symbol for the bar
            timeframe: The timeframe for the bar
            bar_data: The bar data from MT5
        """
        try:
            # Convert time to datetime
            bar_time = datetime.fromtimestamp(bar_data['time'])

            # Get instrument and timeframe IDs
            instrument_id = self._data_fetcher._get_instrument_id(symbol)
            timeframe_id = self._data_fetcher._get_timeframe_id(timeframe)

            # Find the existing bar
            existing_bar = self._data_fetcher._db_session.query(PriceBar).filter(
                and_(
                    PriceBar.instrument_id == instrument_id,
                    PriceBar.timeframe_id == timeframe_id,
                    PriceBar.timestamp == bar_time
                )
            ).first()

            if existing_bar:
                # Update the bar with the latest data
                existing_bar.open = float(bar_data['open'])
                existing_bar.high = float(bar_data['high'])
                existing_bar.low = float(bar_data['low'])
                existing_bar.close = float(bar_data['close'])
                existing_bar.volume = float(bar_data['tick_volume'])
                existing_bar.spread = float(bar_data['spread'])

                # Commit the changes
                self._data_fetcher._db_session.commit()

        except Exception as e:
            self._data_fetcher._db_session.rollback()
            self._logging_service.log('ERROR', 'data_sync', f"Failed to update bar data: {str(e)}")

    def _convert_mt5_bar_to_price_bar(self, symbol: str, timeframe: str, bar_data: dict) -> Optional[PriceBar]:
        """
        Convert MT5 bar data to a PriceBar object

        Args:
            symbol: The symbol for the bar
            timeframe: The timeframe for the bar
            bar_data: The bar data from MT5

        Returns:
            A PriceBar object or None if conversion fails
        """
        try:
            # Convert time to datetime
            bar_time = datetime.fromtimestamp(bar_data['time'])

            # Get instrument and timeframe IDs
            instrument_id = self._data_fetcher._get_instrument_id(symbol)
            timeframe_id = self._data_fetcher._get_timeframe_id(timeframe)

            # Create a price bar object
            price_bar = PriceBar(
                instrument_id=instrument_id,
                timeframe_id=timeframe_id,
                timestamp=bar_time,
                open=float(bar_data['open']),
                high=float(bar_data['high']),
                low=float(bar_data['low']),
                close=float(bar_data['close']),
                volume=float(bar_data['tick_volume']),
                spread=float(bar_data['spread'])
            )

            return price_bar

        except Exception as e:
            self._logging_service.log('ERROR', 'data_sync', f"Failed to convert MT5 bar: {str(e)}")
            return None

    def _update_previous_bar(self, symbol: str, timeframe: str, mt5_timeframe: int) -> None:
        """
        Update the previous completed bar for a specific symbol and timeframe

        Args:
            symbol: The symbol to update
            timeframe: The timeframe to update
            mt5_timeframe: The MT5 timeframe constant
        """
        try:
            # Fetch only the previous bar (position 1)
            bars = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 1, 1)

            if bars is not None and len(bars) > 0:
                # Get the previous bar data
                prev_bar_data = bars[0]
                prev_bar_time = datetime.fromtimestamp(prev_bar_data['time'])

                # Update the bar in the database
                instrument_id = self._data_fetcher._get_instrument_id(symbol)
                timeframe_id = self._data_fetcher._get_timeframe_id(timeframe)

                # Find the existing bar
                existing_bar = self._data_fetcher._db_session.query(PriceBar).filter(
                    and_(
                        PriceBar.instrument_id == instrument_id,
                        PriceBar.timeframe_id == timeframe_id,
                        PriceBar.timestamp == prev_bar_time
                    )
                ).first()

                if existing_bar:
                    # Update the existing bar with the latest data
                    existing_bar.open = float(prev_bar_data['open'])
                    existing_bar.high = float(prev_bar_data['high'])
                    existing_bar.low = float(prev_bar_data['low'])
                    existing_bar.close = float(prev_bar_data['close'])
                    existing_bar.volume = float(prev_bar_data['tick_volume'])
                    existing_bar.spread = float(prev_bar_data['spread'])

                    # Commit the changes
                    self._data_fetcher._db_session.commit()

                    self._logging_service.log(
                        'INFO',
                        'data_sync',
                        f"Updated previous {timeframe} bar for {symbol} at {prev_bar_time}"
                    )

        except Exception as e:
            self._data_fetcher._db_session.rollback()
            self._logging_service.log(
                'ERROR',
                'data_sync',
                f"Failed to update previous {timeframe} bar for {symbol}: {str(e)}"
            )

# Create an instance and register in container
mt5_data_sync_service = MT5DataSyncService()
container.register(MT5DataSyncService, mt5_data_sync_service)