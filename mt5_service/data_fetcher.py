# mt5_service/data_fetcher.py
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from typing import Dict, List, Optional, Tuple, Union
from sqlalchemy.orm import Session
from sqlalchemy import select, and_

from container import container
from config.app_config import AppConfig
from log_service.logger import LoggingService
from mt5_service.connection import MT5ConnectionService
from database.models import Instrument, Timeframe, PriceBar
from sqlalchemy.engine import URL


class TimeframeMapping:
    """Mapping between MT5 timeframe constants and database timeframe names"""

    # Map MT5 timeframe constants to timeframe names
    MT5_TO_NAME = {
        mt5.TIMEFRAME_M1: "M1",
        mt5.TIMEFRAME_M5: "M5",
        mt5.TIMEFRAME_M15: "M15",
        mt5.TIMEFRAME_M30: "M30",
        mt5.TIMEFRAME_H1: "H1",
        mt5.TIMEFRAME_H4: "H4",
        mt5.TIMEFRAME_D1: "D1",
        mt5.TIMEFRAME_W1: "W1",
        mt5.TIMEFRAME_MN1: "MN1"
    }

    # Map timeframe names to MT5 timeframe constants
    NAME_TO_MT5 = {v: k for k, v in MT5_TO_NAME.items()}


class MT5DataFetcher:
    """Service for fetching historical price data from MT5"""

    def __init__(self):
        """Initialize the MT5 data fetcher"""
        self._initialized = False
        self._connection_service = None
        self._logging_service = None
        self._app_config = None
        self._db_url = None
        self._db_session = None
        self._instruments_cache = {}
        self._timeframes_cache = {}

    def initialize(self) -> bool:
        """
        Initialize the data fetcher service

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
            self._logging_service = container.resolve(LoggingService)
            self._app_config = container.resolve(AppConfig)
            self._db_url = container.resolve(URL)

            # Ensure MT5 connection
            if not self._connection_service.ensure_connection():
                raise Exception("Failed to establish MT5 connection")

            # Initialize database session
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker

            engine = create_engine(self._db_url)
            Session = sessionmaker(bind=engine)
            self._db_session = Session()

            # Cache instruments and timeframes
            self._cache_instruments_and_timeframes()

            self._initialized = True
            self._logging_service.log('INFO', 'data_fetcher', "MT5 data fetcher initialized")
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'data_fetcher', f"MT5 data fetcher initialization error: {str(e)}")
            raise

    def _cache_instruments_and_timeframes(self) -> None:
        """
        Cache instruments and timeframes from the database
        """
        try:
            # Cache instruments
            instruments = self._db_session.query(Instrument).all()
            for instrument in instruments:
                self._instruments_cache[instrument.symbol] = instrument

            # Cache timeframes
            timeframes = self._db_session.query(Timeframe).all()
            for timeframe in timeframes:
                self._timeframes_cache[timeframe.name] = timeframe

            self._logging_service.log(
                'INFO',
                'data_fetcher',
                f"Cached {len(self._instruments_cache)} instruments and {len(self._timeframes_cache)} timeframes"
            )

        except Exception as e:
            self._logging_service.log('ERROR', 'data_fetcher', f"Failed to cache instruments and timeframes: {str(e)}")
            raise

    def _get_instrument_id(self, symbol: str) -> int:
        """
        Get the instrument ID for a symbol

        Args:
            symbol: The symbol to get the ID for

        Returns:
            int: The instrument ID

        Raises:
            Exception: If the instrument is not found
        """
        if symbol in self._instruments_cache:
            return self._instruments_cache[symbol].id

        # If not in cache, try to get from database
        instrument = self._db_session.query(Instrument).filter(Instrument.symbol == symbol).first()
        if not instrument:
            raise Exception(f"Instrument {symbol} not found in database")

        # Add to cache
        self._instruments_cache[symbol] = instrument
        return instrument.id

    def _get_timeframe_id(self, timeframe_name: str) -> int:
        """
        Get the timeframe ID for a timeframe name

        Args:
            timeframe_name: The timeframe name to get the ID for

        Returns:
            int: The timeframe ID

        Raises:
            Exception: If the timeframe is not found
        """
        if timeframe_name in self._timeframes_cache:
            return self._timeframes_cache[timeframe_name].id

        # If not in cache, try to get from database
        timeframe = self._db_session.query(Timeframe).filter(Timeframe.name == timeframe_name).first()
        if not timeframe:
            raise Exception(f"Timeframe {timeframe_name} not found in database")

        # Add to cache
        self._timeframes_cache[timeframe_name] = timeframe
        return timeframe.id

    def fetch_initial_data(self) -> None:
        """
        Fetch initial historical price data for all configured instruments and timeframes

        Raises:
            Exception: If data fetching fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # Ensure MT5 connection
            if not self._connection_service.ensure_connection():
                raise Exception("MT5 connection not established")

            # Get trading config
            trading_config = self._app_config.trading

            # Loop through configured instruments
            for symbol, instrument_config in trading_config.instruments.items():
                # Loop through configured timeframes
                for tf_name, tf_config in instrument_config.timeframes.items():
                    # Get the MT5 timeframe constant
                    mt5_timeframe = TimeframeMapping.NAME_TO_MT5.get(tf_name)
                    if mt5_timeframe is None:
                        self._logging_service.log(
                            'WARNING',
                            'data_fetcher',
                            f"Unknown timeframe {tf_name} for {symbol}, skipping"
                        )
                        continue

                    # Log fetching attempt
                    self._logging_service.log(
                        'INFO',
                        'data_fetcher',
                        f"Fetching {tf_config.history_size} {tf_name} bars for {symbol}"
                    )

                    # Fetch data from MT5
                    bars = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, tf_config.history_size)
                    if bars is None or len(bars) == 0:
                        error = mt5.last_error()
                        self._logging_service.log(
                            'WARNING',
                            'data_fetcher',
                            f"Failed to fetch {tf_name} bars for {symbol}: {error}"
                        )
                        continue

                    # Log fetching success
                    self._logging_service.log(
                        'INFO',
                        'data_fetcher',
                        f"Fetched {len(bars)} {tf_name} bars for {symbol}"
                    )

                    # Store data in database
                    self._store_price_bars(symbol, tf_name, bars)

        except Exception as e:
            self._logging_service.log('ERROR', 'data_fetcher', f"Failed to fetch initial data: {str(e)}")
            raise

    def _store_price_bars(self, symbol: str, timeframe_name: str, bars: np.ndarray) -> None:
        """
        Store price bars in the database

        Args:
            symbol: The symbol of the price bars
            timeframe_name: The timeframe name of the price bars
            bars: The price bars to store

        Raises:
            Exception: If storing fails
        """
        try:
            # Get instrument and timeframe IDs
            instrument_id = self._get_instrument_id(symbol)
            timeframe_id = self._get_timeframe_id(timeframe_name)

            # Convert bars to pandas DataFrame for easier handling
            df = pd.DataFrame(bars)

            # Convert time column to datetime
            df['time'] = pd.to_datetime(df['time'], unit='s')

            # Count new and updated bars
            new_bars = 0
            updated_bars = 0

            # Process each bar
            for _, bar in df.iterrows():
                # Create a price bar object
                price_bar = PriceBar(
                    instrument_id=instrument_id,
                    timeframe_id=timeframe_id,
                    timestamp=bar['time'],
                    open=float(bar['open']),
                    high=float(bar['high']),
                    low=float(bar['low']),
                    close=float(bar['close']),
                    volume=float(bar['tick_volume']),
                    spread=float(bar['spread'])
                )

                # Check if the bar already exists
                existing_bar = self._db_session.query(PriceBar).filter(
                    and_(
                        PriceBar.instrument_id == instrument_id,
                        PriceBar.timeframe_id == timeframe_id,
                        PriceBar.timestamp == bar['time']
                    )
                ).first()

                if existing_bar:
                    # Update existing bar
                    existing_bar.open = price_bar.open
                    existing_bar.high = price_bar.high
                    existing_bar.low = price_bar.low
                    existing_bar.close = price_bar.close
                    existing_bar.volume = price_bar.volume
                    existing_bar.spread = price_bar.spread
                    updated_bars += 1
                else:
                    # Add new bar
                    self._db_session.add(price_bar)
                    new_bars += 1

            # Commit changes
            self._db_session.commit()

            self._logging_service.log(
                'INFO',
                'data_fetcher',
                f"Stored {new_bars} new and updated {updated_bars} existing {timeframe_name} bars for {symbol}"
            )

        except Exception as e:
            self._db_session.rollback()
            self._logging_service.log(
                'ERROR',
                'data_fetcher',
                f"Failed to store {timeframe_name} bars for {symbol}: {str(e)}"
            )
            raise

    def fetch_latest_bar(self, symbol: str, timeframe_name: str) -> Optional[PriceBar]:
        """
        Fetch the latest price bar for a specific symbol and timeframe

        Args:
            symbol: The symbol to fetch the latest bar for
            timeframe_name: The timeframe name to fetch the latest bar for

        Returns:
            Optional[PriceBar]: The latest price bar or None if not found

        Raises:
            Exception: If fetching fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # Ensure MT5 connection
            if not self._connection_service.ensure_connection():
                raise Exception("MT5 connection not established")

            # Get the MT5 timeframe constant
            mt5_timeframe = TimeframeMapping.NAME_TO_MT5.get(timeframe_name)
            if mt5_timeframe is None:
                raise Exception(f"Unknown timeframe {timeframe_name}")

            # Fetch the latest bar from MT5
            bars = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, 1)
            if bars is None or len(bars) == 0:
                error = mt5.last_error()
                self._logging_service.log(
                    'WARNING',
                    'data_fetcher',
                    f"Failed to fetch latest {timeframe_name} bar for {symbol}: {error}"
                )
                return None

            # Convert time to datetime
            bar_time = pd.to_datetime(bars[0]['time'], unit='s')

            # Get instrument and timeframe IDs
            instrument_id = self._get_instrument_id(symbol)
            timeframe_id = self._get_timeframe_id(timeframe_name)

            # Create a price bar object
            price_bar = PriceBar(
                instrument_id=instrument_id,
                timeframe_id=timeframe_id,
                timestamp=bar_time,
                open=float(bars[0]['open']),
                high=float(bars[0]['high']),
                low=float(bars[0]['low']),
                close=float(bars[0]['close']),
                volume=float(bars[0]['tick_volume']),
                spread=float(bars[0]['spread'])
            )

            return price_bar

        except Exception as e:
            self._logging_service.log(
                'ERROR',
                'data_fetcher',
                f"Failed to fetch latest {timeframe_name} bar for {symbol}: {str(e)}"
            )
            raise

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """
        Get the latest price for a symbol

        Args:
            symbol: The symbol to get the latest price for

        Returns:
            Optional[float]: The latest price or None if not found

        Raises:
            Exception: If fetching fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # Ensure MT5 connection
            if not self._connection_service.ensure_connection():
                raise Exception("MT5 connection not established")

            # Get the latest tick
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                error = mt5.last_error()
                self._logging_service.log(
                    'WARNING',
                    'data_fetcher',
                    f"Failed to fetch latest tick for {symbol}: {error}"
                )
                return None

            # Return the latest ask price
            return tick.ask

        except Exception as e:
            self._logging_service.log(
                'ERROR',
                'data_fetcher',
                f"Failed to get latest price for {symbol}: {str(e)}"
            )
            raise


# Create an instance and register in container
mt5_data_fetcher = MT5DataFetcher()
container.register(MT5DataFetcher, mt5_data_fetcher)