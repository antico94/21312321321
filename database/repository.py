# database/repository.py
from typing import Dict, List, Optional, Union
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, desc

from container import container
from config.app_config import AppConfig
from log_service.logger import LoggingService
from database.models import Instrument, Timeframe, PriceBar
from sqlalchemy.engine import URL


class PriceRepository:
    """Repository for price data operations"""

    def __init__(self):
        """Initialize the price repository"""
        self._initialized = False
        self._logging_service = None
        self._app_config = None
        self._db_url = None
        self._db_session = None

    def initialize(self) -> bool:
        """
        Initialize the price repository

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
            self._app_config = container.resolve(AppConfig)
            self._db_url = container.resolve(URL)

            # Initialize database session
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker

            engine = create_engine(self._db_url)
            Session = sessionmaker(bind=engine)
            self._db_session = Session()

            self._initialized = True
            self._logging_service.log('INFO', 'price_repository', "Price repository initialized")
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'price_repository', f"Price repository initialization error: {str(e)}")
            raise

    def get_price_bars(self, symbol: str, timeframe_name: str, count: int = 100) -> List[PriceBar]:
        """
        Get price bars for a specific symbol and timeframe

        Args:
            symbol: The symbol to get price bars for
            timeframe_name: The timeframe name to get price bars for
            count: The number of bars to get

        Returns:
            List[PriceBar]: The price bars

        Raises:
            Exception: If getting price bars fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # Get instrument and timeframe records
            instrument = self._db_session.query(Instrument).filter(Instrument.symbol == symbol).first()
            if not instrument:
                raise Exception(f"Instrument {symbol} not found in database")

            timeframe = self._db_session.query(Timeframe).filter(Timeframe.name == timeframe_name).first()
            if not timeframe:
                raise Exception(f"Timeframe {timeframe_name} not found in database")

            # Query price bars
            price_bars = self._db_session.query(PriceBar).filter(
                and_(
                    PriceBar.instrument_id == instrument.id,
                    PriceBar.timeframe_id == timeframe.id
                )
            ).order_by(desc(PriceBar.timestamp)).limit(count).all()

            # Reverse the list to get chronological order
            price_bars.reverse()

            return price_bars

        except Exception as e:
            self._logging_service.log(
                'ERROR',
                'price_repository',
                f"Failed to get {timeframe_name} price bars for {symbol}: {str(e)}"
            )
            raise

    def get_latest_price_bar(self, symbol: str, timeframe_name: str) -> Optional[PriceBar]:
        """
        Get the latest price bar for a specific symbol and timeframe

        Args:
            symbol: The symbol to get the latest price bar for
            timeframe_name: The timeframe name to get the latest price bar for

        Returns:
            Optional[PriceBar]: The latest price bar or None if not found

        Raises:
            Exception: If getting the latest price bar fails
        """
        try:
            # Ensure initialization
            if not self._initialized:
                self.initialize()

            # Get instrument and timeframe records
            instrument = self._db_session.query(Instrument).filter(Instrument.symbol == symbol).first()
            if not instrument:
                raise Exception(f"Instrument {symbol} not found in database")

            timeframe = self._db_session.query(Timeframe).filter(Timeframe.name == timeframe_name).first()
            if not timeframe:
                raise Exception(f"Timeframe {timeframe_name} not found in database")

            # Query the latest price bar
            latest_bar = self._db_session.query(PriceBar).filter(
                and_(
                    PriceBar.instrument_id == instrument.id,
                    PriceBar.timeframe_id == timeframe.id
                )
            ).order_by(desc(PriceBar.timestamp)).first()

            return latest_bar

        except Exception as e:
            self._logging_service.log(
                'ERROR',
                'price_repository',
                f"Failed to get latest {timeframe_name} price bar for {symbol}: {str(e)}"
            )
            raise


# Create an instance and register in container
price_repository = PriceRepository()
container.register(PriceRepository, price_repository)