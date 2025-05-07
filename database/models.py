# Database/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class Instrument(Base):
    __tablename__ = 'Instruments'
    __table_args__ = {'schema': 'Trading'}

    id = Column(Integer, primary_key=True)
    symbol = Column(String(20), nullable=False, unique=True)
    description = Column(String(100), nullable=False)
    pip_value = Column(Float, nullable=False)

    # Relationships
    price_bars = relationship("PriceBar", back_populates="instrument")

    @classmethod
    def from_db_row(cls, row):
        return cls(
            id=row.id,
            symbol=row.symbol,
            description=row.description,
            pip_value=row.pip_value
        )


class Timeframe(Base):
    __tablename__ = 'Timeframes'
    __table_args__ = {'schema': 'Trading'}

    id = Column(Integer, primary_key=True)
    name = Column(String(10), nullable=False, unique=True)
    description = Column(String(50), nullable=False)
    minutes = Column(Integer, nullable=False)

    # Relationships
    price_bars = relationship("PriceBar", back_populates="timeframe")

    @classmethod
    def from_db_row(cls, row):
        return cls(
            id=row.id,
            name=row.name,
            description=row.description,
            minutes=row.minutes
        )


class PriceBar(Base):
    __tablename__ = 'PriceData'
    __table_args__ = (
        UniqueConstraint('instrument_id', 'timeframe_id', 'timestamp', name='UQ_PriceData_InstrumentTimeframeTimestamp'),
        {'schema': 'Trading'}
    )

    id = Column(Integer, primary_key=True)
    instrument_id = Column(Integer, ForeignKey('Trading.Instruments.id'), nullable=False)
    timeframe_id = Column(Integer, ForeignKey('Trading.Timeframes.id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    spread = Column(Float, nullable=True)

    # Relationships
    instrument = relationship("Instrument", back_populates="price_bars")
    timeframe = relationship("Timeframe", back_populates="price_bars")

    @classmethod
    def from_db_row(cls, row):
        return cls(
            id=row.id,
            instrument_id=row.instrument_id,
            timeframe_id=row.timeframe_id,
            timestamp=row.timestamp,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            spread=row.spread
        )


# Add log-related models
class EventLog(Base):
    __tablename__ = 'Events'
    __table_args__ = {'schema': 'Logs'}

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    level = Column(String(10), nullable=False)
    event_type = Column(String(50), nullable=False)
    component = Column(String(50), nullable=False)
    message = Column(String(500), nullable=False)
    source = Column(String(100), nullable=True)
    user_name = Column(String(100), nullable=True)
    action = Column(String(100), nullable=True)
    status = Column(String(20), nullable=True)
    process_id = Column(Integer, nullable=True)
    thread_id = Column(Integer, nullable=True)
    details = Column(Text, nullable=True)


class ErrorLog(Base):
    __tablename__ = 'Errors'
    __table_args__ = {'schema': 'Logs'}

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    level = Column(String(10), nullable=False)
    exception_type = Column(String(100), nullable=False)
    function_name = Column(String(100), nullable=False)
    message = Column(String(500), nullable=False)
    traceback = Column(Text, nullable=False)
    context = Column(Text, nullable=True)


class TradeLog(Base):
    __tablename__ = 'Trades'
    __table_args__ = {'schema': 'Logs'}

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    level = Column(String(10), nullable=False)
    symbol = Column(String(20), nullable=False)
    operation = Column(String(20), nullable=False)
    price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    order_id = Column(Integer, nullable=True)
    strategy = Column(String(50), nullable=True)
    message = Column(String(500), nullable=False)