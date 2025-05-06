# config/setup.py

from sqlalchemy.engine import URL
from container import container
from config.app_config import (
    AppConfig, TradingConfig, LoggingConfig, DatabaseConfig, MT5Config,
    InstrumentConfig, TimeframeConfig, SyncConfig
)
from config.credentials import (
    SQL_SERVER, SQL_DATABASE, SQL_DRIVER,
    USE_WINDOWS_AUTH, SQL_USERNAME, SQL_PASSWORD,
    MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, MT5_TIMEOUT
)


def create_sqlalchemy_url() -> URL:
    """Create SQLAlchemy URL from configuration"""
    if USE_WINDOWS_AUTH:
        # Windows Authentication
        return URL.create(
            "mssql+pyodbc",
            query={
                "driver": SQL_DRIVER,
                "Trusted_Connection": "yes"
            },
            host=SQL_SERVER,
            database=SQL_DATABASE
        )
    else:
        # SQL Authentication
        return URL.create(
            "mssql+pyodbc",
            username=SQL_USERNAME,
            password=SQL_PASSWORD,
            host=SQL_SERVER,
            database=SQL_DATABASE,
            query={"driver": SQL_DRIVER}
        )


def setup_configuration():
    """
    Set up and register all application configuration in the container

    Raises:
        ConfigurationError: If any configuration is invalid
    """
    # Create database config
    database_config = DatabaseConfig(
        server=SQL_SERVER,
        database=SQL_DATABASE,
        driver=SQL_DRIVER,
        use_windows_auth=USE_WINDOWS_AUTH,
        username=SQL_USERNAME,
        password=SQL_PASSWORD
    )

    # Create MT5 config
    mt5_config = MT5Config(
        server=MT5_SERVER,
        login=MT5_LOGIN,
        password=MT5_PASSWORD,
        timeout=MT5_TIMEOUT
    )

    # Create instruments with timeframes
    instruments = {}

    # EURUSD
    eurusd_timeframes = {
        "M1": TimeframeConfig(name="M1", history_size=300),
        "M5": TimeframeConfig(name="M5", history_size=300),
        "M15": TimeframeConfig(name="M15", history_size=300),
        "M30": TimeframeConfig(name="M30", history_size=300),
        "H1": TimeframeConfig(name="H1", history_size=300),
        "H4": TimeframeConfig(name="H4", history_size=300),
        "D1": TimeframeConfig(name="D1", history_size=300),
        "W1": TimeframeConfig(name="W1", history_size=200),
        "MN1": TimeframeConfig(name="MN1", history_size=150)
    }
    instruments["EURUSD"] = InstrumentConfig(
        symbol="EURUSD",
        description="Euro vs US Dollar",
        pip_value=0.0001,
        timeframes=eurusd_timeframes
    )

    # GBPUSD
    gbpusd_timeframes = {
        "M1": TimeframeConfig(name="M1", history_size=300),
        "M5": TimeframeConfig(name="M5", history_size=300),
        "M15": TimeframeConfig(name="M15", history_size=300),
        "M30": TimeframeConfig(name="M30", history_size=300),
        "H1": TimeframeConfig(name="H1", history_size=300),
        "H4": TimeframeConfig(name="H4", history_size=300),
        "D1": TimeframeConfig(name="D1", history_size=300),
        "W1": TimeframeConfig(name="W1", history_size=200),
        "MN1": TimeframeConfig(name="MN1", history_size=150)
    }
    instruments["GBPUSD"] = InstrumentConfig(
        symbol="GBPUSD",
        description="Great Britain Pound vs US Dollar",
        pip_value=0.0001,
        timeframes=gbpusd_timeframes
    )

    # USDJPY
    usdjpy_timeframes = {
        "M1": TimeframeConfig(name="M1", history_size=300),
        "M5": TimeframeConfig(name="M5", history_size=300),
        "M15": TimeframeConfig(name="M15", history_size=300),
        "M30": TimeframeConfig(name="M30", history_size=300),
        "H1": TimeframeConfig(name="H1", history_size=300),
        "H4": TimeframeConfig(name="H4", history_size=300),
        "D1": TimeframeConfig(name="D1", history_size=300),
        "W1": TimeframeConfig(name="W1", history_size=200),
        "MN1": TimeframeConfig(name="MN1", history_size=150)
    }
    instruments["USDJPY"] = InstrumentConfig(
        symbol="USDJPY",
        description="US Dollar vs Japanese Yen",
        pip_value=0.01,
        timeframes=usdjpy_timeframes
    )

    # XAUUSD
    xauusd_timeframes = {
        "M1": TimeframeConfig(name="M1", history_size=300),
        "M5": TimeframeConfig(name="M5", history_size=300),
        "M15": TimeframeConfig(name="M15", history_size=300),
        "M30": TimeframeConfig(name="M30", history_size=300),
        "H1": TimeframeConfig(name="H1", history_size=300),
        "H4": TimeframeConfig(name="H4", history_size=300),
        "D1": TimeframeConfig(name="D1", history_size=300),
        "W1": TimeframeConfig(name="W1", history_size=200),
        "MN1": TimeframeConfig(name="MN1", history_size=150)
    }
    instruments["XAUUSD"] = InstrumentConfig(
        symbol="XAUUSD",
        description="Gold vs US Dollar",
        pip_value=0.01,
        timeframes=xauusd_timeframes
    )

    # Create sync config
    sync_config = SyncConfig(
        interval_seconds=10,
        max_retry_attempts=3,
        retry_delay_seconds=5
    )

    # Create trading config
    trading_config = TradingConfig(
        instruments=instruments,
        sync_config=sync_config
    )

    # Create logging config
    logging_config = LoggingConfig(
        max_records=10000,
        console_output=True,
        enabled_levels={'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
        color_scheme={
            'DEBUG': '\033[37m',  # White
            'INFO': '\033[36m',  # Cyan
            'WARNING': '\033[33m',  # Yellow
            'ERROR': '\033[31m',  # Red
            'CRITICAL': '\033[41m',  # Red background
        },
        component_configs={
            'data_fetcher': {
                'enabled_levels': {'WARNING', 'ERROR', 'CRITICAL'},
                'console_output': False
            },
            'trade_executor': {
                'enabled_levels': {'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
                'console_output': True
            }
        }
    )

    # Create app config
    app_config = AppConfig(
        trading=trading_config,
        logging=logging_config,
        database=database_config,
        mt5=mt5_config
    )

    # Register in container
    container.register(AppConfig, app_config)
    container.register(URL, create_sqlalchemy_url())


def initialize_application():
    """Initialize all application components"""
    setup_configuration()