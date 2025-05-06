# config/app_config.py

from dataclasses import dataclass, field
from typing import Dict, Set, List, Any


@dataclass
class TimeframeConfig:
    """Configuration for a specific timeframe"""
    name: str
    history_size: int

    def __post_init__(self):
        if not self.name:
            raise Exception("Timeframe name cannot be empty")
        if self.history_size <= 0:
            raise Exception(f"Invalid history size for timeframe {self.name}: {self.history_size}")


@dataclass
class InstrumentConfig:
    """Configuration for a trading instrument"""
    symbol: str
    description: str
    pip_value: float
    timeframes: Dict[str, TimeframeConfig]

    def __post_init__(self):
        if not self.symbol:
            raise Exception("Instrument symbol cannot be empty")
        if self.pip_value <= 0:
            raise Exception(f"Invalid pip value for {self.symbol}: {self.pip_value}")
        if not self.timeframes:
            raise Exception(f"No timeframes configured for {self.symbol}")


@dataclass
class SyncConfig:
    """Configuration for data synchronization"""
    interval_seconds: int
    max_retry_attempts: int
    retry_delay_seconds: int

    def __post_init__(self):
        if self.interval_seconds <= 0:
            raise Exception(f"Invalid sync interval: {self.interval_seconds}")
        if self.max_retry_attempts <= 0:
            raise Exception(f"Invalid max retry attempts: {self.max_retry_attempts}")
        if self.retry_delay_seconds <= 0:
            raise Exception(f"Invalid retry delay: {self.retry_delay_seconds}")


@dataclass
class LoggingConfig:
    """Configuration for the application's logging system"""
    # Database connection configuration
    max_records: int
    # Console output configuration
    console_output: bool
    enabled_levels: Set[str]
    color_scheme: Dict[str, str]
    # Component-specific logging configuration
    component_configs: Dict[str, Dict[str, Any]]

    def __post_init__(self):
        if self.max_records <= 0:
            raise Exception(f"Invalid max records: {self.max_records}")
        if not self.enabled_levels:
            raise Exception("No logging levels enabled")
        if not all(level in {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'} for level in self.enabled_levels):
            raise Exception(f"Invalid logging levels: {self.enabled_levels}")

        # Validate color scheme
        required_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if not all(level in self.color_scheme for level in required_levels):
            missing = required_levels - set(self.color_scheme.keys())
            raise Exception(f"Missing color scheme for levels: {missing}")

    def get_component_config(self, component_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific component.

        Args:
            component_name: The name of the component

        Returns:
            The component's configuration

        Raises:
            MissingComponentConfigError: If the component's configuration is not found
        """
        if component_name not in self.component_configs:
            raise Exception(f"No logging configuration found for component: {component_name}")

        component_config = self.component_configs[component_name]

        # Validate component config
        if 'enabled_levels' not in component_config:
            raise Exception(f"No enabled levels specified for component: {component_name}")
        if 'console_output' not in component_config:
            raise Exception(f"Console output setting not specified for component: {component_name}")

        return component_config

    def is_level_enabled(self, level: str, component: str) -> bool:
        """
        Check if a specific log level is enabled for a component.

        Args:
            level: The log level to check (DEBUG, INFO, etc.)
            component: The component name

        Returns:
            True if the level is enabled, False otherwise

        Raises:
            MissingComponentConfigError: If the component's configuration is not found
        """
        component_config = self.get_component_config(component)
        return level in component_config['enabled_levels']


@dataclass
class TradingConfig:
    """Main trading configuration"""
    instruments: Dict[str, InstrumentConfig]
    sync_config: SyncConfig

    def __post_init__(self):
        if not self.instruments:
            raise Exception("No instruments configured")


@dataclass
class DatabaseConfig:
    """Database configuration"""
    server: str
    database: str
    driver: str
    use_windows_auth: bool
    username: str  # Only used if use_windows_auth is False
    password: str  # Only used if use_windows_auth is False

    def __post_init__(self):
        if not self.server:
            raise Exception("Database server cannot be empty")
        if not self.database:
            raise Exception("Database name cannot be empty")
        if not self.driver:
            raise Exception("Database driver cannot be empty")
        if not self.use_windows_auth and (not self.username or not self.password):
            raise Exception("SQL authentication requires username and password")


@dataclass
class MT5Config:
    """MetaTrader5 configuration"""
    server: str
    login: int
    password: str
    timeout: int

    def __post_init__(self):
        if not self.server:
            raise Exception("MT5 server cannot be empty")
        if self.login <= 0:
            raise Exception(f"Invalid MT5 login: {self.login}")
        if not self.password:
            raise Exception("MT5 password cannot be empty")
        if self.timeout <= 0:
            raise Exception(f"Invalid MT5 timeout: {self.timeout}")


@dataclass
class AppConfig:
    """Main application configuration"""
    trading: TradingConfig
    logging: LoggingConfig
    database: DatabaseConfig
    mt5: MT5Config