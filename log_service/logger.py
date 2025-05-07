# log_service/logger.py

from typing import Optional, Dict, Any
import sys
import os
import datetime
import logbook
from logbook import Logger, StreamHandler
from container import container
from config.app_config import AppConfig, LoggingConfig


class LoggingService:
    """Service for centralized logging with component-specific configuration"""

    def __init__(self):
        self._initialized = False
        self._loggers: Dict[str, Logger] = {}
        self._console_handler: Optional[StreamHandler] = None
        self._file = None
        self._config: Optional[LoggingConfig] = None

    def initialize(self) -> None:
        """
        Initialize the logging service
        """
        if self._initialized:
            return

        try:
            # Get application config from container
            app_config = container.resolve(AppConfig)
            self._config = app_config.logging

            # Create logs directory if it doesn't exist
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Open log file
            self._file = open(os.path.join(log_dir, "application.log"), "a", encoding="utf-8")

            # Set up console logging if enabled
            if self._config.console_output:
                self._console_handler = StreamHandler(sys.stdout)
                self._console_handler.push_application()

            self._initialized = True

        except Exception as e:
            raise Exception(f"Failed to initialize logging: {str(e)}") from e

    def get_logger(self, component_name: str) -> Logger:
        """
        Get a logger for a specific component.
        """
        if not self._initialized:
            self.initialize()

        # Return existing logger if already created
        if component_name in self._loggers:
            return self._loggers[component_name]

        # Verify component config exists
        if self._config is None:
            raise Exception("Logging configuration not loaded")

        # This will raise MissingComponentConfigError if component config doesn't exist
        self._config.get_component_config(component_name)

        # Create new logger
        logger = Logger(component_name)

        # Store for future use
        self._loggers[component_name] = logger

        return logger

    def log(self, level: str, component: str, message: str, **kwargs) -> None:
        """
        Log a message with the specified level and component.
        """
        if not self._initialized:
            self.initialize()

        if self._config is None:
            raise Exception("Logging configuration not loaded")

        # Get component-specific config
        comp_config = self._config.get_component_config(component)

        # Check if this level is enabled for this component
        if level not in comp_config['enabled_levels']:
            return

        # Get logger
        logger = self.get_logger(component)

        # Format current time as HH:MM:SS
        current_time = datetime.datetime.now().strftime('%H:%M:%S')

        # Get color codes
        color_code = self._config.color_scheme.get(level, '')
        reset_code = '\033[0m'

        # Format console message with colors
        console_msg = f"[{current_time}] - {color_code}[{level}]{reset_code} - {component} - {color_code}{message}{reset_code}"

        # Format file message without colors
        file_msg = f"[{current_time}] - [{level}] - {component} - {message}\n"

        # Write to file directly
        if self._file:
            self._file.write(file_msg)
            self._file.flush()

        # Log to console with standard logbook
        if level == 'DEBUG':
            logger.debug(console_msg, **kwargs)
        elif level == 'INFO':
            logger.info(console_msg, **kwargs)
        elif level == 'WARNING':
            logger.warning(console_msg, **kwargs)
        elif level == 'ERROR':
            logger.error(console_msg, **kwargs)
        elif level == 'CRITICAL':
            logger.critical(console_msg, **kwargs)


# Create an instance for dependency injection
logging_service = LoggingService()

# Register in container
container.register(LoggingService, logging_service)