# log_service/logger.py

from typing import Optional, Dict, Any
import sys
import logbook
from logbook import Logger, StreamHandler
from logbook.ticketing import TicketingHandler
from container import container
from config.app_config import AppConfig, LoggingConfig
from sqlalchemy.engine import URL


class LoggingService:
    """Service for centralized logging with component-specific configuration"""

    def __init__(self):
        self._initialized = False
        self._loggers: Dict[str, Logger] = {}
        self._db_handler: Optional[TicketingHandler] = None
        self._console_handler: Optional[StreamHandler] = None
        self._config: Optional[LoggingConfig] = None

    # log_service/logger.py

    def initialize(self) -> None:
        """
        Initialize the logging service

        Raises:
            Exception: If the logging system can't be properly initialized
        """
        if self._initialized:
            return

        try:
            # Get application config from container
            app_config = container.resolve(AppConfig)
            self._config = app_config.logging

            # Get SQLAlchemy URL and convert to string URI for TicketingHandler
            sqlalchemy_url = container.resolve(URL)

            # Convert SQLAlchemy URL to string URI format
            # Example: "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
            uri_string = str(sqlalchemy_url)

            # Create TicketingHandler with URI string
            self._db_handler = TicketingHandler(
                uri=uri_string,
                level=logbook.WARNING,  # Base level for DB logging
                max_tickets=self._config.max_records
            )
            self._db_handler.push_application()

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

        Args:
            component_name: The name of the component requiring logging

        Returns:
            Logger: A configured logger instance

        Raises:
            Exception: If the logging system is not initialized
            MissingComponentConfigError: If the component's configuration is not found
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

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            component: Component name
            message: Message to log
            **kwargs: Additional data to include in the log

        Raises:
            Exception: If the logging system is not initialized
            MissingComponentConfigError: If the component's configuration is not found
            ValueError: If the log level is invalid
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

        # Get or create logger
        logger = self.get_logger(component)

        # Add color if console output is enabled for this component
        if comp_config['console_output']:
            color_code = self._config.color_scheme.get(level, '')
            reset_code = '\033[0m'
            colored_message = f"{color_code}{message}{reset_code}"
        else:
            colored_message = message

        # Log with appropriate level
        if level == 'DEBUG':
            logger.debug(colored_message, **kwargs)
        elif level == 'INFO':
            logger.info(colored_message, **kwargs)
        elif level == 'WARNING':
            logger.warning(colored_message, **kwargs)
        elif level == 'ERROR':
            logger.error(colored_message, **kwargs)
        elif level == 'CRITICAL':
            logger.critical(colored_message, **kwargs)
        else:
            raise ValueError(f"Invalid log level: {level}")


# Create an instance for dependency injection
logging_service = LoggingService()

# Register in container
container.register(LoggingService, logging_service)