from container import container
from config.setup import initialize_application
from log_service.logger import LoggingService
from config.app_config import AppConfig


def main():
    try:
        # Initialize application
        initialize_application()

        # Get dependencies from container
        logging_service = container.resolve(LoggingService)
        app_config = container.resolve(AppConfig)

        # Initialize logging
        logging_service.initialize()

        # Log some messages
        logging_service.log('INFO', 'trade_executor', 'Starting trade execution')
        logging_service.log('WARNING', 'data_fetcher', 'Failed to fetch data, retrying...')

        # Access configuration
        eurusd_config = app_config.trading.instruments.get('EURUSD')
        if eurusd_config:
            logging_service.log(
                'INFO',
                'trade_executor',
                f"EURUSD pip value: {eurusd_config.pip_value}"
            )

            h1_config = eurusd_config.timeframes.get('H1')
            if h1_config:
                logging_service.log(
                    'INFO',
                    'trade_executor',
                    f"EURUSD H1 history size: {h1_config.history_size}"
                )

    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()