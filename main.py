# main.py
from container import container
from config.setup import initialize_application
from log_service.logger import LoggingService
from config.app_config import AppConfig
from mt5_service.connection import MT5ConnectionService
from mt5_service.data_fetcher import MT5DataFetcher
from mt5_service.data_sync import MT5DataSyncService


def main():
    try:
        # Initialize application
        initialize_application()

        # Get dependencies from container
        logging_service = container.resolve(LoggingService)
        app_config = container.resolve(AppConfig)
        mt5_connection = container.resolve(MT5ConnectionService)
        mt5_data_fetcher = container.resolve(MT5DataFetcher)
        mt5_data_sync = container.resolve(MT5DataSyncService)

        # Initialize logging
        logging_service.initialize()
        logging_service.log('INFO', 'main', 'Starting application')

        # Initialize MT5 connection
        mt5_connection.initialize()
        logging_service.log('INFO', 'main', 'MT5 connection initialized')

        # Initialize data fetcher and fetch initial data
        mt5_data_fetcher.initialize()
        logging_service.log('INFO', 'main', 'MT5 data fetcher initialized')

        # Initialize and start data sync service
        mt5_data_sync.initialize()
        mt5_data_sync.start()
        logging_service.log('INFO', 'main', 'MT5 data sync service started')

        # Keep the application running
        import time
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging_service.log('INFO', 'main', 'Application interrupted by user')
        finally:
            # Stop the data sync service
            mt5_data_sync.stop()
            logging_service.log('INFO', 'main', 'MT5 data sync service stopped')

            # Shutdown MT5 connection
            mt5_connection.shutdown()
            logging_service.log('INFO', 'main', 'MT5 connection shutdown')

            logging_service.log('INFO', 'main', 'Application shutdown complete')

    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()