# main.py
from container import container
from config.setup import initialize_application
from log_service.logger import LoggingService
from config.app_config import AppConfig
from mt5_service.connection import MT5ConnectionService
from mt5_service.data_fetcher import MT5DataFetcher
from mt5_service.data_sync import MT5DataSyncService
from strategies.strategy_manager import StrategyManager
from trade.trade_manager import TradeManager
import threading
import time


def trailing_stop_updater(stop_event):
    """Background thread for updating trailing stops"""
    trade_manager = container.resolve(TradeManager)
    logging_service = container.resolve(LoggingService)

    while not stop_event.is_set():
        try:
            trade_manager.update_trailing_stops()
        except Exception as e:
            logging_service.log('ERROR', 'trailing_stop_updater', f"Error updating trailing stops: {str(e)}")

        # Update every 5 seconds
        time.sleep(5)


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
        strategy_manager = container.resolve(StrategyManager)
        trade_manager = container.resolve(TradeManager)

        # Initialize logging
        logging_service.initialize()
        logging_service.log('INFO', 'main', 'Starting application')

        # Initialize MT5 connection
        mt5_connection.initialize()
        logging_service.log('INFO', 'main', 'MT5 connection initialized')

        # Initialize data fetcher and fetch initial data
        mt5_data_fetcher.initialize()
        logging_service.log('INFO', 'main', 'MT5 data fetcher initialized')

        # Initialize trade manager
        trade_manager.initialize()
        logging_service.log('INFO', 'main', 'Trade manager initialized')

        # Initialize strategy manager and connect to trade manager
        strategy_manager.initialize()
        logging_service.log('INFO', 'main', 'Strategy manager initialized')

        # Initialize and start data sync service
        mt5_data_sync.initialize()
        mt5_data_sync.start()
        logging_service.log('INFO', 'main', 'MT5 data sync service started')

        # Create and start the trailing stop updater thread
        stop_event = threading.Event()
        trailing_thread = threading.Thread(target=trailing_stop_updater, args=(stop_event,))
        trailing_thread.daemon = True
        trailing_thread.start()
        logging_service.log('INFO', 'main', 'Trailing stop updater thread started')

        # Keep the application running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging_service.log('INFO', 'main', 'Application interrupted by user')
        finally:
            # Signal threads to stop
            stop_event.set()

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