# strategies/strategy_manager.py
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

from container import container
from config.app_config import AppConfig
from log_service.logger import LoggingService
from database.models import PriceBar
from database.repository import PriceRepository
from mt5_service.data_sync import MT5DataSyncService
from strategies.config import (
    StrategyType, TimeFrameType, StrategyConfig,
    InstrumentStrategiesConfig, TradingStrategiesConfig
)
from strategies.base_strategy import BaseStrategy, Signal

from strategies.scalping_strategy import ScalpingStrategy
from trade.trade_manager import TradeManager


class StrategyManager:
    """
    Manager for all trading strategies.

    Responsible for:
    - Loading strategy configurations
    - Creating strategy instances
    - Dispatching new bar events to appropriate strategies
    - Collecting and processing trading signals
    """

    def __init__(self):
        """Initialize the strategy manager"""
        self._initialized = False
        self._logging_service = None
        self._app_config = None
        self._price_repository = None
        self._data_sync_service = None
        self._strategies_config = None
        self._strategies: Dict[str, Dict[str, Dict[TimeFrameType, BaseStrategy]]] = {}
        self._signal_handlers: List[callable] = []

    def initialize(self) -> bool:
        """
        Initialize the strategy manager.

        Returns:
            bool: True if initialization is successful, False otherwise
        """
        if self._initialized:
            return True

        try:
            # Get dependencies
            self._logging_service = container.resolve(LoggingService)
            self._app_config = container.resolve(AppConfig)
            self._price_repository = container.resolve(PriceRepository)
            self._data_sync_service = container.resolve(MT5DataSyncService)

            # Load strategies configuration
            self._load_strategies_config()

            # Create strategy instances
            self._create_strategies()

            # Perform initial evaluation of all strategies
            self.perform_initial_evaluation()

            # Subscribe to new bar events
            self._subscribe_to_data_events()

            self._initialized = True
            self._logging_service.log('INFO', 'strategy_manager', "Strategy manager initialized")
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Strategy manager initialization failed: {str(e)}")
            raise

    def _load_strategies_config(self) -> None:
        """Load trading strategies configuration"""
        try:
            # This would normally be imported from 'config.strategy_config'
            # For now, we'll use a placeholder
            from strategies.config import trading_strategies_config
            self._strategies_config = trading_strategies_config

            instrument_count = len(self._strategies_config.instruments)
            strategy_count = sum(len(i.strategies) for i in self._strategies_config.instruments.values())

            self._logging_service.log(
                'INFO',
                'strategy_manager',
                f"Loaded {strategy_count} strategies for {instrument_count} instruments"
            )

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Failed to load strategies config: {str(e)}")
            raise

    def _create_strategies(self) -> None:
        """Create and initialize strategy instances based on configuration"""
        try:
            strategy_factory = {
                StrategyType.SCALPING: ScalpingStrategy,
            }

            for symbol, instrument_config in self._strategies_config.instruments.items():
                self._strategies[symbol] = {}

                for strategy_name, strategy_config in instrument_config.strategies.items():
                    if not strategy_config.enabled:
                        continue

                    self._strategies[symbol][strategy_name] = {}

                    for timeframe in strategy_config.timeframes:
                        # Create strategy instance
                        strategy_class = strategy_factory.get(strategy_config.strategy_type)
                        if strategy_class is None:
                            self._logging_service.log(
                                'WARNING',
                                'strategy_manager',
                                f"Unknown strategy type: {strategy_config.strategy_type} for {strategy_name}"
                            )
                            continue

                        strategy = strategy_class(strategy_config, symbol, timeframe)
                        self._strategies[symbol][strategy_name][timeframe] = strategy

                        self._logging_service.log(
                            'INFO',
                            'strategy_manager',
                            f"Created {strategy_name} ({strategy_config.strategy_type.value}) "
                            f"for {symbol} on {timeframe.value}"
                        )

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Failed to create strategies: {str(e)}")
            raise

    def _subscribe_to_data_events(self) -> None:
        """Subscribe to new bar events from the data sync service"""
        try:
            # Here we would connect to the event system of the data sync service
            # For now, we'll assume the data sync service has a method to add a listener
            # This is a placeholder and will need to be implemented based on the actual event system
            if hasattr(self._data_sync_service, 'add_bar_listener'):
                self._data_sync_service.add_bar_listener(self._on_new_bar)
                self._logging_service.log('INFO', 'strategy_manager', "Subscribed to new bar events")
            else:
                # If no event system exists, we'll need to implement polling or another mechanism
                self._logging_service.log(
                    'WARNING',
                    'strategy_manager',
                    "Data sync service does not support event listeners, strategies will not be evaluated automatically"
                )
        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Failed to subscribe to data events: {str(e)}")
            raise

    def _on_new_bar(self, symbol: str, timeframe: str, bar: PriceBar) -> None:
        """
        Handle new bar event from the data sync service

        Args:
            symbol: Instrument symbol
            timeframe: Timeframe of the bar
            bar: The new price bar
        """
        try:
            if symbol not in self._strategies:
                return

            # Convert timeframe string to enum
            try:
                tf_enum = TimeFrameType(timeframe)
            except ValueError:
                self._logging_service.log(
                    'WARNING',
                    'strategy_manager',
                    f"Unknown timeframe: {timeframe} for {symbol}"
                )
                return

            # Evaluate all strategies for this symbol and timeframe
            for strategy_name, strategy_dict in self._strategies[symbol].items():
                if tf_enum in strategy_dict:
                    strategy = strategy_dict[tf_enum]

                    # Evaluate the strategy
                    signal = strategy.evaluate(bar)

                    # Process signal if generated
                    if signal:
                        self._process_signal(signal)

        except Exception as e:
            self._logging_service.log(
                'ERROR',
                'strategy_manager',
                f"Error evaluating strategies for {symbol} {timeframe}: {str(e)}"
            )

    def evaluate_all_latest(self) -> List[Signal]:
        """
        Manually evaluate all strategies with the latest available data.
        Useful for testing or when automatic event system is not available.

        Returns:
            List of generated signals
        """
        signals = []

        try:
            for symbol, strategy_dict in self._strategies.items():
                for strategy_name, timeframe_dict in strategy_dict.items():
                    for timeframe, strategy in timeframe_dict.items():
                        # Get the latest bar for this symbol and timeframe
                        latest_bar = self._price_repository.get_latest_price_bar(symbol, timeframe.value)
                        if latest_bar:
                            # Evaluate the strategy
                            signal = strategy.evaluate(latest_bar)
                            if signal:
                                signals.append(signal)
                                self._process_signal(signal)

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Error evaluating all strategies: {str(e)}")

        return signals

    def add_signal_handler(self, handler: callable) -> None:
        """
        Add a signal handler function to be called when a signal is generated

        Args:
            handler: Function that takes a Signal object as parameter
        """
        self._signal_handlers.append(handler)

    def get_active_strategies(self) -> Dict[str, Dict[str, Set[TimeFrameType]]]:
        """
        Get all active strategies

        Returns:
            Dictionary mapping symbols to strategy names to sets of timeframes
        """
        result = {}

        for symbol, strategy_dict in self._strategies.items():
            result[symbol] = {}
            for strategy_name, timeframe_dict in strategy_dict.items():
                result[symbol][strategy_name] = set(timeframe_dict.keys())

        return result

    def _process_signal(self, signal: Signal) -> None:
        """
        Process a trading signal

        Args:
            signal: The signal to process
        """
        try:
            # Log the signal
            self._logging_service.log(
                'INFO',
                'strategy_manager',
                f"Signal generated: {signal}"
            )

            # Get the trade manager
            trade_manager = container.resolve(TradeManager)

            # Check if we can open more positions
            if signal.direction in ['BUY', 'SELL'] and not trade_manager.check_max_positions():
                self._logging_service.log(
                    'WARNING',
                    'strategy_manager',
                    f"Maximum positions reached, ignoring {signal.direction} signal for {signal.symbol}"
                )
                return

            # Process the signal
            result = trade_manager.process_signal(signal)

            if result:
                self._logging_service.log(
                    'INFO',
                    'strategy_manager',
                    f"Signal processed successfully: {signal.direction} {signal.symbol}"
                )
            else:
                self._logging_service.log(
                    'WARNING',
                    'strategy_manager',
                    f"Failed to process signal: {signal.direction} {signal.symbol}"
                )

            # Call all registered signal handlers
            for handler in self._signal_handlers:
                try:
                    handler(signal)
                except Exception as e:
                    self._logging_service.log(
                        'ERROR',
                        'strategy_manager',
                        f"Error in signal handler: {str(e)}"
                    )

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Error processing signal: {str(e)}")

    def perform_initial_evaluation(self) -> None:
        """
        Perform initial evaluation of all strategies using the latest available data
        """
        try:
            self._logging_service.log('INFO', 'strategy_manager', "Performing initial evaluation of all strategies")

            for symbol, strategy_dict in self._strategies.items():
                for strategy_name, timeframe_dict in strategy_dict.items():
                    for timeframe, strategy in timeframe_dict.items():
                        try:
                            # Get the latest bar for this symbol and timeframe
                            latest_bar = self._price_repository.get_latest_price_bar(symbol, timeframe.value)
                            if latest_bar:
                                self._logging_service.log(
                                    'INFO',
                                    'strategy_manager',
                                    f"Evaluating {strategy_name} for {symbol} on {timeframe.value} with latest bar"
                                )

                                # Evaluate the strategy
                                signal = strategy.evaluate(latest_bar)

                                # Process signal if generated
                                if signal:
                                    self._process_signal(signal)
                            else:
                                self._logging_service.log(
                                    'WARNING',
                                    'strategy_manager',
                                    f"No data available for {symbol} on {timeframe.value}"
                                )
                        except Exception as e:
                            self._logging_service.log(
                                'ERROR',
                                'strategy_manager',
                                f"Error evaluating {strategy_name} for {symbol} on {timeframe.value}: {str(e)}"
                            )

            self._logging_service.log('INFO', 'strategy_manager', "Initial evaluation completed")

        except Exception as e:
            self._logging_service.log('ERROR', 'strategy_manager', f"Error performing initial evaluation: {str(e)}")


# Create an instance and register in container
strategy_manager = StrategyManager()
container.register(StrategyManager, strategy_manager)