# trade/trade_manager.py
from typing import Dict, List, Optional, Any
from datetime import datetime
import MetaTrader5 as mt5
import time

from container import container
from log_service.logger import LoggingService
from config.app_config import AppConfig
from strategies.base_strategy import Signal
from mt5_service.connection import MT5ConnectionService


class TradeManager:
    """
    Trade Manager service for executing orders and managing positions.

    Responsible for:
    - Signal processing
    - Order execution
    - Position tracking
    - Risk management
    """

    def __init__(self):
        """Initialize the trade manager"""
        self._initialized = False
        self._logging_service = None
        self._app_config = None
        self._mt5_connection = None
        self._open_positions = {}  # {symbol: {ticket: position_data}}
        self._pending_orders = {}  # {symbol: {ticket: order_data}}
        self._position_history = {}  # {symbol: [position_history]}

    def initialize(self) -> bool:
        """
        Initialize the trade manager

        Returns:
            bool: True if initialization is successful, False otherwise
        """
        if self._initialized:
            return True

        try:
            # Get dependencies
            self._logging_service = container.resolve(LoggingService)
            self._app_config = container.resolve(AppConfig)
            self._mt5_connection = container.resolve(MT5ConnectionService)

            # Ensure MT5 connection
            if not self._mt5_connection.ensure_connection():
                raise Exception("MT5 connection not established")

            # Initialize position tracking
            self._sync_positions()

            self._initialized = True
            self._logging_service.log('INFO', 'trade_manager', "Trade manager initialized")
            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Trade manager initialization failed: {str(e)}")
            raise

    def _sync_positions(self) -> None:
        """Synchronize current open positions from MT5"""
        try:
            # Get all open positions
            positions = mt5.positions_get()
            if positions is None:
                self._logging_service.log('WARNING', 'trade_manager', "No positions to sync")
                self._open_positions = {}
                return

            # Clear existing tracked positions
            self._open_positions = {}

            # Process each position
            for position in positions:
                symbol = position.symbol

                if symbol not in self._open_positions:
                    self._open_positions[symbol] = {}

                self._open_positions[symbol][position.ticket] = {
                    'ticket': position.ticket,
                    'symbol': symbol,
                    'type': 'BUY' if position.type == mt5.POSITION_TYPE_BUY else 'SELL',
                    'volume': position.volume,
                    'open_price': position.price_open,
                    'current_price': position.price_current,
                    'sl': position.sl,
                    'tp': position.tp,
                    'profit': position.profit,
                    'open_time': datetime.fromtimestamp(position.time),
                    'magic': position.magic,
                    'comment': position.comment
                }

            self._logging_service.log('INFO', 'trade_manager', f"Synced {len(positions)} open positions")

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Failed to sync positions: {str(e)}")

    def process_signal(self, signal: Signal) -> bool:
        """
        Process a trading signal

        Args:
            signal: The signal to process

        Returns:
            bool: True if the signal was processed successfully, False otherwise
        """
        try:
            if not self._initialized:
                self.initialize()

            # Ensure MT5 connection
            if not self._mt5_connection.ensure_connection():
                self._logging_service.log('ERROR', 'trade_manager', "MT5 connection lost, cannot process signal")
                return False

            # Process based on signal direction
            if signal.direction == "BUY":
                return self._execute_buy(signal)
            elif signal.direction == "SELL":
                return self._execute_sell(signal)
            elif signal.direction == "CLOSE":
                return self._close_positions(signal)
            else:
                self._logging_service.log('WARNING', 'trade_manager', f"Unknown signal direction: {signal.direction}")
                return False

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Error processing signal: {str(e)}")
            return False

    def _execute_buy(self, signal: Signal) -> bool:
        """
        Execute a buy order

        Args:
            signal: The buy signal

        Returns:
            bool: True if the order was executed successfully, False otherwise
        """
        try:
            # Check if we already have an open position for this symbol
            if signal.symbol in self._open_positions and len(self._open_positions[signal.symbol]) > 0:
                existing_positions = [p for p in self._open_positions[signal.symbol].values()
                                      if p['type'] == 'BUY']
                if existing_positions:
                    self._logging_service.log('INFO', 'trade_manager',
                                              f"Already have {len(existing_positions)} open BUY positions for {signal.symbol}")
                    return False

            # Calculate position size based on risk management
            account_info = mt5.account_info()
            if account_info is None:
                self._logging_service.log('ERROR', 'trade_manager', "Failed to get account info")
                return False

            balance = account_info.balance

            # Get global risk management settings
            global_risk = self._app_config.trading_strategies.global_risk_management

            # Calculate position volume
            risk_percent = global_risk.max_risk_per_trade_percent
            risk_amount = balance * (risk_percent / 100.0)

            # Get symbol info
            symbol_info = mt5.symbol_info(signal.symbol)
            if symbol_info is None:
                self._logging_service.log('ERROR', 'trade_manager', f"Failed to get symbol info for {signal.symbol}")
                return False

            pip_value = 0.0001  # Default for most forex pairs

            # Find the configured pip value for this symbol
            if signal.symbol in self._app_config.trading.instruments:
                pip_value = self._app_config.trading.instruments[signal.symbol].pip_value

            price = symbol_info.ask
            stop_loss_distance = abs(price - signal.stop_loss)

            # Calculate volume in lots
            point_value = symbol_info.trade_tick_value / symbol_info.trade_tick_size
            stop_loss_points = stop_loss_distance / symbol_info.point
            money_per_point = risk_amount / stop_loss_points
            volume_lots = money_per_point / point_value

            # Round to the nearest lot step
            lot_step = symbol_info.volume_step
            volume_lots = round(volume_lots / lot_step) * lot_step

            # Ensure volume is within min/max limits
            volume_lots = max(symbol_info.volume_min, min(symbol_info.volume_max, volume_lots))

            # Create the order request
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": signal.symbol,
                "volume": volume_lots,
                "type": mt5.ORDER_TYPE_BUY,
                "price": price,
                "sl": signal.stop_loss,
                "tp": signal.take_profit,
                "deviation": 20,  # Allow price slippage of 2 pips
                "magic": 123456,  # A unique identifier for this strategy
                "comment": f"{signal.strategy_name} {signal.timeframe}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            # Log the order details
            self._logging_service.log('INFO', 'trade_manager',
                                      f"Sending BUY order for {signal.symbol}: Volume={volume_lots}, "
                                      f"Price={price}, SL={signal.stop_loss}, TP={signal.take_profit}")

            # Send the order
            result = mt5.order_send(request)

            # Check if the order was successful
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                self._logging_service.log('ERROR', 'trade_manager',
                                          f"Order failed: {result.retcode}, {mt5.last_error()}")
                return False

            # Log success
            self._logging_service.log('INFO', 'trade_manager',
                                      f"Buy order executed successfully: Ticket={result.order}, Volume={volume_lots}")

            # Update position tracking
            self._sync_positions()

            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Error executing buy order: {str(e)}")
            return False

    def _execute_sell(self, signal: Signal) -> bool:
        """
        Execute a sell order

        Args:
            signal: The sell signal

        Returns:
            bool: True if the order was executed successfully, False otherwise
        """
        try:
            # Check if we already have an open position for this symbol
            if signal.symbol in self._open_positions and len(self._open_positions[signal.symbol]) > 0:
                existing_positions = [p for p in self._open_positions[signal.symbol].values()
                                      if p['type'] == 'SELL']
                if existing_positions:
                    self._logging_service.log('INFO', 'trade_manager',
                                              f"Already have {len(existing_positions)} open SELL positions for {signal.symbol}")
                    return False

            # Calculate position size based on risk management
            account_info = mt5.account_info()
            if account_info is None:
                self._logging_service.log('ERROR', 'trade_manager', "Failed to get account info")
                return False

            balance = account_info.balance

            # Get global risk management settings
            global_risk = self._app_config.trading_strategies.global_risk_management

            # Calculate position volume
            risk_percent = global_risk.max_risk_per_trade_percent
            risk_amount = balance * (risk_percent / 100.0)

            # Get symbol info
            symbol_info = mt5.symbol_info(signal.symbol)
            if symbol_info is None:
                self._logging_service.log('ERROR', 'trade_manager', f"Failed to get symbol info for {signal.symbol}")
                return False

            pip_value = 0.0001  # Default for most forex pairs

            # Find the configured pip value for this symbol
            if signal.symbol in self._app_config.trading.instruments:
                pip_value = self._app_config.trading.instruments[signal.symbol].pip_value

            price = symbol_info.bid
            stop_loss_distance = abs(price - signal.stop_loss)

            # Calculate volume in lots
            point_value = symbol_info.trade_tick_value / symbol_info.trade_tick_size
            stop_loss_points = stop_loss_distance / symbol_info.point
            money_per_point = risk_amount / stop_loss_points
            volume_lots = money_per_point / point_value

            # Round to the nearest lot step
            lot_step = symbol_info.volume_step
            volume_lots = round(volume_lots / lot_step) * lot_step

            # Ensure volume is within min/max limits
            volume_lots = max(symbol_info.volume_min, min(symbol_info.volume_max, volume_lots))

            # Create the order request
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": signal.symbol,
                "volume": volume_lots,
                "type": mt5.ORDER_TYPE_SELL,
                "price": price,
                "sl": signal.stop_loss,
                "tp": signal.take_profit,
                "deviation": 20,  # Allow price slippage of 2 pips
                "magic": 123456,  # A unique identifier for this strategy
                "comment": f"{signal.strategy_name} {signal.timeframe}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }

            # Log the order details
            self._logging_service.log('INFO', 'trade_manager',
                                      f"Sending SELL order for {signal.symbol}: Volume={volume_lots}, "
                                      f"Price={price}, SL={signal.stop_loss}, TP={signal.take_profit}")

            # Send the order
            result = mt5.order_send(request)

            # Check if the order was successful
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                self._logging_service.log('ERROR', 'trade_manager',
                                          f"Order failed: {result.retcode}, {mt5.last_error()}")
                return False

            # Log success
            self._logging_service.log('INFO', 'trade_manager',
                                      f"Sell order executed successfully: Ticket={result.order}, Volume={volume_lots}")

            # Update position tracking
            self._sync_positions()

            return True

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Error executing sell order: {str(e)}")
            return False

    def _close_positions(self, signal: Signal) -> bool:
        """
        Close positions for a symbol

        Args:
            signal: The close signal

        Returns:
            bool: True if positions were closed successfully, False otherwise
        """
        try:
            # Check if we have open positions for this symbol
            if signal.symbol not in self._open_positions or len(self._open_positions[signal.symbol]) == 0:
                self._logging_service.log('INFO', 'trade_manager', f"No open positions to close for {signal.symbol}")
                return True  # Return true as there's nothing to close

            # Get positions to close
            positions_to_close = list(self._open_positions[signal.symbol].values())

            all_success = True

            # Close each position
            for position in positions_to_close:
                # Create close request
                position_type = position['type']
                close_type = mt5.ORDER_TYPE_SELL if position_type == 'BUY' else mt5.ORDER_TYPE_BUY

                # Get current price
                symbol_info = mt5.symbol_info(signal.symbol)
                price = symbol_info.bid if position_type == 'BUY' else symbol_info.ask

                request = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": signal.symbol,
                    "volume": position['volume'],
                    "type": close_type,
                    "position": position['ticket'],
                    "price": price,
                    "deviation": 20,  # Allow price slippage of 2 pips
                    "magic": 123456,  # A unique identifier for this strategy
                    "comment": f"Close {position_type} - {signal.custom_data.get('reason', 'Exit signal')}",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }

                # Log the close request
                self._logging_service.log('INFO', 'trade_manager',
                                          f"Closing {position_type} position {position['ticket']} for {signal.symbol}: "
                                          f"Volume={position['volume']}, Price={price}")

                # Send the order
                result = mt5.order_send(request)

                # Check if the close was successful
                if result.retcode != mt5.TRADE_RETCODE_DONE:
                    self._logging_service.log('ERROR', 'trade_manager',
                                              f"Position close failed: {result.retcode}, {mt5.last_error()}")
                    all_success = False
                else:
                    # Log success
                    self._logging_service.log('INFO', 'trade_manager',
                                              f"Position closed successfully: Ticket={position['ticket']}")

            # Update position tracking
            self._sync_positions()

            return all_success

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Error closing positions: {str(e)}")
            return False

    def update_trailing_stops(self) -> None:
        """Update trailing stops for open positions if enabled"""
        try:
            # Check if we have any open positions
            if not self._open_positions:
                return

            # Get global risk management settings
            global_risk = self._app_config.trading_strategies.global_risk_management

            # Skip if trailing stop is not enabled
            if not global_risk.trailing_stop_enabled:
                return

            # Process each symbol's positions
            for symbol, positions in self._open_positions.items():
                for ticket, position in positions.items():
                    # Skip if no stop loss
                    if position['sl'] == 0.0:
                        continue

                    current_price = position['current_price']
                    position_type = position['type']
                    current_sl = position['sl']

                    # Get symbol point value for calculations
                    symbol_info = mt5.symbol_info(symbol)
                    if symbol_info is None:
                        continue

                    # Calculate ATR for this symbol (if available)
                    # For simplicity, we'll use a fixed trailing distance based on
                    # the ATR multiplier from global risk settings
                    atr_multiplier = global_risk.trailing_stop_atr_multiplier
                    trailing_distance = 100 * symbol_info.point * atr_multiplier  # Example: 100 points * multiplier

                    # Calculate new stop loss based on position type
                    new_sl = current_sl

                    if position_type == 'BUY':
                        # For buy positions, we move stop loss up as price increases
                        min_price_for_update = current_sl + trailing_distance  # Minimum price to trigger update

                        if current_price > min_price_for_update:
                            new_sl = current_price - trailing_distance

                    else:  # SELL position
                        # For sell positions, we move stop loss down as price decreases
                        max_price_for_update = current_sl - trailing_distance  # Maximum price to trigger update

                        if current_price < max_price_for_update:
                            # Calculate how much to move the stop loss
                            new_sl = current_price + trailing_distance

                    # Update stop loss if it has changed
                    if new_sl != current_sl:
                        # Log the stop loss update
                        self._logging_service.log('INFO', 'trade_manager',
                                                  f"Updating trailing stop for {symbol} position {ticket}: "
                                                  f"Current price={current_price}, Old SL={current_sl}, New SL={new_sl}")

                        # Create the modify request
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "symbol": symbol,
                            "position": ticket,
                            "sl": new_sl,
                            "tp": position['tp']  # Keep the same take profit
                        }

                        # Send the modify request
                        result = mt5.order_send(request)

                        # Check if the modify was successful
                        if result.retcode != mt5.TRADE_RETCODE_DONE:
                            self._logging_service.log('ERROR', 'trade_manager',
                                                      f"Failed to update trailing stop: {result.retcode}, {mt5.last_error()}")

            # Update position tracking after all modifications
            self._sync_positions()

        except Exception as e:
            self._logging_service.log('ERROR', 'trade_manager', f"Error updating trailing stops: {str(e)}")

    def get_position_count(self, symbol: Optional[str] = None) -> int:
        """
        Get the number of open positions

        Args:
            symbol: Symbol to count positions for, or None for all positions

        Returns:
            int: Number of open positions
        """
        if not self._open_positions:
            return 0

        if symbol:
            return len(self._open_positions.get(symbol, {}))
        else:
            return sum(len(positions) for positions in self._open_positions.values())

    def check_max_positions(self) -> bool:
        """
        Check if we've reached the maximum allowed positions

        Returns:
            bool: True if we can open more positions, False if at maximum
        """
        # Get global risk management settings
        global_risk = self._app_config.trading_strategies.global_risk_management

        total_positions = self.get_position_count()

        return total_positions < global_risk.max_total_positions


# Create an instance and register in container
trade_manager = TradeManager()
container.register(TradeManager, trade_manager)