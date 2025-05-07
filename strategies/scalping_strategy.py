# strategies/scalping_strategy.py
from datetime import datetime
from typing import Optional, Dict, List, Tuple

import numpy as np

from database.models import PriceBar
from strategies.base_strategy import BaseStrategy, Signal
from strategies.config import StrategyConfig, TimeFrameType
from strategies.indicator_utils import MAType


class ScalpingStrategy(BaseStrategy):
    """
    Scalping strategy implementation using MACD and RSI indicators.
    """

    def __init__(self, config: StrategyConfig, symbol: str, timeframe: TimeFrameType):
        """
        Initialize the scalping strategy.

        Args:
            config: Strategy configuration
            symbol: Trading instrument symbol
            timeframe: Trading timeframe
        """
        super().__init__(config, symbol, timeframe)

        # Extract indicators configuration
        self.macd_config = config.indicators.macd
        self.rsi_config = config.indicators.rsi
        self.ma_config = config.indicators.ma

        # Set default values if configs are None
        if self.macd_config is None:
            self.macd_fast_period = 12
            self.macd_slow_period = 26
            self.macd_signal_period = 9
        else:
            self.macd_fast_period = self.macd_config.fast_period
            self.macd_slow_period = self.macd_config.slow_period
            self.macd_signal_period = self.macd_config.signal_period

        if self.rsi_config is None:
            self.rsi_period = 14
            self.rsi_overbought = 70.0
            self.rsi_oversold = 30.0
        else:
            self.rsi_period = self.rsi_config.period
            self.rsi_overbought = self.rsi_config.overbought_level
            self.rsi_oversold = self.rsi_config.oversold_level

        if self.ma_config is None:
            self.ma_period = 20
            self.ma_type = MAType.EMA
        else:
            self.ma_period = self.ma_config.period
            self.ma_type = getattr(MAType, self.ma_config.ma_type)

        # Extract custom parameters
        self.custom_params = config.custom_parameters
        self.ema_values = self.custom_params.get("ema_values", [5, 20, 55])
        self.require_above_long_ema = self.custom_params.get("require_above_long_ema", False)
        self.rapid_exit = self.custom_params.get("rapid_exit", False)
        self.max_spread_pips = self.custom_params.get("max_spread_pips", 2.0)
        self.require_volume_confirmation = self.custom_params.get("require_tick_volume_spike", False)
        self.volume_threshold = self.custom_params.get("tick_volume_threshold", 1.5)

        # Alternative MACD settings if provided
        alt_macd = self.custom_params.get("alternative_macd_settings", None)
        if alt_macd:
            self.macd_fast_period = alt_macd.get("fast_period", self.macd_fast_period)
            self.macd_slow_period = alt_macd.get("slow_period", self.macd_slow_period)
            self.macd_signal_period = alt_macd.get("signal_period", self.macd_signal_period)

        # ATR for stop loss and take profit calculations
        self.atr_period = 14  # Default
        if config.indicators.atr is not None:
            self.atr_period = config.indicators.atr.period

        # Risk parameters
        self.stop_loss_atr_multiplier = config.risk_management.stop_loss_atr_multiplier
        self.take_profit_atr_multiplier = config.risk_management.take_profit_atr_multiplier

        # Track last signal to avoid repeated signals
        self.last_signal_type = None

        self.log('INFO',
                 f"Initialized ScalpingStrategy with MACD({self.macd_fast_period},{self.macd_slow_period},{self.macd_signal_period}), "
                 f"RSI({self.rsi_period}), EMAs={self.ema_values}")

    def evaluate(self, new_bar: PriceBar) -> Optional[Signal]:
        """
        Evaluate the strategy for a new price bar.

        Args:
            new_bar: The newly arrived price bar

        Returns:
            A Signal object if a trade signal is generated, None otherwise
        """
        try:
            # Get historical bars (including the new one)
            # We need enough bars to calculate all indicators
            min_bars_needed = max(
                self.macd_slow_period + self.macd_signal_period,
                self.rsi_period,
                self.ma_period,
                self.atr_period,
                max(self.ema_values) if self.ema_values else 0
            ) + 10  # Add some buffer

            bars = self.get_price_bars(min_bars_needed)
            if len(bars) < min_bars_needed:
                self.log('WARNING', f"Not enough bars to calculate indicators. Need {min_bars_needed}, got {len(bars)}")
                return None

            # Extract OHLCV data
            ohlcv = self.extract_ohlcv(bars)
            open_prices = ohlcv['open']
            high_prices = ohlcv['high']
            low_prices = ohlcv['low']
            close_prices = ohlcv['close']
            volumes = ohlcv['volume']
            timestamps = ohlcv['timestamp']

            # Check if spread exceeds maximum allowed
            if hasattr(new_bar, 'spread') and new_bar.spread > self.max_spread_pips:
                self.log('INFO', f"Spread too high: {new_bar.spread} > {self.max_spread_pips}")
                return None

            # Calculate indicators
            # MACD
            macd_line, macd_signal, macd_histogram = self._indicators.macd(
                close_prices, self.macd_fast_period, self.macd_slow_period, self.macd_signal_period
            )

            # RSI
            rsi_values = self._indicators.rsi(close_prices, self.rsi_period)

            # Moving Averages
            ma_values = self._indicators.moving_average(close_prices, self.ma_period, self.ma_type)

            # EMAs for the specified periods
            ema_values = {}
            for period in self.ema_values:
                ema_values[period] = self._indicators.moving_average(close_prices, period, MAType.EMA)

            # ATR for stop loss and take profit
            atr_values = self._indicators.atr(high_prices, low_prices, close_prices, self.atr_period)

            # Volume Confirmation (using previous completed bar volume, not the current one)
            volume_confirmed = True  # Default if no volume confirmation required
            if self.require_volume_confirmation and len(volumes) > 1:
                # Calculate average volume over the last 20 bars, excluding the most recent
                avg_volume = np.mean(volumes[-21:-1]) if len(volumes) >= 21 else np.mean(volumes[:-1])

                # Check if the previous bar's volume exceeds the threshold
                prev_volume = volumes[-2]  # Previous bar volume, not current
                volume_confirmed = prev_volume > (avg_volume * self.volume_threshold)

                self.log('INFO', f"Volume check: prev_volume={prev_volume:.2f}, avg_volume={avg_volume:.2f}, "
                                 f"threshold={avg_volume * self.volume_threshold:.2f}, confirmed={volume_confirmed}")

            # Detect crossovers (current bar vs previous bar)
            macd_crosses_above = False
            macd_crosses_below = False

            if len(macd_line) >= 2 and len(macd_signal) >= 2:
                macd_crosses_above = (macd_line[-2] <= macd_signal[-2]) and (macd_line[-1] > macd_signal[-1])
                macd_crosses_below = (macd_line[-2] >= macd_signal[-2]) and (macd_line[-1] < macd_signal[-1])

            # Prepare condition groups for detailed logging
            condition_groups = {
                "Bullish Scalping Conditions": [
                    ("MACD crossed above Signal", macd_crosses_above,
                     f"MACD[-2]={macd_line[-2]:.5f}, Signal[-2]={macd_signal[-2]:.5f}, "
                     f"MACD[-1]={macd_line[-1]:.5f}, Signal[-1]={macd_signal[-1]:.5f}"),

                    ("MACD above Signal", macd_line[-1] > macd_signal[-1],
                     f"MACD: {macd_line[-1]:.5f}, Signal: {macd_signal[-1]:.5f}"),

                    ("Positive MACD Histogram", macd_histogram[-1] > 0,
                     f"Histogram: {macd_histogram[-1]:.5f}"),

                    ("RSI not overbought", rsi_values[-1] < self.rsi_overbought,
                     f"RSI: {rsi_values[-1]:.2f}, Threshold: {self.rsi_overbought:.2f}"),
                ],

                "EMA Conditions": []
            }

            # Add EMA conditions
            if len(self.ema_values) >= 2:
                short_ema = ema_values[self.ema_values[0]]
                medium_ema = ema_values[self.ema_values[1]]

                condition_groups["EMA Conditions"].append(
                    (f"Short EMA > Medium EMA", short_ema[-1] > medium_ema[-1],
                     f"EMA{self.ema_values[0]}: {short_ema[-1]:.5f}, EMA{self.ema_values[1]}: {medium_ema[-1]:.5f}")
                )

                # Check if price is above short EMA
                condition_groups["EMA Conditions"].append(
                    (f"Price above Short EMA", close_prices[-1] > short_ema[-1],
                     f"Price: {close_prices[-1]:.5f}, EMA{self.ema_values[0]}: {short_ema[-1]:.5f}")
                )

                # Check if we have a long EMA and need to check it
                if len(self.ema_values) >= 3 and self.require_above_long_ema:
                    long_ema = ema_values[self.ema_values[2]]
                    condition_groups["EMA Conditions"].append(
                        (f"Price above Long EMA", close_prices[-1] > long_ema[-1],
                         f"Price: {close_prices[-1]:.5f}, EMA{self.ema_values[2]}: {long_ema[-1]:.5f}")
                    )

            # Add volume condition if required
            if self.require_volume_confirmation:
                condition_groups["Additional Conditions"] = [
                    ("Volume Confirmation", volume_confirmed,
                     f"Prev Vol: {volumes[-2]:.2f}, Avg Vol: {avg_volume:.2f}, "
                     f"Threshold: {avg_volume * self.volume_threshold:.2f}")
                ]

            # Log all conditions
            self.log_conditions("BUY SIGNAL", condition_groups)

            # Check for bullish signal
            bullish_signal = (
                    macd_crosses_above and
                    macd_line[-1] > macd_signal[-1] and
                    macd_histogram[-1] > 0 and
                    rsi_values[-1] < self.rsi_overbought and
                    volume_confirmed
            )

            # Add EMA checks if configured
            if len(self.ema_values) >= 2:
                short_ema = ema_values[self.ema_values[0]]
                medium_ema = ema_values[self.ema_values[1]]

                # Short EMA should be above medium EMA
                bullish_signal = bullish_signal and short_ema[-1] > medium_ema[-1]

                # Check if price is above short EMA
                bullish_signal = bullish_signal and close_prices[-1] > short_ema[-1]

                # Check if we need to check long EMA
                if len(self.ema_values) >= 3 and self.require_above_long_ema:
                    long_ema = ema_values[self.ema_values[2]]
                    bullish_signal = bullish_signal and close_prices[-1] > long_ema[-1]

            # Generate bullish signal if conditions are met and we don't have an active buy signal
            if bullish_signal and self.last_signal_type != "BUY":
                entry_price = close_prices[-1]
                stop_loss = entry_price - (atr_values[-1] * self.stop_loss_atr_multiplier)
                take_profit = entry_price + (atr_values[-1] * self.take_profit_atr_multiplier)

                self.log('INFO',
                         f"Bullish signal generated at {entry_price:.5f}, SL: {stop_loss:.5f}, TP: {take_profit:.5f}")

                # Update last signal type
                self.last_signal_type = "BUY"

                return Signal(
                    symbol=self.symbol,
                    direction="BUY",
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    strategy_name=self.name,
                    timeframe=self.timeframe.value,
                    timestamp=datetime.now(),
                    custom_data={
                        "macd": macd_line[-1],
                        "macd_signal": macd_signal[-1],
                        "rsi": rsi_values[-1],
                        "atr": atr_values[-1]
                    }
                )

            # Check for bearish signal conditions
            condition_groups = {
                "Bearish Scalping Conditions": [
                    ("MACD crossed below Signal", macd_crosses_below,
                     f"MACD[-2]={macd_line[-2]:.5f}, Signal[-2]={macd_signal[-2]:.5f}, "
                     f"MACD[-1]={macd_line[-1]:.5f}, Signal[-1]={macd_signal[-1]:.5f}"),

                    ("MACD below Signal", macd_line[-1] < macd_signal[-1],
                     f"MACD: {macd_line[-1]:.5f}, Signal: {macd_signal[-1]:.5f}"),

                    ("Negative MACD Histogram", macd_histogram[-1] < 0,
                     f"Histogram: {macd_histogram[-1]:.5f}"),

                    ("RSI not oversold", rsi_values[-1] > self.rsi_oversold,
                     f"RSI: {rsi_values[-1]:.2f}, Threshold: {self.rsi_oversold:.2f}"),
                ],

                "EMA Conditions": []
            }

            # Add EMA conditions for bearish signal
            if len(self.ema_values) >= 2:
                short_ema = ema_values[self.ema_values[0]]
                medium_ema = ema_values[self.ema_values[1]]

                condition_groups["EMA Conditions"].append(
                    (f"Short EMA < Medium EMA", short_ema[-1] < medium_ema[-1],
                     f"EMA{self.ema_values[0]}: {short_ema[-1]:.5f}, EMA{self.ema_values[1]}: {medium_ema[-1]:.5f}")
                )

                # Check if price is below short EMA
                condition_groups["EMA Conditions"].append(
                    (f"Price below Short EMA", close_prices[-1] < short_ema[-1],
                     f"Price: {close_prices[-1]:.5f}, EMA{self.ema_values[0]}: {short_ema[-1]:.5f}")
                )

                # Check if we have a long EMA and need to check it
                if len(self.ema_values) >= 3 and self.require_above_long_ema:
                    long_ema = ema_values[self.ema_values[2]]
                    condition_groups["EMA Conditions"].append(
                        (f"Price below Long EMA", close_prices[-1] < long_ema[-1],
                         f"Price: {close_prices[-1]:.5f}, EMA{self.ema_values[2]}: {long_ema[-1]:.5f}")
                    )

            # Add volume condition if required
            if self.require_volume_confirmation:
                condition_groups["Additional Conditions"] = [
                    ("Volume Confirmation", volume_confirmed,
                     f"Prev Vol: {volumes[-2]:.2f}, Avg Vol: {avg_volume:.2f}, "
                     f"Threshold: {avg_volume * self.volume_threshold:.2f}")
                ]

            # Log all conditions
            self.log_conditions("SELL SIGNAL", condition_groups)

            # Check for bearish signal
            bearish_signal = (
                    macd_crosses_below and
                    macd_line[-1] < macd_signal[-1] and
                    macd_histogram[-1] < 0 and
                    rsi_values[-1] > self.rsi_oversold and
                    volume_confirmed
            )

            # Add EMA checks if configured
            if len(self.ema_values) >= 2:
                short_ema = ema_values[self.ema_values[0]]
                medium_ema = ema_values[self.ema_values[1]]

                # Short EMA should be below medium EMA
                bearish_signal = bearish_signal and short_ema[-1] < medium_ema[-1]

                # Check if price is below short EMA
                bearish_signal = bearish_signal and close_prices[-1] < short_ema[-1]

                # Check if we need to check long EMA
                if len(self.ema_values) >= 3 and self.require_above_long_ema:
                    long_ema = ema_values[self.ema_values[2]]
                    bearish_signal = bearish_signal and close_prices[-1] < long_ema[-1]

            # Generate bearish signal if conditions are met and we don't have an active sell signal
            if bearish_signal and self.last_signal_type != "SELL":
                entry_price = close_prices[-1]
                stop_loss = entry_price + (atr_values[-1] * self.stop_loss_atr_multiplier)
                take_profit = entry_price - (atr_values[-1] * self.take_profit_atr_multiplier)

                self.log('INFO',
                         f"Bearish signal generated at {entry_price:.5f}, SL: {stop_loss:.5f}, TP: {take_profit:.5f}")

                # Update last signal type
                self.last_signal_type = "SELL"

                return Signal(
                    symbol=self.symbol,
                    direction="SELL",
                    entry_price=entry_price,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    strategy_name=self.name,
                    timeframe=self.timeframe.value,
                    timestamp=datetime.now(),
                    custom_data={
                        "macd": macd_line[-1],
                        "macd_signal": macd_signal[-1],
                        "rsi": rsi_values[-1],
                        "atr": atr_values[-1]
                    }
                )

            # Check for exit signals
            if self.last_signal_type == "BUY":
                exit_signal = False
                exit_reason = ""

                # Rapid exit conditions
                if self.rapid_exit:
                    if macd_crosses_below:
                        exit_signal = True
                        exit_reason = "Rapid Exit: MACD crossed below signal line"
                    elif len(self.ema_values) >= 2:
                        short_ema = ema_values[self.ema_values[0]]
                        medium_ema = ema_values[self.ema_values[1]]
                        if short_ema[-1] < medium_ema[-1]:
                            exit_signal = True
                            exit_reason = "Rapid Exit: Short EMA crossed below Medium EMA"
                # Standard exit conditions
                else:
                    if macd_crosses_below:
                        exit_signal = True
                        exit_reason = "Standard Exit: MACD crossed below signal line"

                if exit_signal:
                    self.log('INFO', f"Exit signal for BUY position: {exit_reason}")
                    self.last_signal_type = None

                    return Signal(
                        symbol=self.symbol,
                        direction="CLOSE",
                        entry_price=None,
                        stop_loss=None,
                        take_profit=None,
                        strategy_name=self.name,
                        timeframe=self.timeframe.value,
                        timestamp=datetime.now(),
                        custom_data={"reason": exit_reason}
                    )

            # Similar logic for SELL exit signals
            if self.last_signal_type == "SELL":
                exit_signal = False
                exit_reason = ""

                # Rapid exit conditions
                if self.rapid_exit:
                    if macd_crosses_above:
                        exit_signal = True
                        exit_reason = "Rapid Exit: MACD crossed above signal line"
                    elif len(self.ema_values) >= 2:
                        short_ema = ema_values[self.ema_values[0]]
                        medium_ema = ema_values[self.ema_values[1]]
                        if short_ema[-1] > medium_ema[-1]:
                            exit_signal = True
                            exit_reason = "Rapid Exit: Short EMA crossed above Medium EMA"
                # Standard exit conditions
                else:
                    if macd_crosses_above:
                        exit_signal = True
                        exit_reason = "Standard Exit: MACD crossed above signal line"

                if exit_signal:
                    self.log('INFO', f"Exit signal for SELL position: {exit_reason}")
                    self.last_signal_type = None

                    return Signal(
                        symbol=self.symbol,
                        direction="CLOSE",
                        entry_price=None,
                        stop_loss=None,
                        take_profit=None,
                        strategy_name=self.name,
                        timeframe=self.timeframe.value,
                        timestamp=datetime.now(),
                        custom_data={"reason": exit_reason}
                    )

            # No signal generated
            return None

        except Exception as e:
            self.log('ERROR', f"Error evaluating strategy: {str(e)}")
            import traceback
            self.log('ERROR', f"Traceback: {traceback.format_exc()}")
            return None
