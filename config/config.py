# Strategies/config.py
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any


class StrategyType(Enum):
    BREAKOUT = "BREAKOUT"
    TREND_FOLLOWING = "TREND_FOLLOWING"
    MOMENTUM = "MOMENTUM"
    MEAN_REVERSION = "MEAN_REVERSION"
    SCALPING = "SCALPING"
    ICHIMOKU = "ICHIMOKU"
    TRIPLE_MA = "TRIPLE_MA"
    HYBRID = "HYBRID"


class TimeFrameType(Enum):
    M1 = "M1"
    M5 = "M5"
    M15 = "M15"
    M30 = "M30"
    H1 = "H1"
    H4 = "H4"
    D1 = "D1"
    W1 = "W1"
    MN1 = "MN1"


@dataclass
class IndicatorConfig:
    """Base indicator configuration"""
    enabled: bool = True


@dataclass
class MAConfig(IndicatorConfig):
    """Moving Average configuration"""
    period: int = 20
    ma_type: str = "EMA"  # "SMA", "EMA", "WMA", "TEMA", etc.
    price_type: str = "close"  # "open", "high", "low", "close", etc.


@dataclass
class MACDConfig(IndicatorConfig):
    """MACD configuration"""
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    price_type: str = "close"


@dataclass
class RSIConfig(IndicatorConfig):
    """RSI configuration"""
    period: int = 14
    price_type: str = "close"
    overbought_level: float = 70.0
    oversold_level: float = 30.0


@dataclass
class StochasticConfig(IndicatorConfig):
    """Stochastic configuration"""
    k_period: int = 14
    k_slowing: int = 3
    d_period: int = 3
    overbought_level: float = 80.0
    oversold_level: float = 20.0


@dataclass
class BollingerBandsConfig(IndicatorConfig):
    """Bollinger Bands configuration"""
    period: int = 20
    deviation: float = 2.0
    price_type: str = "close"


@dataclass
class ATRConfig(IndicatorConfig):
    """Average True Range configuration"""
    period: int = 14


@dataclass
class DonchianChannelsConfig(IndicatorConfig):
    """Donchian Channels configuration"""
    period: int = 20


@dataclass
class PivotPointsConfig(IndicatorConfig):
    """Pivot Points configuration"""
    type: str = "standard"  # "standard", "fibonacci", "camarilla", etc.
    time_frame: str = "D1"  # "D1", "W1", "MN1"


@dataclass
class ADXConfig(IndicatorConfig):
    """Average Directional Index configuration"""
    period: int = 14
    threshold: float = 25.0


@dataclass
class IchimokuConfig(IndicatorConfig):
    """Ichimoku configuration"""
    tenkan_period: int = 9
    kijun_period: int = 26
    senkou_span_b_period: int = 52
    displacement: int = 26


@dataclass
class TripleMAConfig(IndicatorConfig):
    """Triple Moving Average configuration"""
    short_period: int = 8
    medium_period: int = 21
    long_period: int = 55
    ma_type: str = "EMA"
    price_type: str = "close"


@dataclass
class CCIConfig(IndicatorConfig):
    """Commodity Channel Index configuration"""
    period: int = 20


@dataclass
class StrategyIndicatorsConfig:
    """All indicators configuration for a strategy"""
    ma: Optional[MAConfig] = None
    macd: Optional[MACDConfig] = None
    rsi: Optional[RSIConfig] = None
    stochastic: Optional[StochasticConfig] = None
    bollinger_bands: Optional[BollingerBandsConfig] = None
    atr: Optional[ATRConfig] = None
    donchian_channels: Optional[DonchianChannelsConfig] = None
    pivot_points: Optional[PivotPointsConfig] = None
    adx: Optional[ADXConfig] = None
    ichimoku: Optional[IchimokuConfig] = None
    triple_ma: Optional[TripleMAConfig] = None
    cci: Optional[CCIConfig] = None


@dataclass
class RiskManagementConfig:
    """Risk management configuration"""
    max_position_size_percent: float = 2.0  # % of account balance
    max_risk_per_trade_percent: float = 1.0  # % of account balance
    stop_loss_atr_multiplier: float = 2.0
    trailing_stop_enabled: bool = False
    trailing_stop_atr_multiplier: float = 1.0
    take_profit_atr_multiplier: float = 3.0
    take_profit_enabled: bool = True
    max_positions_per_instrument: int = 1
    max_total_positions: int = 10


@dataclass
class StrategyConfig:
    """Strategy configuration"""
    name: str
    description: str
    enabled: bool = True
    strategy_type: StrategyType = StrategyType.TREND_FOLLOWING
    timeframes: List[TimeFrameType] = field(default_factory=list)
    indicators: StrategyIndicatorsConfig = field(default_factory=StrategyIndicatorsConfig)
    risk_management: RiskManagementConfig = field(default_factory=RiskManagementConfig)
    custom_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InstrumentStrategiesConfig:
    """Strategies configuration for a specific instrument"""
    symbol: str
    description: str
    strategies: Dict[str, StrategyConfig] = field(default_factory=dict)


@dataclass
class TradingStrategiesConfig:
    """Root configuration for all trading strategies"""
    instruments: Dict[str, InstrumentStrategiesConfig] = field(default_factory=dict)
    global_risk_management: RiskManagementConfig = field(default_factory=RiskManagementConfig)


# Configuration instance based on the document
trading_strategies_config = TradingStrategiesConfig(
    global_risk_management=RiskManagementConfig(
        max_position_size_percent=2.0,
        max_risk_per_trade_percent=1.0,
        stop_loss_atr_multiplier=2.0,
        trailing_stop_enabled=True,
        trailing_stop_atr_multiplier=1.0,
        take_profit_atr_multiplier=3.0,
        take_profit_enabled=True,
        max_positions_per_instrument=1,
        max_total_positions=10
    ),
    instruments={
        "EURUSD": InstrumentStrategiesConfig(
            symbol="EURUSD",
            description="Euro vs US Dollar",
            strategies={
                "eurusd_breakout": StrategyConfig(
                    name="EURUSD Breakout Strategy",
                    description="Breakout strategy for EUR/USD",
                    enabled=True,
                    strategy_type=StrategyType.BREAKOUT,
                    timeframes=[TimeFrameType.H1, TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        donchian_channels=DonchianChannelsConfig(period=20),
                        atr=ATRConfig(period=14),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0),
                        adx=ADXConfig(period=14, threshold=25.0)
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=2.0,
                        take_profit_atr_multiplier=3.0
                    ),
                    custom_parameters={
                        "session_filter": "london_new_york",
                        "min_volatility_trigger": 1.2  # Minimum volatility trigger as ATR multiplier
                    }
                ),
                "eurusd_ichimoku": StrategyConfig(
                    name="EURUSD Ichimoku Strategy",
                    description="Ichimoku Cloud strategy for EUR/USD",
                    enabled=True,
                    strategy_type=StrategyType.ICHIMOKU,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        ichimoku=IchimokuConfig(
                            tenkan_period=9,
                            kijun_period=26,
                            senkou_span_b_period=52,
                            displacement=26
                        ),
                        adx=ADXConfig(period=14, threshold=25.0)
                    ),
                    custom_parameters={
                        "require_tenkan_kijun_cross": True,
                        "require_price_above_cloud": True,
                        "require_chikou_confirmation": True
                    }
                ),
                "eurusd_scalping": StrategyConfig(
                    name="EURUSD Scalping Strategy",
                    description="Fast MACD and RSI scalping for EUR/USD",
                    enabled=True,
                    strategy_type=StrategyType.SCALPING,
                    timeframes=[TimeFrameType.M1, TimeFrameType.M5],
                    indicators=StrategyIndicatorsConfig(
                        macd=MACDConfig(
                            fast_period=3,
                            slow_period=10,
                            signal_period=16
                        ),
                        rsi=RSIConfig(
                            period=5,
                            overbought_level=70.0,
                            oversold_level=30.0
                        )
                    ),
                    risk_management=RiskManagementConfig(
                        max_position_size_percent=1.0,
                        stop_loss_atr_multiplier=1.0,
                        take_profit_atr_multiplier=1.5
                    ),
                    custom_parameters={
                        "rapid_exit": True,
                        "max_spread_pips": 1.0,
                        "alternative_macd_settings": {
                            "fast_period": 5,
                            "slow_period": 34,
                            "signal_period": 1
                        }
                    }
                ),
                "eurusd_mean_reversion": StrategyConfig(
                    name="EURUSD Mean Reversion Strategy",
                    description="Range-bound mean reversion for EUR/USD",
                    enabled=True,
                    strategy_type=StrategyType.MEAN_REVERSION,
                    timeframes=[TimeFrameType.H1, TimeFrameType.H4],
                    indicators=StrategyIndicatorsConfig(
                        rsi=RSIConfig(period=14, overbought_level=70.0, oversold_level=30.0),
                        stochastic=StochasticConfig(
                            k_period=14,
                            k_slowing=3,
                            d_period=3,
                            overbought_level=80.0,
                            oversold_level=20.0
                        ),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0)
                    ),
                    custom_parameters={
                        "internal_bar_strength": True,
                        "ibs_threshold_low": 0.1,
                        "ibs_threshold_high": 0.9
                    }
                ),
                "eurusd_triple_ma": StrategyConfig(
                    name="EURUSD Triple MA Strategy",
                    description="Triple Moving Average trend following for EUR/USD",
                    enabled=True,
                    strategy_type=StrategyType.TRIPLE_MA,
                    timeframes=[TimeFrameType.H1, TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        triple_ma=TripleMAConfig(
                            short_period=8,
                            medium_period=21,
                            long_period=55,
                            ma_type="EMA"
                        ),
                        adx=ADXConfig(period=14, threshold=25.0),
                        macd=MACDConfig(
                            fast_period=12,
                            slow_period=26,
                            signal_period=9
                        )
                    ),
                    custom_parameters={
                        "alternative_settings": {
                            "short_period": 5,
                            "medium_period": 13,
                            "long_period": 34
                        },
                        "require_macd_confirmation": True
                    }
                )
            }
        ),
        "GBPUSD": InstrumentStrategiesConfig(
            symbol="GBPUSD",
            description="Great Britain Pound vs US Dollar",
            strategies={
                "gbpusd_breakout": StrategyConfig(
                    name="GBPUSD Breakout Strategy",
                    description="London/NY session breakouts for GBP/USD",
                    enabled=True,
                    strategy_type=StrategyType.BREAKOUT,
                    timeframes=[TimeFrameType.H1, TimeFrameType.H4],
                    indicators=StrategyIndicatorsConfig(
                        donchian_channels=DonchianChannelsConfig(period=20),
                        atr=ATRConfig(period=14),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0)
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=1.5,
                        take_profit_atr_multiplier=3.0
                    ),
                    custom_parameters={
                        "session_filter": "london",
                        "overnight_range": True
                    }
                ),
                "gbpusd_ichimoku": StrategyConfig(
                    name="GBPUSD Ichimoku Strategy",
                    description="Ichimoku Cloud strategy for GBP/USD",
                    enabled=True,
                    strategy_type=StrategyType.ICHIMOKU,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        ichimoku=IchimokuConfig(
                            tenkan_period=9,
                            kijun_period=26,
                            senkou_span_b_period=52,
                            displacement=26
                        ),
                        adx=ADXConfig(period=14, threshold=25.0)
                    ),
                    custom_parameters={
                        "alternative_settings": {
                            "tenkan_period": 20,
                            "kijun_period": 60,
                            "senkou_span_b_period": 120
                        },
                        "require_chikou_confirmation": True
                    }
                ),
                "gbpusd_scalping": StrategyConfig(
                    name="GBPUSD Scalping Strategy",
                    description="5-20 EMA crossover scalping for GBP/USD",
                    enabled=True,
                    strategy_type=StrategyType.SCALPING,
                    timeframes=[TimeFrameType.M5],
                    indicators=StrategyIndicatorsConfig(
                        ma=MAConfig(period=5, ma_type="EMA"),
                        macd=MACDConfig(
                            fast_period=5,
                            slow_period=34,
                            signal_period=1
                        ),
                        rsi=RSIConfig(period=5, overbought_level=70.0, oversold_level=30.0)
                    ),
                    risk_management=RiskManagementConfig(
                        max_position_size_percent=1.0,
                        stop_loss_atr_multiplier=1.0,
                        take_profit_atr_multiplier=1.5
                    ),
                    custom_parameters={
                        "ema_values": [5, 20, 55],
                        "require_above_long_ema": True
                    }
                ),
                "gbpusd_mean_reversion": StrategyConfig(
                    name="GBPUSD Mean Reversion Strategy",
                    description="RSI and Stochastic range trading for GBP/USD",
                    enabled=True,
                    strategy_type=StrategyType.MEAN_REVERSION,
                    timeframes=[TimeFrameType.H1, TimeFrameType.H4],
                    indicators=StrategyIndicatorsConfig(
                        rsi=RSIConfig(period=14, overbought_level=70.0, oversold_level=30.0),
                        stochastic=StochasticConfig(
                            k_period=14,
                            k_slowing=3,
                            d_period=3,
                            overbought_level=80.0,
                            oversold_level=20.0
                        ),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0)
                    ),
                    custom_parameters={
                        "internal_bar_strength": True,
                        "ibs_threshold_low": 0.1,
                        "ibs_threshold_high": 0.9,
                        "big_round_numbers": [1.2500, 1.3000, 1.3500, 1.4000, 1.4500]
                    }
                ),
                "gbpusd_triple_ma": StrategyConfig(
                    name="GBPUSD Triple MA Strategy",
                    description="Triple MA with longer periods for GBP/USD",
                    enabled=True,
                    strategy_type=StrategyType.TRIPLE_MA,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        triple_ma=TripleMAConfig(
                            short_period=13,
                            medium_period=34,
                            long_period=89,
                            ma_type="EMA"
                        ),
                        adx=ADXConfig(period=14, threshold=25.0)
                    ),
                    custom_parameters={}
                )
            }
        ),
        "USDJPY": InstrumentStrategiesConfig(
            symbol="USDJPY",
            description="US Dollar vs Japanese Yen",
            strategies={
                "usdjpy_ichimoku": StrategyConfig(
                    name="USDJPY Ichimoku Strategy",
                    description="Ichimoku Cloud strategy for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.ICHIMOKU,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        ichimoku=IchimokuConfig(
                            tenkan_period=9,
                            kijun_period=26,
                            senkou_span_b_period=52,
                            displacement=26
                        ),
                        atr=ATRConfig(period=14)
                    ),
                    custom_parameters={
                        "require_kumo_breakout": True,
                        "min_breakout_distance_atr": 0.5,
                        "require_chikou_confirmation": True
                    }
                ),
                "usdjpy_breakout": StrategyConfig(
                    name="USDJPY Opening Range Breakout",
                    description="Tokyo/NY session opening range breakout for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.BREAKOUT,
                    timeframes=[TimeFrameType.M15, TimeFrameType.H1],
                    indicators=StrategyIndicatorsConfig(
                        atr=ATRConfig(period=14),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0)
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=1.5
                    ),
                    custom_parameters={
                        "session": "tokyo_ny_overlap",  # 8-12 UTC
                        "range_minutes": 60,  # First hour range
                        "require_bb_expansion": True
                    }
                ),
                "usdjpy_scalping": StrategyConfig(
                    name="USDJPY Momentum Scalping",
                    description="Fast MACD and momentum scalping for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.SCALPING,
                    timeframes=[TimeFrameType.M1],
                    indicators=StrategyIndicatorsConfig(
                        macd=MACDConfig(
                            fast_period=3,
                            slow_period=10,
                            signal_period=16
                        ),
                        ma=MAConfig(period=5, ma_type="EMA"),
                        rsi=RSIConfig(period=5)
                    ),
                    risk_management=RiskManagementConfig(
                        max_position_size_percent=0.5,
                        stop_loss_atr_multiplier=0.5,
                        take_profit_atr_multiplier=1.0
                    ),
                    custom_parameters={
                        "ema_values": [5, 20],
                        "require_equity_filter": True,
                        "require_tick_volume_spike": True,
                        "tick_volume_threshold": 1.5  # Times average
                    }
                ),
                "usdjpy_range": StrategyConfig(
                    name="USDJPY Range Trading",
                    description="RSI and Bollinger range trading for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.MEAN_REVERSION,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        rsi=RSIConfig(period=14, overbought_level=70.0, oversold_level=30.0),
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0),
                        ma=MAConfig(period=100, ma_type="EMA")
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=1.5
                    ),
                    custom_parameters={
                        "momentum_filter": True,
                        "momentum_period": 10,
                        "momentum_threshold": -100,
                        "channel_ranges": [[105, 115]]
                    }
                ),
                "usdjpy_triple_ma": StrategyConfig(
                    name="USDJPY Triple MA Strategy",
                    description="Slower Triple MA trend following for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.TRIPLE_MA,
                    timeframes=[TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        triple_ma=TripleMAConfig(
                            short_period=21,
                            medium_period=50,
                            long_period=100,
                            ma_type="EMA"
                        ),
                        adx=ADXConfig(period=14, threshold=25.0)
                    ),
                    custom_parameters={
                        "use_t3_ma": True,  # T3 Moving Average option
                        "t3_period": 50,
                        "t3_vfactor": 0.7
                    }
                ),
                "usdjpy_carry": StrategyConfig(
                    name="USDJPY Carry Trade",
                    description="Fundamental carry trade for USD/JPY",
                    enabled=True,
                    strategy_type=StrategyType.HYBRID,
                    timeframes=[TimeFrameType.D1, TimeFrameType.W1],
                    indicators=StrategyIndicatorsConfig(
                        ma=MAConfig(period=100, ma_type="EMA")
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=1.5
                    ),
                    custom_parameters={
                        "yield_differential_threshold": 0.5,  # Minimum yield diff in %
                        "boj_dovish_bias": True,
                        "us_bond_yield_rising": True
                    }
                )
            }
        ),
        "XAUUSD": InstrumentStrategiesConfig(
            symbol="XAUUSD",
            description="Gold vs US Dollar",
            strategies={
                "xauusd_breakout": StrategyConfig(
                    name="XAUUSD Breakout Strategy",
                    description="Bollinger Band breakout for Gold",
                    enabled=True,
                    strategy_type=StrategyType.BREAKOUT,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        bollinger_bands=BollingerBandsConfig(period=50, deviation=2.0),
                        donchian_channels=DonchianChannelsConfig(period=10),
                        macd=MACDConfig(
                            fast_period=5,
                            slow_period=34,
                            signal_period=1
                        ),
                        rsi=RSIConfig(period=14)
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=2.0,
                        take_profit_atr_multiplier=4.0
                    ),
                    custom_parameters={
                        "squeeze_period": 5,  # Days of narrow BB before breakout
                        "min_squeeze_width": 0.5,  # % of normal width
                        "require_fast_macd": True
                    }
                ),
                "xauusd_mean_reversion": StrategyConfig(
                    name="XAUUSD Mean Reversion Strategy",
                    description="RSI mean reversion for Gold",
                    enabled=True,
                    strategy_type=StrategyType.MEAN_REVERSION,
                    timeframes=[TimeFrameType.H4, TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        rsi=RSIConfig(period=14, overbought_level=70.0, oversold_level=30.0),
                        ma=MAConfig(period=50, ma_type="EMA")
                    ),
                    risk_management=RiskManagementConfig(
                        stop_loss_atr_multiplier=1.5,
                        take_profit_atr_multiplier=3.0
                    ),
                    custom_parameters={
                        "fib_levels": [0.382, 0.500, 0.618],
                        "use_technical_support": True
                    }
                ),
                "xauusd_ichimoku": StrategyConfig(
                    name="XAUUSD Ichimoku Strategy",
                    description="Ichimoku Cloud strategy for Gold",
                    enabled=True,
                    strategy_type=StrategyType.ICHIMOKU,
                    timeframes=[TimeFrameType.D1],
                    indicators=StrategyIndicatorsConfig(
                        ichimoku=IchimokuConfig(
                            tenkan_period=9,
                            kijun_period=26,
                            senkou_span_b_period=52,
                            displacement=26
                        )
                    ),
                    custom_parameters={
                        "real_yield_filter": True,
                        "max_real_yield_long": 0.0,  # Only go long if real yield < 0
                        "min_real_yield_short": 0.5  # Only go short if real yield > 0.5
                    }
                ),
                "xauusd_momentum_scalping": StrategyConfig(
                    name="XAUUSD Momentum Scalping",
                    description="Bollinger Band and MACD scalping for Gold",
                    enabled=True,
                    strategy_type=StrategyType.SCALPING,
                    timeframes=[TimeFrameType.M5, TimeFrameType.M15],
                    indicators=StrategyIndicatorsConfig(
                        bollinger_bands=BollingerBandsConfig(period=20, deviation=2.0),
                        macd=MACDConfig(
                            fast_period=3,
                            slow_period=10,
                            signal_period=16
                        ),
                        rsi=RSIConfig(period=5, overbought_level=80.0, oversold_level=20.0)
                    ),
                    risk_management=RiskManagementConfig(
                        max_position_size_percent=0.5,
                        stop_loss_atr_multiplier=0.5,
                        take_profit_atr_multiplier=1.0
                    ),
                    custom_parameters={
                        "use_keltner_channels": True,
                        "keltner_period": 20,
                        "keltner_atr_multiplier": 1.5
                    }
                )
            }
        )
    }
)
