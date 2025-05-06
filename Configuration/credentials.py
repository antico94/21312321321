# Config/credentials.py

# SQL Server configuration
SQL_SERVER = r"localhost"
SQL_DATABASE = "TestDB"
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
USE_WINDOWS_AUTH = False
SQL_USERNAME = "app_user"  # Only used if USE_WINDOWS_AUTH is False
SQL_PASSWORD = "password01!"  # Only used if USE_WINDOWS_AUTH is False

# MetaTrader5 credentials (for future use)
MT5_SERVER = "FusionMarkets-Demo"
MT5_LOGIN = 166774
MT5_PASSWORD = "O11e7nqlX."
MT5_TIMEOUT = 60000