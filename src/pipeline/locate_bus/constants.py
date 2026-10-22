from dataclasses import dataclass
from datetime import datetime
from os.path import join

# @dataclass(frozen=True)
class Paths:

    # Current year and month for organizing data
    YER:                    str = datetime.now().strftime("%Y")
    MONTH:                  str = datetime.now().strftime("%m")
    DAY:                    str = datetime.now().strftime("%d")

    # Base path for database
    _BASE_PATH_DB:          str = "database"

    # Extracted data
    _BRONZE_MEDALION:       str = "bronze"
    _BASE_PATH_BRONZE:      str = join(_BASE_PATH_DB, _BRONZE_MEDALION)
    _BASE_PATH_YER_BRONZE:  str = join(_BASE_PATH_BRONZE, YER)
    _BASE_PATH_MONTH_BRONZE:str = join(_BASE_PATH_YER_BRONZE, f"MONTH_{MONTH}")
    _BASE_PATH_DAY_BRONZE:  str = join(_BASE_PATH_MONTH_BRONZE, f"DAY_{DAY}")
    LIST_PATH_BRONZE:       list= [_BASE_PATH_BRONZE, _BASE_PATH_YER_BRONZE, _BASE_PATH_MONTH_BRONZE, _BASE_PATH_DAY_BRONZE]
    EXTRACTED_DATA_PATH:    str = join(_BASE_PATH_DAY_BRONZE, "locate_rmtc_go_{}.json")

    # Transformed data
    _SILVER_MEDALION:       str = "silver"
    _BASE_PATH_SILVER:      str = join(_BASE_PATH_DB, _SILVER_MEDALION)
    _BASE_PATH_YER_SILVER:  str = join(_BASE_PATH_SILVER, YER)
    _BASE_PATH_MONTH_SILVER:str = join(_BASE_PATH_YER_SILVER, f"MONTH_{MONTH}")
    _BASE_PATH_DAY_SILVER:  str = join(_BASE_PATH_MONTH_SILVER, f"DAY_{DAY}")
    LIST_PATH_SILVER:       list= [_BASE_PATH_SILVER, _BASE_PATH_YER_SILVER, _BASE_PATH_MONTH_SILVER, _BASE_PATH_DAY_SILVER]
    TRANSFORMED_DATA_PATH:  str = join(_BASE_PATH_DAY_SILVER, "locate_rmtc_go_{}.parquet")

    @classmethod
    def change_data(cls, YER: str, MONTH: str, DAY: str) -> None:
        """
        Update the date-related paths based on the current date.
        """
        cls.YER                    = YER
        cls.MONTH                  = MONTH
        cls.DAY                    = DAY

        cls._BASE_PATH_YER_BRONZE  = join(cls._BASE_PATH_BRONZE, cls.YER)
        cls._BASE_PATH_MONTH_BRONZE= join(cls._BASE_PATH_YER_BRONZE, f"MONTH_{cls.MONTH}")
        cls._BASE_PATH_DAY_BRONZE  = join(cls._BASE_PATH_MONTH_BRONZE, f"DAY_{cls.DAY}")
        cls.LIST_PATH_BRONZE       = [cls._BASE_PATH_BRONZE, cls._BASE_PATH_YER_BRONZE, cls._BASE_PATH_MONTH_BRONZE, cls._BASE_PATH_DAY_BRONZE]
        cls.EXTRACTED_DATA_PATH    = join(cls._BASE_PATH_DAY_BRONZE, "locate_rmtc_go_{}.json")

        cls._BASE_PATH_YER_SILVER  = join(cls._BASE_PATH_SILVER, cls.YER)
        cls._BASE_PATH_MONTH_SILVER= join(cls._BASE_PATH_YER_SILVER, f"MONTH_{cls.MONTH}")
        cls._BASE_PATH_DAY_SILVER  = join(cls._BASE_PATH_MONTH_SILVER, f"DAY_{cls.DAY}")
        cls.LIST_PATH_SILVER       = [cls._BASE_PATH_SILVER, cls._BASE_PATH_YER_SILVER, cls._BASE_PATH_MONTH_SILVER, cls._BASE_PATH_DAY_SILVER]
        cls.TRANSFORMED_DATA_PATH  = join(cls._BASE_PATH_DAY_SILVER, "locate_rmtc_go_{}.parquet")

class DefaultValues:

    # Default values for various parameters
    DEFAULT_LINHA:          str = "000"
    DEFAULT_DATA:           str = datetime.now().strftime("%d-%m-%Y")
    DEFAULT_ALL_DATA:       str = datetime.now().strftime("%d-%m-%Y-%H-%M-%S")

    # Minimum number of JSON files required to perform the transformation to CSV
    MIN_GRUP_JSON_TO_CSV:   int = 1
