from validators.locate_bus.transform_validator import MinDataValidator, DataValuesValidator
from src.pipeline.locate_bus.constants import DefaultValues, Paths
from src.utils.init_folders import init_folders
from src.utils.os_files import folder, read
from datetime import datetime
import pandas as pd

def transform_json_to_df(json_dict: dict) -> pd.DataFrame:
    """
    Transform the validated data into a pandas DataFrame.

    Args:
        json_dict (dict): The validated data containing 'onibus' list.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed bus data.
    """
    if json_dict.get("status") != "sucesso":
        raise ValueError(f"API returned status: {json_dict.get('status')}")

    df = pd.json_normalize(json_dict["onibus"], sep="=")
    return df

def run_transform(data: str = DefaultValues.DEFAULT_DATA) -> pd.DataFrame:
    """
    Run the transformation process from JSON to DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the transformed data.
    """

    if data not in [None, ""]: data_process = MinDataValidator(data=data)   # Create a MinDataValidator instance with provided data
    else: data_process = MinDataValidator(data=DefaultValues.DEFAULT_DATA)  # Create a MinDataValidator instance with default data
    data_process.validate()                                                 # Validate the data

    Paths.change_data(
        YER     =   data_process.datetime().strftime("%Y"),
        MONTH   =   data_process.datetime().strftime("%m").zfill(2),
        DAY     =   data_process.datetime().strftime("%d").zfill(2)
    )                                                                       # Update Paths with the new date

    all_files   = folder.get_files(Paths._BASE_PATH_DAY_BRONZE)             # Get all files from the specified folder
    df = pd.DataFrame()

    for file in all_files:
        df = pd.concat([df, transform_json_to_df(read.read_json(file))], axis=0, ignore_index=True)

    df["Hora"]      = df["Data"].apply(lambda x: x.split("-")[-3])
    df["Minuto"]    = df["Data"].apply(lambda x: x.split("-")[-2][0].ljust(2, '0'))
    df.columns = [colum.split("=")[-1] for colum in df.columns]
    df.drop_duplicates(["LinhaNumero", "Latitude", "Longitude", "Hora", "Minuto"], inplace=True, ignore_index=True)

    df = pd.DataFrame(
        [DataValuesValidator(**row).model_dump() for _, row in df.iterrows()]
    ).reset_index(drop=True)

    init_folders(Paths.LIST_PATH_SILVER)                                    # Initialize folders

    df.to_parquet(
        Paths.TRANSFORMED_DATA_PATH.format(DefaultValues.DEFAULT_DATA),
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    return df
