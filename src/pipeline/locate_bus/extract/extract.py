from validators.locate_bus.transform_validator import MaxDataValidator
from pipeline.locate_bus.constants import Paths, DefaultValues
from src.utils.init_folders import init_folders
from src.utils.os_files import write
from requests.status_codes import codes
from src.prefect.config import Config
# from datetime import datetime
from requests import request
from os.path import join
from typing import Any

from validators.locate_bus.extract_validator import LocateBusValidator # typping

def validate_response(data_response: dict[str, Any]) -> dict[str, Any] | None:
    """
    Validate the response from the RMTC API.

    Args:
        req: The response object from the requests library.

    Returns:
        A dictionary containing the JSON response or the text content if JSON parsing fails, or None if the response is invalid.
    """
    try:
        validated = LocateBusValidator(**data_response)
        validated.validate()
        validated = validated.dict()
        validated["onibus"] = [
            {**bus, "Data": DefaultValues.DEFAULT_ALL_DATA}
            for bus in validated["onibus"]
        ]
        return validated
    except Exception as e:
        raise ValueError(f"Response validation failed: {e}")

def collect_locate_rmtc_go(linha: str = "000") -> dict[str, Any] | None:
    link    = join(str(Config.BASE_URL), str(Config.PREFIX_URL))
    headers = {
        "accept": "application/json",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "priority": "u=1, i",
        "referer": "https://www.rmtcgoiania.com.br/index.php/olho-no-onibus?enviar=Acessar",
        "sec-ch-ua": '"Google Chrome";v="155", "Chromium";v="155", "Not.A/Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36",
        "x-request": "JSON",
        "x-requested-with": "XMLHttpRequest"
    }
    params = {
        "option": "com_rmtclinhas",
        "view": "cconaweb",
        "format": "json",
        "linha": linha
    }
    req = request(
        method="GET",
        url=link,
        headers=headers,
        params=params,
        timeout=30
    )
    if req.status_code == codes.ok:
        try:
            data_response = req.json()
        except ValueError:
            data_response = req.text
            raise Exception(
                f"Erro ao consultar RMTC. "
                f"Status: {req.status_code} | Resposta: {req.text}"
            )
    return validate_response(data_response)

def run_extract() -> None:
    """
    Run the data collection process from the RMTC API.
    """

    print("Starting data collection from RMTC API...")
    data_process = MaxDataValidator(data=DefaultValues.DEFAULT_ALL_DATA)    # Create a MaxDataValidator instance with default data
    data_process.validate()                                                 # Validate the data
    init_folders(Paths.LIST_PATH_BRONZE)                                    # Initialize folders
    write.write_json(
        collect_locate_rmtc_go(),
        Paths.EXTRACTED_DATA_PATH.format(data_process.data)
    )  # Collect data from RMTC API and Save extracted data to JSON file
