from pydantic import BaseModel, Field
from datetime import datetime

class MinDataValidator(BaseModel):
    """
    A Pydantic model for validating data with a date field.

    Attributes:
        data (str): A string representing a date in the format "DD-MM-YYYY".
    """
    data: str = Field(..., pattern=r"^\d{2}-\d{2}-\d{4}$", description="Date in DD-MM-YYYY format")

    def validate(self):
        try:
            datetime.strptime(self.data, "%d-%m-%Y")
        except ValueError:
            raise ValueError(f"Data informada tem formato inválido para 'data': {self.data}. Formato esperado: DD-MM-YYYY.")

    def datetime(self) -> datetime:
        """
        Convert the validated date string to a datetime object.

        Returns:
            datetime: A datetime object representing the validated date.
        """
        return datetime.strptime(self.data, "%d-%m-%Y")

class MaxDataValidator(BaseModel):
    """
    A Pydantic model for validating data with a date field.

    Attributes:
        data (str): A string representing a date in the format "DD-MM-YYYY".
    """
    data: str = Field(..., pattern=r"^\d{2}-\d{2}-\d{4}-\d{2}-\d{2}-\d{2}$", description="Date in DD-MM-YYYY-HH-MM-SS format")

    def validate(self):
        try:
            datetime.strptime(self.data, "%d-%m-%Y-%H-%M-%S")
        except ValueError:
            raise ValueError(f"Data informada tem formato inválido para 'data': {self.data}. Formato esperado: DD-MM-YYYY-HH-MM-SS.")

    def datetime(self) -> datetime:
        """
        Convert the validated date string to a datetime object.

        Returns:
            datetime: A datetime object representing the validated date.
        """
        return datetime.strptime(self.data, "%d-%m-%Y-%H-%M-%S")

class DataValuesValidator(BaseModel):
    Numero:         int
    Latitude:       float
    Longitude:      float
    Acessivel:      int
    Situacao:       str
    LinhaNumero:    str
    LinhaKMLIda:    str
    LinhaKMLVolta:  str
    DestinoCurto:   str
    Data:           str
    Hora:           str
    Minuto:         str
