from pydantic import BaseModel, Field
from src.prefect.config import Config
from typing import List

# not finish
class InfoDestino(BaseModel):
    DestinoCurto:   str = Field(..., min_length=3, max_length=50)

# not finish
class InfoLinha(BaseModel):
    LinhaNumero:    str = Field(..., min_length=1, max_length=4)
    LinhaKMLIda:    str
    LinhaKMLVolta:  str

    def LinhaKMLIda_valid(self):
        if Config.BASE_URL not in self.LinhaKMLIda:
            raise ValueError("Dominío do Link da Rota Diferente do API, Algo mudou.")

    def LinhaKMLVolta_valid(self):
        if Config.BASE_URL not in self.LinhaKMLVolta:
            raise ValueError("Dominío do Link da Rota Diferente do API, Algo mudou.")

    def validate(self):
        self.LinhaKMLIda_valid()
        self.LinhaKMLVolta_valid()

class ListOnibus(BaseModel):
    Numero:     int = Field(..., ge=1, le=99999)
    Latitude:   float = Field(..., ge=-23, le=-12)
    Longitude:  float = Field(..., ge=-54, le=-45)
    Acessivel:  int = Field(..., ge=0, le=1)
    Situacao:   str = Field(..., min_length=4, max_length=12)
    Linha:      InfoLinha
    Destino:    InfoDestino

    def validate(self):
        self.Linha.validate()

class LocateBusValidator(BaseModel):
    status:     str = Field(..., min_length=3, max_length=15)
    onibus:     List[ListOnibus]

    def validate(self):
        for onibus in self.onibus:
            onibus.validate()
