DATASET = "locate_bus"

# "000" retorna todas as linhas
DEFAULT_LINHA = "000"

# Status que a API devolve quando a consulta deu certo
STATUS_SUCESSO = "sucesso"

HEADERS = {
    "accept": "application/json",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.rmtcgoiania.com.br/index.php/olho-no-onibus?enviar=Acessar",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
    ),
    "x-request": "JSON",
    "x-requested-with": "XMLHttpRequest",
}


def build_params(linha: str = DEFAULT_LINHA) -> dict[str, str]:
    return {
        "option": "com_rmtclinhas",
        "view": "cconaweb",
        "format": "json",
        "linha": linha,
    }
