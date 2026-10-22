# rmtc_go

Pipeline de dados que coleta a localização em tempo real dos ônibus da **RMTC Goiânia** (serviço "Olho no Ônibus") e organiza os dados em camadas no padrão medallion (bronze → silver), além de uma API FastAPI para expor o serviço.

## Visão geral

```
API RMTC (JSON)
      │  extract  → valida com Pydantic e adiciona timestamp da coleta
      ▼
database/bronze/AAAA/MONTH_MM/DAY_DD/locate_rmtc_go_DD-MM-AAAA-HH-MM-SS.json
      │  transform → normaliza, deduplica por linha/posição/hora/minuto e valida
      ▼
database/silver/AAAA/MONTH_MM/DAY_DD/locate_rmtc_go_DD-MM-AAAA.parquet
```

- **Extract**: consulta o endpoint `index.php?option=com_rmtclinhas&view=cconaweb&format=json` e salva um JSON por execução na camada bronze.
- **Transform**: junta todos os JSONs do dia, achata os campos aninhados (`Linha`, `Destino`), remove duplicatas e grava um Parquet (snappy) na camada silver.
- **Load**: ainda não implementado.

## Stack

- Python 3.15, gerenciado com [uv](https://docs.astral.sh/uv/)
- requests, pandas, pyarrow, pydantic, python-dotenv
- FastAPI + Uvicorn
- pre-commit

## Estrutura

```
.
├── run.py                       # Sobe a API (uvicorn, porta 3001)
├── src/
│   ├── pipeline/locate_bus/
│   │   ├── main.py              # CLI do pipeline (-e / -t / -d)
│   │   ├── constants.py         # Caminhos bronze/silver e valores padrão
│   │   ├── extract/             # Coleta na API da RMTC
│   │   ├── transform/           # JSON → DataFrame → Parquet
│   │   └── load/                # (a implementar)
│   ├── validators/locate_bus/   # Modelos Pydantic de extração e transformação
│   ├── prefect/config.py        # Lê variáveis do .env
│   ├── routes/health.py         # Rota GET /health
│   ├── server/server.py         # App FastAPI
│   └── utils/                   # Leitura/escrita de arquivos e criação de pastas
└── database/                    # Dados gerados (ignorado pelo git)
```

## Instalação

```powershell
git clone https://github.com/JohnHeberty/rmtc_go.git
cd rmtc_go
uv sync
Copy-Item .env.example .env
uv run pre-commit install
```

### Variáveis de ambiente (`.env`)

| Variável     | Exemplo                            | Descrição                |
|--------------|------------------------------------|--------------------------|
| `BASE_URL`   | `https://www.rmtcgoiania.com.br/`  | Domínio da RMTC          |
| `PREFIX_URL` | `index.php`                        | Caminho do endpoint      |

## Uso

O código importa módulos tanto como `src.xxx` quanto como `pipeline.xxx`/`validators.xxx`, então a raiz e a pasta `src` precisam estar no `PYTHONPATH`:

```powershell
$env:PYTHONPATH = ".;src"
```

### Pipeline

```powershell
# Coletar a posição atual dos ônibus (camada bronze)
uv run python src/pipeline/locate_bus/main.py --extract

# Transformar os dados do dia atual (camada silver)
uv run python src/pipeline/locate_bus/main.py --transform
```

Dica: para montar uma série histórica, agende o `--extract` para rodar periodicamente (ex.: a cada 10 minutos).

### API

```powershell
uv run python run.py
```

- Health check: `GET http://localhost:3001/health`
- Documentação interativa: `http://localhost:3001/docs`

## Schema da camada silver

| Coluna          | Tipo  | Descrição                                   |
|-----------------|-------|---------------------------------------------|
| `Numero`        | int   | Número do veículo                           |
| `Latitude`      | float | Latitude                                    |
| `Longitude`     | float | Longitude                                   |
| `Acessivel`     | int   | 1 = acessível, 0 = não                      |
| `Situacao`      | str   | Situação do veículo                         |
| `LinhaNumero`   | str   | Número da linha                             |
| `LinhaKMLIda`   | str   | URL do KML da rota de ida                   |
| `LinhaKMLVolta` | str   | URL do KML da rota de volta                 |
| `DestinoCurto`  | str   | Destino                                     |
| `Data`          | str   | Momento da coleta (`DD-MM-AAAA-HH-MM-SS`)   |
| `Hora`          | str   | Hora da coleta                              |
| `Minuto`        | str   | Faixa de 10 minutos da coleta (`00`, `10`…) |

## Roadmap

- [ ] Etapa de load
- [ ] Orquestração com Prefect
- [ ] Rotas da API para consultar os dados
- [ ] Redis para consulta em realtime
- [ ] Pipeline de criação das rotas

## Licença

[MIT](LICENSE) © 2026 John Heberty de Freitas
