# rmtc-go

Pipelines ELT para os dados abertos da RMTC Goiânia ("Olho no Ônibus"), em arquitetura
medallion e orquestradas com [Prefect](https://docs.prefect.io).

| Etapa | O que faz | Camada |
|---|---|---|
| **E**xtract | busca na API e salva o payload bruto | bronze (JSON gzip, imutável) |
| **L**oad | carrega o bruto no "banco": tipado, validado, 1:1, sem regra de negócio | silver (parquet zstd) |
| **T**ransform | regras de negócio sobre o silver (a definir) | gold |

## Setup

```bash
uv sync
cp .env.example .env
```

## Uso

```bash
uv run locate-bus extract                    # E: API -> bronze
uv run locate-bus load --day 2026-10-22      # L: coletas novas do dia -> silver (incremental)
uv run locate-bus load --day 2026-10-22 --full  # recarrega o dia inteiro (idempotente)
uv run locate-bus dedup --day 2026-10-22     # apaga coletas iguais à anterior (bronze + silver)
uv run locate-bus compact                    # junta o silver dos dias fechados (1 parquet por dia)
uv run locate-bus run                        # extract + load + dedup de hoje + compact
uv run locate-bus serve                      # flow agendado no Prefect (cron LOCATE_BUS_CRON)

uv run rmtc-api                              # API (GET /health)
```

Backfill de um dia sem coletar: `load --day YYYY-MM-DD --full`
(ou o flow com `extract=False, day=..., full=True`).

## Docker

```bash
docker compose up -d --build                 # servidor Prefect + pipeline agendada
docker compose --profile api up -d           # + API em http://localhost:3001/health
docker compose logs -f locate-bus
docker compose exec locate-bus locate-bus load --day 2026-10-22 --full   # backfill
docker compose down                          # para tudo (dados e histórico ficam)
```

- UI do Prefect: http://localhost:4200
- Agendamento: `LOCATE_BUS_CRON` no `.env` (fuso de `TIMEZONE`), depois `docker compose up -d`.
- Data lake: `./database` montado em `/data`; troque com `DATA_HOST_DIR` no `.env`
  (recomendado fora do OneDrive, ex.: `DATA_HOST_DIR=C:/data/rmtc_go`).
- Histórico do Prefect: volume `prefect-data` (SQLite).

## Estrutura

```
src/rmtc_go/
├── core/                  # compartilhado entre pipelines
│   ├── settings.py        # configuração via .env (pydantic-settings)
│   ├── http.py            # Session com retry/backoff
│   ├── storage.py         # partições Hive, escrita atômica, bronze imutável
│   └── logging.py
├── pipelines/
│   └── locate_bus/        # uma pasta por pipeline
│       ├── config.py      # dataset, headers, params
│       ├── schemas.py     # contratos pydantic (bronze, API, silver)
│       ├── extract.py     # E: busca na API e grava o bruto no bronze
│       ├── load.py        # L: bronze -> silver + quarentena
│       ├── transform.py   # T: a definir (vazio)
│       ├── pipeline.py    # funções puras usadas pela CLI e pelo flow
│       ├── flow.py        # flow/tasks do Prefect
│       └── __main__.py    # CLI
└── api/                   # FastAPI
```

## Data lake (`DATA_DIR`, padrão `database/`)

```
bronze/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.json.gz   # {metadata, payload}, nunca sobrescrito
silver/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.parquet    # dia corrente: 1 por coleta
silver/locate_bus/year=YYYY/month=MM/day=DD/locate_bus_YYYYMMDD.parquet # dia fechado: 1 por dia
quarantine/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.json   # registros que falharam na validação
```

Coletas repetidas: o extract não grava uma coleta cujo payload (`metadata.payload_sha256`)
é igual ao da última do dia, e o passo `dedup` no fim do flow apaga do bronze, do silver e
da quarentena as que escaparem. Só compara com a coleta anterior: um payload que volta depois
de outro diferente é mantido.

Compactação do silver: o dia corrente é gravado incrementalmente (um parquet por coleta) e,
depois que o dia fecha, o passo `compact` do flow reconstrói o dia **a partir do bronze** num
parquet único. Regras para não perder dado:

- o bronze nunca é compactado nem sobrescrito; o silver sempre pode ser refeito a partir dele;
- o arquivo do dia é gravado de forma atômica e conferido (nº de linhas) antes da troca;
- se o resultado tiver menos linhas que o silver atual (ex.: arquivo sumiu do bronze), a
  compactação é recusada e o silver fica como está (erro no log do Prefect);
- os parquets por coleta só são apagados depois do arquivo do dia gravado; se cair no meio,
  a próxima execução termina sem duplicar.

`load --day` de um dia fechado reconstrói direto o arquivo do dia.

Leitura com DuckDB:

```sql
select * from read_parquet('database/silver/locate_bus/**/*.parquet', hive_partitioning = true);
```

## Traçado das linhas (`linha_kml`)

Os KML das rotas são uma dimensão separada das posições (embutir o traçado em cada linha do
parquet de posições multiplicaria o volume por ~1000). Roda 1×/dia (`LINHA_KML_CRON`, padrão
05:30) no container `linha-kml`: extract → dedup → load → uso.

```bash
uv run linha-kml run      # tudo
uv run linha-kml uso      # só linha_kml_uso (a partir das posições, sem baixar nada)
```

**Bronze** (nada é sobrescrito):
- `bronze/linha_kml/linha=<n>/sentido=<ida|volta>/arquivo=<nome>/<ts>.json.gz`: o KML bruto,
  gravado só quando o conteúdo muda. Algumas linhas têm um KML por eixo no mesmo sentido.
- `bronze/linha_kml/_fetch/<ts>.json.gz`: registro de cada execução, com toda tentativa de
  download (ok sem mudança, ok com versão nova, erro). É o que prova que um traçado foi
  conferido num momento.
- As URLs vêm das posições dos últimos 7 dias (`DISCOVERY_DAYS`), sem o `?rev=` (muda a cada
  hora, o arquivo não). O `dedup` apaga versões iguais à anterior que escaparem.

**Silver** (reconstruído do bronze, atômico, recusa se encolher):
- `silver/linha_kml/kml_observacao.parquet`: uma linha por verificação.
- `silver/linha_kml/kml_versao.parquet`: uma linha por versão de cada arquivo, comparando a
  geometria (reformatar o KML não vira versão nova; A→B→A são 3 versões). A troca aconteceu
  em `(TrocaApos, PrimeiraObs]`; `UltimaObs` é a última confirmação.
- `silver/linha_kml/uso/year=/month=/day=/linha_kml_uso_YYYYMMDD.parquet`: qual KML cada
  linha/sentido usou e quando (precisão da coleta), a partir das posições. Um período fecha
  quando a linha troca de arquivo ou some por mais de 15 min (`USO_GAP_MINUTES`).

**Qual traçado uma linha usava no instante t**: em `linha_kml_uso` pega o `Arquivo` do período
que contém t; em `kml_versao` pega a versão desse arquivo com maior `PrimeiraObs` ≤ t. Se não
houver (t antes do 1º download) ou t > `UltimaObs`, a versão é **incerta**. O "traçado atual"
é uma consulta (última versão + uso recente), não uma coluna.

## Nova pipeline

Crie `src/rmtc_go/pipelines/<nome>/` seguindo o mesmo formato de `locate_bus`, reutilizando `core/`.

## Desenvolvimento

```bash
uv run pytest
uv run ruff check . && uv run ruff format .
uv run pre-commit install
```
