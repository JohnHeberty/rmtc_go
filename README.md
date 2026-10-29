# rmtc-go

Pipelines ELT para os dados abertos da RMTC Goiânia ("Olho no Ônibus"), em arquitetura
medallion e orquestradas com [Prefect](https://docs.prefect.io).

| Pipeline | O que coleta | Frequência |
|---|---|---|
| [`locate_bus`](#pipeline-locate_bus) | posição de todos os ônibus em operação | a cada 1 min |
| [`linha_kml`](#pipeline-linha_kml) | traçado (KML) de cada linha e qual KML cada linha usou, e quando | KML 1× por dia (05:30); uso/tracado de hora em hora |

| Camada | Conteúdo | Regra |
|---|---|---|
| **bronze** | payload bruto da RMTC, em `.json.gz` | nunca sobrescrito; é a fonte da verdade |
| **silver** | parquet (zstd) tipado e validado, 1:1 com o bruto, sem regra de negócio | sempre reconstruível a partir do bronze |
| **quarantine** | registros que falharam na validação, com o motivo | nada é descartado em silêncio |
| **gold** | regras de negócio (a definir) | — |

## Requisitos

- [uv](https://docs.astral.sh/uv/) (instala o Python 3.15 do `.python-version`)
- Docker Desktop, para rodar agendado (recomendado)

## Setup

```bash
uv sync
cp .env.example .env
```

### Configuração (`.env`)

| Variável | Padrão | Uso |
|---|---|---|
| `DATA_DIR` | `database` | raiz do data lake (execução local) |
| `DATA_HOST_DIR` | `./database` | pasta do host montada em `/data` no Docker (prefira fora do OneDrive, ex.: `C:/data/rmtc_go`) |
| `TIMEZONE` | `America/Sao_Paulo` | fuso das datas, partições e crons |
| `LOCATE_BUS_CRON` | `* * * * *` | agendamento da `locate_bus` |
| `LINHA_KML_CRON` | `30 5 * * *` | agendamento da `linha_kml` (depois da falha de dados da RMTC das 04h, ver abaixo) |
| `LINHA_KML_USO_CRON` | `15 * * * *` | `linha_kml` só com uso + tracado (sem baixar KML), para o dia corrente |
| `HTTP_TIMEOUT` / `HTTP_RETRIES` | `30` / `3` | requisições à RMTC (retry com backoff para 429/5xx) |
| `BASE_URL` / `PREFIX_URL` | `https://www.rmtcgoiania.com.br/` / `index.php` | endpoint da API |
| `API_HOST` / `API_PORT` / `API_RELOAD` | `0.0.0.0` / `3001` / `false` | API FastAPI (só `/health` por enquanto) |

## Rodando com Docker (produção)

```bash
docker compose up -d --build                 # servidor Prefect + as duas pipelines agendadas
docker compose --profile api up -d           # + API em http://localhost:3001/health
docker compose ps
docker compose logs -f locate-bus            # ou linha-kml
docker compose down                          # para tudo (dados e histórico ficam)
```

| Serviço | Comando | Função |
|---|---|---|
| `prefect-server` | `prefect server start` | UI em http://localhost:4200 e histórico das execuções (volume `prefect-data`, SQLite) |
| `locate-bus` | `locate-bus serve` | agenda e executa a `locate_bus` |
| `linha-kml` | `linha-kml serve` | agenda e executa a `linha_kml` |
| `api` (profile `api`) | `rmtc-api` | API |

Comandos avulsos dentro do container:

```bash
docker compose exec locate-bus locate-bus load --day 2026-10-22 --full   # backfill de um dia
docker compose exec linha-kml linha-kml run                              # força a linha_kml agora
docker compose exec locate-bus prefect deployment run 'locate-bus/locate-bus-schedule'
```

Operação:

- **Mudou o código ou o `.env`?** `docker compose up -d --build`. A execução que estiver rodando
  no momento do restart morre (perde-se a coleta daquele minuto); ao subir, o `serve` marca
  essa execução como `Crashed` (`core/orchestration.py`) para ela não segurar a fila.
- Cada deployment roda **uma execução por vez** (`global_limit=1`): uma coleta atrasada não
  sobrepõe a próxima.
- Os containers usam `restart: unless-stopped` e voltam sozinhos quando o Docker Desktop
  inicia. Para coleta contínua: Docker Desktop iniciando com o Windows e o Windows sem
  suspensão na tomada (a suspensão congela os containers e a coleta para).

## Rodando local (CLI)

```bash
# locate_bus
uv run locate-bus extract                    # E: API -> bronze
uv run locate-bus load --day 2026-10-22      # L: coletas novas do dia -> silver (incremental)
uv run locate-bus load --day 2026-10-22 --full  # recarrega o dia inteiro (idempotente)
uv run locate-bus dedup --day 2026-10-22     # apaga coletas iguais à anterior (bronze + silver)
uv run locate-bus compact                    # junta o silver dos dias fechados (1 parquet por dia)
uv run locate-bus run                        # extract + load + dedup de hoje + compact
uv run locate-bus serve                      # flow agendado (precisa de um servidor Prefect)

# linha_kml
uv run linha-kml run                         # extract + dedup + load + uso + tracado
uv run linha-kml uso                         # só a tabela de uso (dias cujas posições mudaram)
uv run linha-kml uso --day 2026-10-28 --allow-shrink   # refaz um dia aceitando menos coletas
uv run linha-kml tracado                     # cobertura + uso resolvido por versão
uv run linha-kml consulta 004 ida "2026-10-28 04:50"   # qual traçado, e com que certeza
uv run linha-kml serve

uv run rmtc-api                              # API (GET /health)
```

## Pipeline `locate_bus`

Flow `locate-bus`, a cada minuto: **extract → load → dedup → compact**.

```
bronze/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.json.gz    # {metadata, payload}
silver/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.parquet    # dia corrente: 1 por coleta
silver/locate_bus/year=YYYY/month=MM/day=DD/locate_bus_YYYYMMDD.parquet # dia fechado: 1 por dia
quarantine/locate_bus/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.json   # registros rejeitados
bronze/locate_bus_coleta/year=YYYY/month=MM/day=DD/YYYYMMDDTHHMMSS.json.gz   # cada tentativa
silver/locate_bus_coleta/year=YYYY/month=MM/day=DD/locate_bus_coleta_YYYYMMDD.parquet
```

- **Cobertura** (`locate_bus_coleta`): toda tentativa de coleta é registrada com `Status`
  `gravada`, `igual` (igual à anterior, não gravada), `vazia` (sucesso sem ônibus) ou `erro`,
  mais `Onibus` e `OnibusSemKml`. É o que separa "a linha estava sem ônibus" de "não
  estávamos coletando". Dias anteriores ao registro (até 29/10) são inferidos das posições
  (`inferida`). Uma falha ao gravar o registro só vai para o log, nunca derruba a coleta.

- **Extract**: busca todos os ônibus (`linha=000`) e grava o payload intacto num envelope com
  `collected_at` e `payload_sha256`. Não grava se o payload for igual ao da última coleta do
  dia, e avisa no log quando a API responde sucesso com a lista vazia.
- **Load**: valida cada ônibus (pydantic) e grava um parquet por coleta, com schema fixo.
  Colunas: `Numero`, `Latitude`, `Longitude`, `Acessivel`, `Situacao`, `LinhaNumero`,
  `LinhaKMLIda`, `LinhaKMLVolta`, `DestinoCurto`, `ColetadoEm`, `ColetaSha256`. Um ônibus
  inválido vai para a quarentena sem descartar a coleta.
- **Dedup**: apaga do bronze, do silver e da quarentena as coletas iguais à anterior que
  escaparem do extract. Só compara com a anterior: um payload que volta depois de outro
  diferente é mantido.
- **Compact**: depois que o dia fecha, reconstrói o dia **a partir do bronze** num parquet
  único (não dá para acrescentar linhas a um parquet, por isso o dia corrente fica picado).
  Regras para não perder dado:
  - o bronze nunca é compactado nem sobrescrito;
  - o arquivo do dia é gravado de forma atômica e conferido (nº de linhas) antes da troca;
  - se o resultado tiver menos linhas que o silver atual (ex.: arquivo sumiu do bronze), a
    compactação é recusada e o silver fica como está (erro no log do Prefect);
  - os parquets por coleta só são apagados depois do arquivo do dia gravado; se cair no meio,
    a próxima execução termina sem duplicar.
- `load --day` de um dia fechado reconstrói direto o arquivo do dia. Backfill sem coletar:
  `load --day YYYY-MM-DD --full` (ou o flow com `extract=False, day=..., full=True`).

## Pipeline `linha_kml`

Os KML das rotas são uma dimensão separada das posições (embutir o traçado em cada linha do
parquet de posições multiplicaria o volume por ~1000). Flow `linha-kml`:
**extract → dedup → load → uso → tracado** às 05:30 (`LINHA_KML_CRON`), e só
**uso → tracado** de hora em hora (`LINHA_KML_USO_CRON`), para o dia corrente não ficar parado.

```
bronze/linha_kml/linha=<n>/sentido=<ida|volta>/arquivo=<nome>/<ts>.json.gz   # KML bruto
bronze/linha_kml/_fetch/<ts>.json.gz                                         # registro de cada execução
silver/linha_kml/kml_observacao.parquet
silver/linha_kml/kml_versao.parquet
silver/linha_kml/uso/year=YYYY/month=MM/day=DD/linha_kml_uso_YYYYMMDD.parquet
silver/linha_kml/tracado/year=YYYY/month=MM/day=DD/linha_kml_tracado_YYYYMMDD.parquet
quarantine/linha_kml/rejected.json
```

**O KML é da linha, não do ônibus.** Toda posição traz o KML de ida e o de volta da linha (não
diz para que lado o ônibus vai), e a RMTC troca o arquivo para todos os ônibus da linha juntos.
Então o `uso` diz qual era o traçado **oficial** da linha em cada minuto, não por onde um ônibus
passou. Se isso mudar, o `uso` avisa no log (ver "premissas" abaixo).

**Bronze** (nada é sobrescrito):
- As URLs vêm das posições dos últimos 7 dias (`DISCOVERY_DAYS`), sem o `?rev=` (é só a hora
  do servidor; o Cloudflare não faz cache desses arquivos). Não há endpoint que liste as linhas.
- O KML bruto é gravado só quando o conteúdo muda, com o `Last-Modified` e o `ETag` do servidor.
  Algumas linhas têm um KML por eixo no mesmo sentido (ex.: `Eixo85-004ida` e
  `EixoT9-004ida`), por isso o arquivo faz parte da chave.
- `_fetch/` registra toda tentativa de download (ok sem mudança, ok com versão nova, erro),
  também com o `Last-Modified`: é o que prova que um traçado foi conferido num momento.
- O `dedup` apaga versões iguais à anterior que escaparem.

**Silver** (reconstruído do bronze/posições, atômico, recusa se encolher):
- `kml_observacao`: uma linha por verificação (~620 por dia: cresce devagar, mas cresce).
- `kml_versao`: uma linha por versão de cada arquivo, comparando a geometria (reformatar o KML
  não vira versão nova; A→B→A são 3 versões). Geometria em WKT (lon lat). Quando a versão
  passou a valer:
  - `Precisao = exata`: o `Last-Modified` cai na janela da troca `(TrocaApos, PrimeiraObs]`,
    então `ValidoDesde` é o horário exato. Na 1ª versão, vale desde o `Last-Modified` (ex.:
    `EixoT9-004ida` desde 05/09/2022), mesmo antes do nosso 1º download.
  - `janela`: sem `Last-Modified` coerente; `ValidoDesde = PrimeiraObs` e a troca foi em algum
    momento da janela.
  - `desde_1a_obs`: 1ª versão sem `Last-Modified`.
- `uso/`: qual KML cada linha/sentido usou e quando (precisão da coleta), a partir das
  posições. Um período fecha quando a linha troca de arquivo ou some por mais de 15 min
  (`USO_GAP_MINUTES`); minutos com KML null viram um período sem `Url`. Cada dia é refeito
  quando as posições dele mudam (impressão da fonte nos metadados do parquet). Num dia
  fechado, recusa encolher e a execução falha (`--allow-shrink` para aceitar).
  Premissas checadas a cada dia, com aviso no log: um KML por linha/sentido em cada coleta,
  cada KML usado por uma só linha/sentido, nomes de arquivo sem colisão.
- `tracado/`: o `uso` já resolvido para a versão, cortado nas fronteiras de versão, com
  `Versao` (junta com `kml_versao` por `Url` + `Versao`), `ArquivoAnterior` (calculado entre
  dias: o corte da meia-noite não parece troca) e `Status`:

  | Status | Significado |
  |---|---|
  | `certo` | a versão estava confirmada nesse horário |
  | `janela` | a troca para esta versão foi em algum momento aqui; pode ser esta ou a anterior |
  | `anterior` | antes da 1ª versão conhecida do arquivo (a `Versao` 1 é só palpite) |
  | `nao_confirmado` | depois da última confirmação; vira `certo` no próximo download se não mudou |
  | `sem_download` | arquivo nunca baixado (ainda não, ou nome barrado pelo extract) |
  | `sem_kml` | a RMTC não informou o KML (ex.: falha das 04h) |

**Qual traçado uma linha usava no instante t**: `linha-kml consulta <linha> <sentido> "<t>"`
(ou `tracado.tracado_em`). Fora dos períodos do `tracado`, responde `sem_uso` (havia coleta e a
linha não apareceu) ou `sem_coleta` (não estávamos coletando), pela cobertura da `locate_bus`.

**Refazer do zero**: `kml_observacao` e `kml_versao` saem do bronze da `linha_kml`; `uso`,
`tracado` e a cobertura saem das posições e do registro de tentativas. Isso só vale enquanto
esses bronzes forem guardados: se um dia houver retenção, o que for apagado deixa de ter fonte.

## Consultando com DuckDB

```sql
-- posições
select * from read_parquet('database/silver/locate_bus/**/*.parquet', hive_partitioning = true);

-- coletas repetidas (mesmo payload em coletas diferentes)
select ColetaSha256, count(distinct ColetadoEm) as coletas
from read_parquet('database/silver/locate_bus/**/*.parquet', hive_partitioning = true)
group by 1 having coletas > 1;

-- qual KML a linha 004 (ida) usou em 28/10, com a versão e a certeza
select Arquivo, Versao, Status, Desde, Ate
from read_parquet('database/silver/linha_kml/tracado/**/*.parquet', hive_partitioning = true)
where LinhaNumero = '004' and Sentido = 'ida' and day = 28
order by Desde;

-- quando cada linha trocou de KML (sem os cortes da meia-noite)
select LinhaNumero, Sentido, ArquivoAnterior, Arquivo, Desde
from read_parquet('database/silver/linha_kml/tracado/**/*.parquet', hive_partitioning = true)
where Arquivo <> ArquivoAnterior
order by Desde;

-- EixoT9 das 04h: traçado da madrugada ou falha da RMTC? Põe lado a lado, por minuto, os
-- ônibus sem KML e as trocas de eixo (rever com ~7 dias de cobertura)
select date_trunc('minute', c.TentativaEm) as minuto, c.OnibusSemKml,
       list(distinct t.Arquivo) filter (where t.Arquivo <> t.ArquivoAnterior) as eixos_trocados
from read_parquet('database/silver/locate_bus_coleta/**/*.parquet') c
left join read_parquet('database/silver/linha_kml/tracado/**/*.parquet') t
  on c.TentativaEm between t.Desde and t.Ate
where extract(hour from c.TentativaEm) between 3 and 5
group by all order by minuto;
```

## Pontos conhecidos dos dados

- **`Numero` não identifica o ônibus sozinho**: já apareceu o mesmo número em duas linhas na
  mesma coleta. Use `Numero` + `LinhaNumero`.
- **Nulos da RMTC por volta das 04h**: em 28/10, das 04:01 às 05:00, a API mandou ônibus em
  operação sem `DestinoCurto` ou sem KML. Esses três campos aceitam null (a chave continua
  obrigatória); os demais são validados normalmente.
- **API vazia**: às vezes a RMTC responde `status: sucesso` com a lista de ônibus vazia por
  alguns minutos. Fica registrado no bronze e avisado no log.
- **Ônibus parados**: ~6% das linhas de posição repetem a anterior do mesmo ônibus (parado
  ou GPS travado). Isso é dado válido do silver; tirar a repetição é regra do gold.
- **Precisão da coleta**: cada ônibus atualiza a posição a cada ~20 s (mediana); a coleta de
  1 min captura parte dessas atualizações.

## Estrutura

```
src/rmtc_go/
├── core/                    # compartilhado entre pipelines
│   ├── settings.py          # configuração via .env (pydantic-settings)
│   ├── http.py              # Session com retry/backoff
│   ├── storage.py           # partições Hive, escrita atômica e conferida, JSON gzip, bronze sem sobrescrita
│   ├── orchestration.py     # Prefect: libera execuções órfãs ao subir o serve
│   └── logging.py
├── pipelines/               # uma pasta por pipeline, mesmo formato
│   ├── locate_bus/
│   │   ├── config.py        # dataset, headers, params
│   │   ├── schemas.py       # contratos pydantic (bronze, API) e schema do silver
│   │   ├── extract.py       # E: API -> bronze (sem gravar repetido)
│   │   ├── load.py          # L: bronze -> silver + quarentena
│   │   ├── dedup.py         # apaga coletas iguais à anterior
│   │   ├── compact.py       # dia fechado -> 1 parquet, reconstruído do bronze
│   │   ├── coleta.py        # cobertura: registro de cada tentativa de coleta
│   │   ├── pipeline.py      # funções usadas pela CLI e pelo flow
│   │   ├── flow.py          # flow/tasks do Prefect + serve
│   │   └── __main__.py      # CLI
│   └── linha_kml/
│       ├── config.py, schemas.py, extract.py, dedup.py, load.py
│       ├── uso.py           # silver de uso (KML por linha) a partir das posições
│       ├── tracado.py       # uso resolvido por versão + consulta tracado_em
│       └── pipeline.py, flow.py, __main__.py
└── api/                     # FastAPI
tests/                       # pytest, espelha src/
```

## Nova pipeline

Crie `src/rmtc_go/pipelines/<nome>/` no mesmo formato (config, schemas, extract, load,
pipeline, flow, `__main__`), reutilizando `core/`, registre o script em `pyproject.toml`
(`[project.scripts]`) e um serviço no `compose.yaml` com `<nome> serve`.

## Desenvolvimento

```bash
uv run pytest
uv run ruff check . && uv run ruff format .
uv run pre-commit install
```

Dependência fixada: `sqlalchemy<2.1` (`[tool.uv] constraint-dependencies` no
`pyproject.toml`), porque o scheduler do servidor do Prefect 3.9.0 quebra com SQLAlchemy 2.1
e nenhuma execução agendada é criada. Remover quando o Prefect suportar a 2.1.
