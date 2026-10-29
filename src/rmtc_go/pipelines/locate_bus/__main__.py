"""CLI da pipeline: uv run locate-bus <comando> (ou python -m rmtc_go.pipelines.locate_bus)."""

from argparse import ArgumentParser
from datetime import date

from rmtc_go.core.logging import setup_logging

from .config import DEFAULT_LINHA


def main(argv: list[str] | None = None) -> None:
    parser = ArgumentParser(prog="locate-bus", description="Pipeline ELT de localização dos ônibus")
    sub = parser.add_subparsers(dest="command", required=True)

    p_extract = sub.add_parser("extract", help="E: coleta da API e salva o bruto no bronze")
    p_extract.add_argument("--linha", default=DEFAULT_LINHA)

    p_load = sub.add_parser("load", help="L: carrega o bronze de um dia no silver (parquet)")
    p_load.add_argument("--day", type=date.fromisoformat, help="YYYY-MM-DD (padrão: hoje)")
    p_load.add_argument(
        "--full", action="store_true", help="recarrega todas as coletas do dia, não só as novas"
    )

    p_dedup = sub.add_parser("dedup", help="apaga as coletas do dia iguais à anterior")
    p_dedup.add_argument("--day", type=date.fromisoformat, help="YYYY-MM-DD (padrão: hoje)")

    sub.add_parser("compact", help="junta num parquet por dia o silver dos dias fechados")

    p_run = sub.add_parser("run", help="extract + load + dedup do dia atual + compact")
    p_run.add_argument("--linha", default=DEFAULT_LINHA)

    p_serve = sub.add_parser("serve", help="Sobe o flow no Prefect com agendamento")
    p_serve.add_argument("--cron", help='ex.: "*/5 * * * *" (padrão: LOCATE_BUS_CRON)')

    args = parser.parse_args(argv)
    setup_logging()

    if args.command == "serve":
        from .flow import serve  # importa o Prefect só quando necessário

        serve(args.cron)
        return

    from . import pipeline

    match args.command:
        case "extract":
            pipeline.extract(args.linha)
        case "load":
            pipeline.load(args.day, args.full)
        case "dedup":
            pipeline.dedup(args.day)
        case "compact":
            pipeline.compact()
        case "run":
            pipeline.extract(args.linha)
            pipeline.load()
            pipeline.dedup()
            pipeline.compact()


if __name__ == "__main__":
    main()
