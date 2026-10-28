"""CLI da pipeline: uv run linha-kml <comando> (ou python -m rmtc_go.pipelines.linha_kml)."""

from argparse import ArgumentParser

from rmtc_go.core.logging import setup_logging


def main(argv: list[str] | None = None) -> None:
    parser = ArgumentParser(prog="linha-kml", description="Pipeline do traçado (KML) das linhas")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("extract", help="E: baixa os KML e grava no bronze só o que mudou")
    sub.add_parser("dedup", help="apaga do bronze as versões iguais à anterior")
    sub.add_parser("load", help="L: reconstrói kml_observacao e kml_versao a partir do bronze")
    sub.add_parser("uso", help="reconstrói linha_kml_uso (KML usado por linha) das posições")
    sub.add_parser("run", help="extract + dedup + load + uso")
    p_serve = sub.add_parser("serve", help="Sobe o flow no Prefect com agendamento")
    p_serve.add_argument("--cron", help='ex.: "30 5 * * *" (padrão: LINHA_KML_CRON)')

    args = parser.parse_args(argv)
    setup_logging()

    if args.command == "serve":
        from .flow import serve  # importa o Prefect só quando necessário

        serve(args.cron)
        return

    from . import pipeline

    match args.command:
        case "extract":
            pipeline.extract()
        case "dedup":
            pipeline.dedup()
        case "load":
            pipeline.load()
        case "uso":
            pipeline.uso()
        case "run":
            pipeline.extract()
            pipeline.dedup()
            pipeline.load()
            pipeline.uso()


if __name__ == "__main__":
    main()
