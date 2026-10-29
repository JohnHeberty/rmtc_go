"""CLI da pipeline: uv run linha-kml <comando> (ou python -m rmtc_go.pipelines.linha_kml)."""

from argparse import ArgumentParser
from datetime import date, datetime

from rmtc_go.core.logging import setup_logging


def main(argv: list[str] | None = None) -> None:
    parser = ArgumentParser(prog="linha-kml", description="Pipeline do traçado (KML) das linhas")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("extract", help="E: baixa os KML e grava no bronze só o que mudou")
    sub.add_parser("dedup", help="apaga do bronze as versões iguais à anterior")
    sub.add_parser("load", help="L: reconstrói kml_observacao e kml_versao a partir do bronze")
    p_uso = sub.add_parser(
        "uso", help="reconstrói linha_kml_uso (KML usado por linha) das posições"
    )
    p_uso.add_argument("--day", type=date.fromisoformat, help="só este dia (YYYY-MM-DD)")
    p_uso.add_argument(
        "--allow-shrink",
        action="store_true",
        help="aceita menos coletas que o uso atual (posições deduplicadas de propósito)",
    )
    sub.add_parser("tracado", help="cobertura + linha_kml_tracado (uso resolvido por versão)")
    p_consulta = sub.add_parser("consulta", help="qual traçado a linha usava num instante")
    p_consulta.add_argument("linha", help="ex.: 004")
    p_consulta.add_argument("sentido", choices=["ida", "volta"])
    p_consulta.add_argument("instante", type=datetime.fromisoformat, help='"2026-10-28 04:50"')
    sub.add_parser("run", help="extract + dedup + load + uso + tracado")
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
            pipeline.uso(args.day, args.allow_shrink)
        case "tracado":
            pipeline.tracado()
        case "consulta":
            from rmtc_go.core.settings import get_settings

            from .tracado import tracado_em

            print(tracado_em(args.linha, args.sentido, args.instante, get_settings()))
        case "run":
            pipeline.extract()
            pipeline.dedup()
            pipeline.load()
            pipeline.uso()
            pipeline.tracado()


if __name__ == "__main__":
    main()
