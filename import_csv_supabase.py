"""Importa um CSV para uma tabela PostgreSQL no schema totvs.

Uso basico:
    python import_csv_supabase.py caminho/arquivo.csv

Configuracao:
    DATABASE_URL deve estar definido no .env ou no ambiente.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg import sql


DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_SCHEMA = "totvs"
ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
SCRIPT_VERSION = "2026-07-03.2"


class SemicolonDialect(csv.Dialect):
    delimiter = ";"
    quotechar = '"'
    escapechar = None
    doublequote = True
    skipinitialspace = False
    lineterminator = "\r\n"
    quoting = csv.QUOTE_MINIMAL


def require_database_url() -> str:
    database_url = os.getenv(DATABASE_URL_ENV)
    if not database_url:
        raise RuntimeError(
            f"Variavel de ambiente obrigatoria nao definida: {DATABASE_URL_ENV}"
        )

    scheme = urlparse(database_url).scheme
    if scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise RuntimeError(
            "DATABASE_URL invalida. Use uma URL PostgreSQL, por exemplo: "
            "postgresql://usuario:senha@host:porta/database"
        )
    return database_url


def quote_identifier_part(raw_value: str, fallback: str) -> str:
    value = unicodedata.normalize("NFKD", raw_value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9_]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")

    if not value:
        value = fallback
    if value[0].isdigit():
        value = f"_{value}"

    if len(value) > 63:
        digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
        value = f"{value[:54]}_{digest}"
    return value


def table_name_from_csv(csv_path: Path) -> str:
    return quote_identifier_part(csv_path.stem, "csv_import")


def unique_column_names(raw_columns: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    columns: list[str] = []

    for index, raw_column in enumerate(raw_columns, start=1):
        base = quote_identifier_part(raw_column, f"coluna_{index}")
        count = seen.get(base, 0)
        seen[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count + 1}")

    return columns


def detect_encoding(csv_path: Path) -> tuple[str, str]:
    for encoding in ENCODINGS_TO_TRY:
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                while handle.read(1024 * 1024):
                    pass
            return encoding, "strict"
        except UnicodeDecodeError:
            continue
    return "cp1252", "replace"


def detect_dialect(csv_path: Path, encoding: str, errors: str) -> csv.Dialect:
    with csv_path.open("r", encoding=encoding, errors=errors, newline="") as handle:
        sample = handle.read(65536)

    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return SemicolonDialect


def read_header(
    csv_path: Path,
    encoding: str,
    errors: str,
    dialect: csv.Dialect,
) -> list[str]:
    with csv_path.open("r", encoding=encoding, errors=errors, newline="") as handle:
        reader = csv.reader(handle, dialect)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise RuntimeError("O CSV esta vazio.") from exc

    cleaned = [column.strip() for column in header]
    if not any(cleaned):
        raise RuntimeError("O cabecalho do CSV esta vazio.")
    return cleaned


def count_data_rows(
    csv_path: Path,
    encoding: str,
    errors: str,
    dialect: csv.Dialect,
) -> int:
    with csv_path.open("r", encoding=encoding, errors=errors, newline="") as handle:
        reader = csv.reader(handle, dialect)
        next(reader, None)
        return sum(1 for row in reader if row)


def ensure_schema_exists(conn: psycopg.Connection, schema_name: str) -> None:
    schema_exists = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
        )
        """,
        (schema_name,),
    ).fetchone()[0]
    if not schema_exists:
        raise RuntimeError(
            f'O schema "{schema_name}" nao existe neste banco. '
            "Confira a DATABASE_URL ou crie o schema antes da importacao."
        )


def table_exists(conn: psycopg.Connection, schema_name: str, table_name: str) -> bool:
    return conn.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        )
        """,
        (schema_name, table_name),
    ).fetchone()[0]


def create_empty_table(
    conn: psycopg.Connection,
    schema_name: str,
    table_name: str,
    columns: list[str],
) -> None:
    table_identifier = sql.Identifier(schema_name, table_name)

    column_defs = [
        sql.SQL("{} text").format(sql.Identifier(column)) for column in columns
    ]
    conn.execute(
        sql.SQL("CREATE TABLE {} ({})").format(
            table_identifier,
            sql.SQL(", ").join(column_defs),
        )
    )


def prepare_staging_table(
    conn: psycopg.Connection,
    schema_name: str,
    staging_table_name: str,
    columns: list[str],
) -> None:
    staging_identifier = sql.Identifier(schema_name, staging_table_name)
    conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(staging_identifier))
    create_empty_table(conn, schema_name, staging_table_name, columns)


def promote_staging_table(
    conn: psycopg.Connection,
    schema_name: str,
    staging_table_name: str,
    target_table_name: str,
    if_exists: str,
) -> None:
    target_identifier = sql.Identifier(schema_name, target_table_name)
    staging_identifier = sql.Identifier(schema_name, staging_table_name)

    if if_exists == "fail" and table_exists(conn, schema_name, target_table_name):
        raise RuntimeError(
            f'A tabela "{schema_name}"."{target_table_name}" ja existe. '
            "Use --if-exists replace para recriar."
        )
    if if_exists == "replace":
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target_identifier))
    elif if_exists != "fail":
        raise RuntimeError(f"Opcao --if-exists invalida: {if_exists}")

    conn.execute(
        sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            staging_identifier,
            sql.Identifier(target_table_name),
        )
    )


def copy_csv_to_table(
    conn: psycopg.Connection,
    csv_path: Path,
    encoding: str,
    errors: str,
    delimiter: str,
    schema_name: str,
    table_name: str,
    columns: list[str],
) -> None:
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    table_identifier = sql.Identifier(schema_name, table_name)
    copy_sql = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER {}, "
        "QUOTE '\"', ESCAPE '\"')"
    ).format(
        table_identifier,
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.Literal(delimiter),
    )

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with csv_path.open(
                "r",
                encoding=encoding,
                errors=errors,
                newline="",
            ) as handle:
                sent_bytes = 0
                next_log_mb = 10
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    data = chunk.encode("utf-8")
                    copy.write(data)
                    sent_bytes += len(data)
                    sent_mb = sent_bytes / (1024 * 1024)
                    if sent_mb >= next_log_mb:
                        print(
                            f"Importando CSV... {sent_mb:.1f} MB enviados "
                            f"de aproximadamente {file_size_mb:.1f} MB.",
                            flush=True,
                        )
                        next_log_mb += 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Le um CSV, conta as linhas e importa para uma tabela no schema totvs."
        )
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {SCRIPT_VERSION}",
    )
    parser.add_argument("csv_file", help="Caminho do arquivo CSV")
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"Schema destino. Padrao: {DEFAULT_SCHEMA}",
    )
    parser.add_argument(
        "--table-name",
        help="Nome da tabela destino. Padrao: nome do arquivo CSV.",
    )
    parser.add_argument(
        "--if-exists",
        choices=("fail", "replace"),
        default="fail",
        help="O que fazer se a tabela ja existir. Padrao: fail.",
    )
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=0,
        help=(
            "Timeout das instrucoes SQL em milissegundos. "
            "Use 0 para desativar. Padrao: 0."
        ),
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    csv_path = Path(args.csv_file).expanduser().resolve()
    if not csv_path.exists() or not csv_path.is_file():
        raise RuntimeError(f"Arquivo CSV nao encontrado: {csv_path}")

    database_url = require_database_url()
    schema_name = quote_identifier_part(args.schema, DEFAULT_SCHEMA)
    table_name = (
        quote_identifier_part(args.table_name, "csv_import")
        if args.table_name
        else table_name_from_csv(csv_path)
    )
    staging_table_name = quote_identifier_part(
        f"{table_name}_staging_import",
        "csv_staging_import",
    )

    encoding, encoding_errors = detect_encoding(csv_path)
    dialect = detect_dialect(csv_path, encoding, encoding_errors)
    header = read_header(csv_path, encoding, encoding_errors, dialect)
    columns = unique_column_names(header)
    row_count = count_data_rows(csv_path, encoding, encoding_errors, dialect)

    print(f"Arquivo: {csv_path}", flush=True)
    print(f"Linhas de dados: {row_count}", flush=True)
    print(f"Tabela destino: {schema_name}.{table_name}", flush=True)
    print(f"Tabela staging: {schema_name}.{staging_table_name}", flush=True)
    print(f"Encoding detectado: {encoding} ({encoding_errors})", flush=True)
    print(f"Delimitador detectado: {repr(dialect.delimiter)}", flush=True)

    print("Conectando ao Supabase/PostgreSQL...", flush=True)
    with psycopg.connect(database_url, connect_timeout=30) as conn:
        print(
            f"Ajustando statement_timeout para {args.statement_timeout_ms} ms...",
            flush=True,
        )
        conn.execute(
            sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(args.statement_timeout_ms)
            )
        )
        conn.commit()

        print("Conexao aberta. Validando schema e tabela destino...", flush=True)
        ensure_schema_exists(conn, schema_name)
        if args.if_exists == "fail" and table_exists(conn, schema_name, table_name):
            raise RuntimeError(
                f'A tabela "{schema_name}"."{table_name}" ja existe. '
                "Use --if-exists replace para recriar."
            )
        conn.commit()

        print("Criando tabela staging...", flush=True)
        prepare_staging_table(conn, schema_name, staging_table_name, columns)
        conn.commit()

        print("Staging criada. Iniciando importacao via COPY...", flush=True)
        copy_csv_to_table(
            conn,
            csv_path,
            encoding,
            encoding_errors,
            dialect.delimiter,
            schema_name,
            staging_table_name,
            columns,
        )
        conn.commit()

        print("COPY finalizado. Conferindo total importado na staging...", flush=True)
        imported_rows = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(schema_name, staging_table_name)
            )
        ).fetchone()[0]
        if imported_rows != row_count:
            raise RuntimeError(
                "Quantidade importada diferente da quantidade lida no CSV: "
                f"CSV={row_count}, staging={imported_rows}. "
                "A tabela principal nao foi alterada."
            )
        conn.commit()

        print("Contagem validada. Promovendo staging para tabela principal...", flush=True)
        promote_staging_table(
            conn,
            schema_name,
            staging_table_name,
            table_name,
            args.if_exists,
        )
        conn.commit()

    print(
        f"Importacao concluida: {imported_rows} linhas importadas em "
        f"{schema_name}.{table_name}.",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Erro: {exc}") from exc
