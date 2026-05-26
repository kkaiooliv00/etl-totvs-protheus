"""ETL incremental de requisicoes HyperSync TOTVS para PostgreSQL."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from urllib3.util.retry import Retry


load_dotenv()

API_URL = (
    "https://transagil202609.protheus.cloudtotvs.com.br:11258"
    "/rest/v1/hypersync/request"
)
API_USER = "kaio.dantas"
API_PASSWORD_ENV = "API_PROTHEUS_PASSWORD"
DATABASE_URL_ENV = "DATABASE_URL"

JOB_CONFIG_PATH = Path(os.getenv("ETL_JOBS_FILE", "etl_jobs.yml"))
START_PAGE = 1
PAGE_SIZE = 100
DB_WRITE_CHUNK_SIZE = 1000
STAGING_FLUSH_RECORDS = 5000
UPSERT_BATCH_SIZE = 5000
POSTGRES_PARAMETER_LIMIT = 60000
REQUEST_TIMEOUT_SECONDS = 60
REQUEST_RETRY_COUNT = 3
PAGE_LOG_INTERVAL = 10
BUSINESS_TIMEZONE = ZoneInfo("America/Fortaleza")

TARGET_SCHEMA = "totvs"
BUSINESS_KEY = "super_chave"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("totvs_protheus_etl")


@dataclass(frozen=True)
class EtlJob:
    request_id: int
    target_table: str
    date_parameter: str | None = None
    business_key: str = BUSINESS_KEY
    business_key_columns: tuple[str, ...] = ()

    @property
    def staging_table(self) -> str:
        return f"{self.target_table}_staging"

    @property
    def dedup_staging_table(self) -> str:
        return f"{self.target_table}_staging_dedup"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def require_postgres_database_url() -> str:
    database_url = require_env(DATABASE_URL_ENV)
    try:
        parsed_url = urlparse(database_url)
    except ValueError as exc:
        raise RuntimeError(
            "DATABASE_URL invalida. Verifique se o host nao esta entre colchetes "
            "e se caracteres especiais da senha estao codificados, por exemplo @ como %40."
        ) from exc

    scheme = parsed_url.scheme
    if scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise RuntimeError(
            "DATABASE_URL invalida. Use uma URL PostgreSQL como "
            "postgresql+psycopg://usuario:senha@host:5432/postgres."
        )
    return database_url


def preflight_database_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


def load_jobs() -> list[EtlJob]:
    if not JOB_CONFIG_PATH.exists():
        raise RuntimeError(f"Arquivo de jobs nao encontrado: {JOB_CONFIG_PATH}")

    with JOB_CONFIG_PATH.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    raw_jobs = config.get("jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        raise RuntimeError("etl_jobs.yml deve conter uma lista nao vazia em jobs.")

    jobs: list[EtlJob] = []
    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            raise RuntimeError("Cada job deve conter request_id e target_table.")

        try:
            request_id = int(raw_job["request_id"])
            target_table = str(raw_job["target_table"]).strip()
            date_parameter = raw_job.get("date_parameter")
            business_key = str(raw_job.get("business_key", BUSINESS_KEY)).strip()
            raw_business_key_columns = raw_job.get("business_key_columns") or []
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Job invalido em {JOB_CONFIG_PATH}: {raw_job!r}"
            ) from exc

        if not target_table:
            raise RuntimeError("target_table nao pode ser vazio.")
        if not business_key:
            raise RuntimeError(f"business_key vazio em {target_table}.")
        if date_parameter is not None:
            date_parameter = str(date_parameter).strip()
            if not date_parameter:
                raise RuntimeError(f"date_parameter vazio em {target_table}.")
        if not isinstance(raw_business_key_columns, list):
            raise RuntimeError(f"business_key_columns deve ser uma lista em {target_table}.")
        business_key_columns = tuple(
            str(column).strip() for column in raw_business_key_columns if str(column).strip()
        )
        jobs.append(
            EtlJob(
                request_id=request_id,
                target_table=target_table,
                date_parameter=date_parameter,
                business_key=business_key,
                business_key_columns=business_key_columns,
            )
        )

    return jobs


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        if not all(isinstance(item, dict) for item in payload):
            raise ValueError("A API retornou itens que nao sao objetos JSON.")
        return payload

    if not isinstance(payload, dict):
        raise ValueError("A API retornou um JSON fora do formato esperado.")

    for key in ("items", "results", "records", "rows", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return extract_items(value)

    data = payload.get("data")
    if isinstance(data, (dict, list)):
        return extract_items(data)

    list_values = [value for value in payload.values() if isinstance(value, list)]
    if len(list_values) == 1:
        return extract_items(list_values[0])
    if not payload:
        return []

    raise ValueError("Nao foi possivel localizar os registros no JSON da API.")


def format_start_date(lookback_days: int | None) -> str:
    if lookback_days is None or lookback_days <= 0:
        raise RuntimeError("lookback_days deve ser maior que zero.")
    current_date = datetime.now(BUSINESS_TIMEZONE).date()
    return (current_date - timedelta(days=lookback_days)).strftime("%Y%m%d")


def request_data(job: EtlJob, page: int, lookback_days: int | None) -> dict[str, Any]:
    data: dict[str, Any] = {"page": page, "pageSize": PAGE_SIZE}
    if job.date_parameter and lookback_days is not None:
        data[job.date_parameter] = format_start_date(lookback_days)
    return data


def build_api_session() -> requests.Session:
    retry = Retry(
        total=REQUEST_RETRY_COUNT,
        connect=REQUEST_RETRY_COUNT,
        read=REQUEST_RETRY_COUNT,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"POST"},
    )
    session = requests.Session()
    session.auth = HTTPBasicAuth(API_USER, require_env(API_PASSWORD_ENV))
    session.headers.update({"Content-Type": "application/json"})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def iter_api_pages(job: EtlJob, lookback_days: int | None):
    page = START_PAGE

    with build_api_session() as session:
        while True:
            body = {
                "id": job.request_id,
                "data": request_data(job, page, lookback_days),
            }

            try:
                response = session.post(
                    API_URL,
                    json=body,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                if response.status_code >= 400:
                    logger.error(
                        "%s | API retornou HTTP %s na pagina %s. Resposta: %s",
                        job.target_table,
                        response.status_code,
                        page,
                        response.text[:1000],
                    )
                response.raise_for_status()
                page_items = extract_items(response.json())
            except requests.RequestException:
                logger.exception(
                    "Falha ao extrair pagina %s do request_id %s.",
                    page,
                    job.request_id,
                )
                raise
            except ValueError:
                logger.exception(
                    "Resposta invalida na pagina %s do request_id %s.",
                    page,
                    job.request_id,
                )
                raise

            if page == START_PAGE or page % PAGE_LOG_INTERVAL == 0 or len(page_items) < PAGE_SIZE:
                logger.info(
                    "%s | Pagina %s extraida com %s registros.",
                    job.target_table,
                    page,
                    len(page_items),
                )
            if len(page_items) > PAGE_SIZE:
                raise ValueError(
                    f"{job.target_table} retornou {len(page_items)} registros "
                    f"na pagina {page}; pageSize solicitado: {PAGE_SIZE}."
                )

            yield page, page_items
            if not page_items or len(page_items) < PAGE_SIZE:
                logger.info("%s | Paginacao encerrada na pagina %s.", job.target_table, page)
                break
            page += 1


def transform_records(job: EtlJob, records: list[dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.json_normalize(records, sep="_")

    if dataframe.empty:
        return dataframe
    if job.business_key not in dataframe.columns and job.business_key_columns:
        missing_columns = [
            column for column in job.business_key_columns if column not in dataframe.columns
        ]
        if missing_columns:
            raise KeyError(
                f"Colunas para chave composta ausentes em {job.target_table}: "
                f"{', '.join(missing_columns)}"
            )
        dataframe[job.business_key] = (
            dataframe.loc[:, list(job.business_key_columns)]
            .fillna("")
            .astype(str)
            .agg("|".join, axis=1)
        )

    if job.business_key not in dataframe.columns:
        received_columns = ", ".join(str(column) for column in dataframe.columns)
        raise KeyError(
            f"Coluna obrigatoria ausente: {job.business_key}. "
            f"Colunas recebidas em {job.target_table}: {received_columns}"
        )

    dataframe = dataframe.drop_duplicates(subset=[job.business_key], keep="last")
    return dataframe


def create_schema(connection: Connection) -> None:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(TARGET_SCHEMA)}"))


def create_target_from_staging(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    staging = qualified_table(TARGET_SCHEMA, job.staging_table)
    connection.execute(text(f"CREATE TABLE IF NOT EXISTS {target} AS TABLE {staging} WITH NO DATA"))


def drop_staging_tables(connection: Connection, job: EtlJob) -> None:
    connection.execute(text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.dedup_staging_table)}"))
    connection.execute(text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.staging_table)}"))


def add_missing_target_columns(connection: Connection, engine: Engine, job: EtlJob) -> None:
    inspector = inspect(connection)
    target_columns = {
        column["name"]
        for column in inspector.get_columns(job.target_table, schema=TARGET_SCHEMA)
    }
    for column in inspector.get_columns(job.staging_table, schema=TARGET_SCHEMA):
        column_name = column["name"]
        if column_name in target_columns:
            continue
        column_type = column["type"].compile(dialect=engine.dialect)
        connection.execute(
            text(
                f"ALTER TABLE {qualified_table(TARGET_SCHEMA, job.target_table)} "
                f"ADD COLUMN IF NOT EXISTS {quote_identifier(column_name)} {column_type}"
            )
        )


def ensure_unique_constraint(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    constraint_name = f"{job.target_table}_{job.business_key}_uk"
    constraint = quote_identifier(constraint_name)
    key = quote_identifier(job.business_key)

    constraint_exists = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint constraint_info
                JOIN pg_class table_info
                    ON table_info.oid = constraint_info.conrelid
                JOIN pg_namespace schema_info
                    ON schema_info.oid = table_info.relnamespace
                WHERE constraint_info.conname = :constraint_name
                    AND table_info.relname = :target_table
                    AND schema_info.nspname = :target_schema
            )
            """
        ),
        {
            "constraint_name": constraint_name,
            "target_table": job.target_table,
            "target_schema": TARGET_SCHEMA,
        },
    ).scalar_one()

    if constraint_exists:
        return

    connection.execute(text(f"ALTER TABLE {target} ADD CONSTRAINT {constraint} UNIQUE ({key})"))


def build_dedup_staging_sql(job: EtlJob, columns: list[str]) -> str:
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    source = qualified_table(TARGET_SCHEMA, job.staging_table)
    dedup = qualified_table(TARGET_SCHEMA, job.dedup_staging_table)
    key = quote_identifier(job.business_key)

    return f"""
        CREATE TABLE {dedup} AS
        SELECT {quoted_columns}
        FROM (
            SELECT
                {quoted_columns},
                ROW_NUMBER() OVER (
                    PARTITION BY {key}
                    ORDER BY ctid DESC
                ) AS __etl_row_number
            FROM {source}
        ) dedup_source
        WHERE __etl_row_number = 1
    """


def create_dedup_staging(connection: Connection, job: EtlJob, columns: list[str]) -> None:
    connection.execute(text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.dedup_staging_table)}"))
    connection.execute(text(build_dedup_staging_sql(job, columns)))


def build_upsert_sql(job: EtlJob, columns: list[str]) -> str:
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    update_columns = [column for column in columns if column != job.business_key]
    if update_columns:
        assignments = ", ".join(
            f"{quote_identifier(column)} = EXCLUDED.{quote_identifier(column)}"
            for column in update_columns
        )
        conflict_action = f"DO UPDATE SET {assignments}"
    else:
        conflict_action = "DO NOTHING"

    return f"""
        INSERT INTO {qualified_table(TARGET_SCHEMA, job.target_table)} ({quoted_columns})
        SELECT {quoted_columns}
        FROM {qualified_table(TARGET_SCHEMA, job.dedup_staging_table)}
        WHERE TRUE
        ORDER BY ctid
        LIMIT :batch_size
        OFFSET :offset
        ON CONFLICT ({quote_identifier(job.business_key)}) {conflict_action}
    """


def upsert_staging_in_batches(connection: Connection, job: EtlJob, columns: list[str]) -> None:
    staging = qualified_table(TARGET_SCHEMA, job.dedup_staging_table)
    staging_count = connection.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar_one()
    upsert_sql = text(build_upsert_sql(job, columns))

    for offset in range(0, staging_count, UPSERT_BATCH_SIZE):
        connection.execute(upsert_sql, {"batch_size": UPSERT_BATCH_SIZE, "offset": offset})
        logger.info(
            "%s | UPSERT ate o registro %s de %s.",
            job.target_table,
            min(offset + UPSERT_BATCH_SIZE, staging_count),
            staging_count,
        )


def write_page_to_staging(
    engine: Engine,
    job: EtlJob,
    dataframe: pd.DataFrame,
    if_exists: str,
) -> None:
    if dataframe.empty:
        return

    safe_chunksize = max(
        1,
        min(DB_WRITE_CHUNK_SIZE, POSTGRES_PARAMETER_LIMIT // max(len(dataframe.columns), 1)),
    )
    logger.info(
        "%s | Enviando %s registros para staging em lotes de ate %s linhas.",
        job.target_table,
        len(dataframe),
        safe_chunksize,
    )

    dataframe.to_sql(
        job.staging_table,
        engine,
        schema=TARGET_SCHEMA,
        if_exists=if_exists,
        index=False,
        chunksize=safe_chunksize,
        method="multi",
    )


def flush_staging_buffer(
    engine: Engine,
    job: EtlJob,
    buffer_frames: list[pd.DataFrame],
    if_exists: str,
) -> tuple[int, str]:
    if not buffer_frames:
        return 0, if_exists

    dataframe = pd.concat(buffer_frames, ignore_index=True)
    write_page_to_staging(engine, job, dataframe, if_exists)
    return len(dataframe), "append"


def finalize_load(engine: Engine, job: EtlJob, columns: list[str]) -> None:
    with engine.begin() as connection:
        create_target_from_staging(connection, job)
        add_missing_target_columns(connection, engine, job)
        ensure_unique_constraint(connection, job)
        create_dedup_staging(connection, job, columns)
        upsert_staging_in_batches(connection, job, columns)
        drop_staging_tables(connection, job)


def chunk_jobs(jobs: list[EtlJob], chunk_index: int, chunk_size: int) -> list[EtlJob]:
    if chunk_index < 0:
        raise RuntimeError("chunk_index nao pode ser negativo.")
    if chunk_size <= 0:
        raise RuntimeError("chunk_size deve ser maior que zero.")
    start = chunk_index * chunk_size
    return jobs[start : start + chunk_size]


def filter_jobs_by_request_id(jobs: list[EtlJob], request_id: int | None) -> list[EtlJob]:
    if request_id is None or request_id == 0:
        return jobs
    if request_id < 0:
        raise RuntimeError("request_id nao pode ser negativo. Use 0 para executar todos.")

    selected_jobs = [job for job in jobs if job.request_id == request_id]
    if not selected_jobs:
        available_ids = ", ".join(str(job.request_id) for job in jobs)
        raise RuntimeError(
            f"Nenhum job encontrado para request_id={request_id}. "
            f"IDs disponiveis: {available_ids}"
        )
    return selected_jobs


def chunk_matrix(jobs: list[EtlJob], chunk_size: int) -> str:
    chunk_count = (len(jobs) + chunk_size - 1) // chunk_size
    return json.dumps({"chunk_index": list(range(chunk_count))}, separators=(",", ":"))


def run_job(engine: Engine, job: EtlJob, lookback_days: int | None) -> None:
    started_at = time.perf_counter()
    logger.info("Iniciando job request_id=%s target_table=%s.", job.request_id, job.target_table)

    extracted_records = 0
    staged_records = 0
    processed_pages = 0
    columns: list[str] | None = None
    staging_mode = "replace"
    buffer_frames: list[pd.DataFrame] = []
    buffer_records = 0

    try:
        with engine.begin() as connection:
            create_schema(connection)
            drop_staging_tables(connection, job)

        for page, records in iter_api_pages(job, lookback_days):
            processed_pages = page
            extracted_records += len(records)
            if not records:
                continue

            dataframe = transform_records(job, records)
            if dataframe.empty:
                continue

            if columns is None:
                columns = list(dataframe.columns)
            buffer_frames.append(dataframe)
            buffer_records += len(dataframe)

            if buffer_records >= STAGING_FLUSH_RECORDS:
                flushed_records, staging_mode = flush_staging_buffer(
                    engine,
                    job,
                    buffer_frames,
                    staging_mode,
                )
                staged_records += flushed_records
                buffer_frames.clear()
                buffer_records = 0

            if page == START_PAGE or page % PAGE_LOG_INTERVAL == 0:
                logger.info(
                    "%s | Progresso: %s paginas, %s registros extraidos, %s enviados a staging.",
                    job.target_table,
                    page,
                    extracted_records,
                    staged_records,
                )

        if columns is None:
            logger.info("%s | Nenhum registro retornado; carga dispensada.", job.target_table)
            return

        flushed_records, staging_mode = flush_staging_buffer(
            engine,
            job,
            buffer_frames,
            staging_mode,
        )
        staged_records += flushed_records
        buffer_frames.clear()

        finalize_load(engine, job, columns)
    except SQLAlchemyError:
        logger.exception("%s | Falha durante a carga no PostgreSQL.", job.target_table)
        raise

    elapsed_seconds = time.perf_counter() - started_at
    logger.info(
        "%s | Carga concluida em %s.%s. Paginas=%s extraidos=%s staging=%s tempo=%.1fs.",
        job.target_table,
        TARGET_SCHEMA,
        job.target_table,
        processed_pages,
        extracted_records,
        staged_records,
        elapsed_seconds,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa jobs ETL TOTVS configurados no YAML.")
    parser.add_argument("--chunk-index", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Dias para filtro incremental. Use 0 para carga total sem filtro de data.",
    )
    parser.add_argument(
        "--request-id",
        type=int,
        default=0,
        help="Executa apenas o job com este request_id. Use 0 para executar todos.",
    )
    parser.add_argument("--print-chunk-matrix", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    lookback_days = None if args.lookback_days == 0 else args.lookback_days
    jobs = load_jobs()
    if args.print_chunk_matrix:
        print(chunk_matrix(jobs, args.chunk_size))
        return

    jobs = filter_jobs_by_request_id(jobs, args.request_id)
    selected_jobs = chunk_jobs(jobs, args.chunk_index, args.chunk_size)
    if not selected_jobs:
        logger.info("Bloco %s sem jobs; nada a executar.", args.chunk_index)
        return

    engine = create_engine(require_postgres_database_url(), pool_pre_ping=True)
    failed_jobs: list[str] = []
    succeeded_jobs: list[str] = []
    try:
        preflight_database_connection(engine)

        for job in selected_jobs:
            try:
                run_job(engine, job, lookback_days)
                succeeded_jobs.append(job.target_table)
            except Exception:
                logger.exception(
                    "Job request_id=%s target_table=%s falhou.",
                    job.request_id,
                    job.target_table,
                )
                failed_jobs.append(job.target_table)

        logger.info(
            "Resumo da execucao: %s jobs com sucesso, %s jobs com falha.",
            len(succeeded_jobs),
            len(failed_jobs),
        )
        if failed_jobs:
            logger.error("Jobs com falha: %s", ", ".join(failed_jobs))

        if failed_jobs:
            raise RuntimeError(f"Jobs com falha no bloco: {', '.join(failed_jobs)}")
    except (KeyError, RuntimeError):
        logger.exception("Pipeline interrompido por configuracao ou dados invalidos.")
        raise
    except Exception:
        logger.exception("Pipeline ETL interrompido.")
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
