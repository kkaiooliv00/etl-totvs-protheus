"""ETL incremental de requisicoes HyperSync TOTVS para PostgreSQL."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError


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
PAGE_SIZE = 25
UPSERT_BATCH_SIZE = 500
REQUEST_TIMEOUT_SECONDS = 60

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

    @property
    def staging_table(self) -> str:
        return f"{self.target_table}_staging"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def require_postgres_database_url() -> str:
    database_url = require_env(DATABASE_URL_ENV)
    scheme = urlparse(database_url).scheme
    if scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise RuntimeError(
            "DATABASE_URL invalida. Use uma URL PostgreSQL como "
            "postgresql+psycopg://usuario:senha@host:5432/postgres."
        )
    return database_url


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
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Job invalido em {JOB_CONFIG_PATH}: {raw_job!r}"
            ) from exc

        if not target_table:
            raise RuntimeError("target_table nao pode ser vazio.")
        jobs.append(EtlJob(request_id=request_id, target_table=target_table))

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


def extract_from_api(job: EtlJob) -> list[dict[str, Any]]:
    page = START_PAGE
    records: list[dict[str, Any]] = []

    with requests.Session() as session:
        session.auth = HTTPBasicAuth(API_USER, require_env(API_PASSWORD_ENV))
        session.headers.update({"Content-Type": "application/json"})

        while True:
            body = {
                "id": job.request_id,
                "data": {"page": page, "pageSize": PAGE_SIZE},
            }

            try:
                response = session.post(
                    API_URL,
                    json=body,
                    timeout=REQUEST_TIMEOUT_SECONDS,
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

            records.extend(page_items)
            if not page_items or len(page_items) < PAGE_SIZE:
                logger.info("%s | Paginacao encerrada na pagina %s.", job.target_table, page)
                break
            page += 1

    logger.info("%s | Extracao concluida com %s registros.", job.target_table, len(records))
    return records


def transform_records(job: EtlJob, records: list[dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.json_normalize(records, sep="_")
    logger.info("%s | Registros antes da deduplicacao: %s.", job.target_table, len(dataframe))

    if dataframe.empty:
        return dataframe
    if BUSINESS_KEY not in dataframe.columns:
        received_columns = ", ".join(str(column) for column in dataframe.columns)
        raise KeyError(
            f"Coluna obrigatoria ausente: {BUSINESS_KEY}. "
            f"Colunas recebidas em {job.target_table}: {received_columns}"
        )

    dataframe = dataframe.drop_duplicates(subset=[BUSINESS_KEY], keep="last")
    logger.info("%s | Registros depois da deduplicacao: %s.", job.target_table, len(dataframe))
    return dataframe


def create_schema(connection: Connection) -> None:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(TARGET_SCHEMA)}"))


def create_target_from_staging(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    staging = qualified_table(TARGET_SCHEMA, job.staging_table)
    connection.execute(text(f"CREATE TABLE IF NOT EXISTS {target} AS TABLE {staging} WITH NO DATA"))


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
    constraint = quote_identifier(f"{job.target_table}_{BUSINESS_KEY}_uk")
    key = quote_identifier(BUSINESS_KEY)
    connection.execute(
        text(
            f"""
            DO $$
            BEGIN
                ALTER TABLE {target} ADD CONSTRAINT {constraint} UNIQUE ({key});
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """
        )
    )


def build_upsert_sql(job: EtlJob, columns: list[str]) -> str:
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    update_columns = [column for column in columns if column != BUSINESS_KEY]
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
        FROM {qualified_table(TARGET_SCHEMA, job.staging_table)}
        WHERE TRUE
        ORDER BY ctid
        LIMIT :batch_size
        OFFSET :offset
        ON CONFLICT ({quote_identifier(BUSINESS_KEY)}) {conflict_action}
    """


def upsert_staging_in_batches(connection: Connection, job: EtlJob, columns: list[str]) -> None:
    staging = qualified_table(TARGET_SCHEMA, job.staging_table)
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


def load_to_postgres(engine: Engine, job: EtlJob, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        logger.info("%s | DataFrame vazio; carga dispensada.", job.target_table)
        return

    try:
        with engine.begin() as connection:
            create_schema(connection)

        dataframe.to_sql(
            job.staging_table,
            engine,
            schema=TARGET_SCHEMA,
            if_exists="replace",
            index=False,
            chunksize=PAGE_SIZE,
            method="multi",
        )
        with engine.begin() as connection:
            create_target_from_staging(connection, job)
            add_missing_target_columns(connection, engine, job)
            ensure_unique_constraint(connection, job)
            upsert_staging_in_batches(connection, job, list(dataframe.columns))
            connection.execute(text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.staging_table)}"))
    except SQLAlchemyError:
        logger.exception("%s | Falha durante a carga no PostgreSQL.", job.target_table)
        raise

    logger.info("%s | Carga concluida em %s.%s.", job.target_table, TARGET_SCHEMA, job.target_table)


def run_job(engine: Engine, job: EtlJob) -> None:
    logger.info("Iniciando job request_id=%s target_table=%s.", job.request_id, job.target_table)
    records = extract_from_api(job)
    dataframe = transform_records(job, records)
    load_to_postgres(engine, job, dataframe)


def main() -> None:
    engine = create_engine(require_postgres_database_url(), pool_pre_ping=True)
    try:
        for job in load_jobs():
            run_job(engine, job)
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
