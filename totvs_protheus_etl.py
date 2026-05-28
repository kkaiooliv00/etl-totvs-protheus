"""ETL incremental de requisicoes HyperSync TOTVS para PostgreSQL.

Otimizacoes em relacao a versao anterior:
- Extracao da API com aiohttp + asyncio (requisicoes concorrentes por job)
- Escrita no staging via COPY binario (psycopg3 copy_records_to_table)
  em vez de INSERT em chunks pelo SQLAlchemy/pandas.to_sql
- Upsert final executado em uma unica instrucao SQL sem paginacao por OFFSET
  (elimina varreduras repetidas na staging table)
- Pool de conexoes reutilizado no engine (remove overhead de connect/disconnect)
- Buffer de DataFrames concatenado so uma vez por flush, sem .copy() desnecessario
- Conversao de tipos feita uma unica vez antes do COPY
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiohttp
import pandas as pd
import psycopg
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

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

# ── Tuning ──────────────────────────────────────────────────────────────
# Numero de paginas da API buscadas em paralelo por job (aiohttp).
# Aumentar aqui reduz o tempo de extracao, mas pressiona o rate-limit da API.
API_CONCURRENT_PAGES = 8

# Registros acumulados no buffer antes de despejar no staging via COPY.
# Valores maiores = menos round-trips; valores menores = menor uso de RAM.
STAGING_FLUSH_RECORDS = 10_000

# Tamanho do lote para upsert final (nao usa mais OFFSET; e apenas para log).
UPSERT_LOG_INTERVAL = 10_000

REQUEST_TIMEOUT_SECONDS = 60
REQUEST_RETRY_COUNT = 3
DATABASE_WRITE_RETRY_COUNT = 3
PAGE_LOG_INTERVAL = 10
BUSINESS_TIMEZONE = ZoneInfo("America/Fortaleza")

TARGET_SCHEMA = "totvs"
BUSINESS_KEY = "super_chave"
# ────────────────────────────────────────────────────────────────────────

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


# ── Utilitarios ──────────────────────────────────────────────────────────

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Variavel de ambiente obrigatoria nao definida: {name}")
    return value


def require_postgres_database_url() -> str:
    database_url = require_env(DATABASE_URL_ENV)
    parsed_url = urlparse(database_url)
    scheme = parsed_url.scheme
    if scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise RuntimeError(
            "DATABASE_URL invalida. Use uma URL PostgreSQL como "
            "postgresql+psycopg://usuario:senha@host:5432/postgres."
        )
    return database_url


def _normalize_dsn(database_url: str) -> str:
    """Converte URL SQLAlchemy para DSN nativo do psycopg3."""
    return database_url.replace("postgresql+psycopg://", "postgresql://").replace(
        "postgresql+psycopg2://", "postgresql://"
    )


def preflight_database_connection(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


# ── Carregamento de jobs ─────────────────────────────────────────────────

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
            raw_bk_cols = raw_job.get("business_key_columns") or []
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Job invalido em {JOB_CONFIG_PATH}: {raw_job!r}") from exc

        if not target_table:
            raise RuntimeError("target_table nao pode ser vazio.")
        if not business_key:
            raise RuntimeError(f"business_key vazio em {target_table}.")
        if date_parameter is not None:
            date_parameter = str(date_parameter).strip()
        business_key_columns = tuple(
            str(c).strip() for c in raw_bk_cols if str(c).strip()
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


# ── Extracao assincrona da API ───────────────────────────────────────────

def format_start_date(lookback_days: int) -> str:
    current_date = datetime.now(BUSINESS_TIMEZONE).date()
    return (current_date - timedelta(days=lookback_days)).strftime("%Y%m%d")


def _build_body(job: EtlJob, page: int, lookback_days: int | None) -> dict[str, Any]:
    data: dict[str, Any] = {"page": page, "pageSize": PAGE_SIZE}
    if job.date_parameter and lookback_days:
        data[job.date_parameter] = format_start_date(lookback_days)
    return {"id": job.request_id, "data": data}


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
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
    list_values = [v for v in payload.values() if isinstance(v, list)]
    if len(list_values) == 1:
        return extract_items(list_values[0])
    if not payload:
        return []
    raise ValueError("Nao foi possivel localizar os registros no JSON da API.")


async def _fetch_page(
    session: aiohttp.ClientSession,
    job: EtlJob,
    page: int,
    lookback_days: int | None,
    semaphore: asyncio.Semaphore,
) -> tuple[int, list[dict[str, Any]]]:
    """Busca uma unica pagina; retorna (page, items)."""
    async with semaphore:
        body = _build_body(job, page, lookback_days)
        for attempt in range(1, REQUEST_RETRY_COUNT + 2):
            try:
                async with session.post(
                    API_URL,
                    json=body,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS),
                ) as response:
                    if response.status >= 400:
                        text_body = await response.text()
                        logger.error(
                            "%s | HTTP %s na pagina %s: %s",
                            job.target_table, response.status, page, text_body[:500],
                        )
                    response.raise_for_status()
                    payload = await response.json(content_type=None)
                    return page, extract_items(payload)
            except (aiohttp.ClientError, ValueError):
                if attempt <= REQUEST_RETRY_COUNT:
                    wait = 2 * attempt
                    logger.warning(
                        "%s | Falha pagina %s tentativa %s/%s; aguardando %ss.",
                        job.target_table, page, attempt, REQUEST_RETRY_COUNT + 1, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                logger.exception("Falha definitiva pagina %s request_id %s.", page, job.request_id)
                raise
    # nunca atingido; satisfaz o type checker
    return page, []


async def iter_api_pages_async(
    job: EtlJob,
    lookback_days: int | None,
) -> AsyncIterator[tuple[int, list[dict[str, Any]]]]:
    """Pagina a API com janela de requisicoes paralelas.

    Estrategia:
    1. Dispara API_CONCURRENT_PAGES requisicoes em paralelo.
    2. Processa os resultados em ordem crescente de pagina.
    3. Se a ultima pagina retornar menos que PAGE_SIZE itens, para.
    """
    semaphore = asyncio.Semaphore(API_CONCURRENT_PAGES)
    auth = aiohttp.BasicAuth(API_USER, require_env(API_PASSWORD_ENV))
    headers = {"Content-Type": "application/json"}

    async with aiohttp.ClientSession(auth=auth, headers=headers) as session:
        page = START_PAGE
        finished = False

        while not finished:
            # Prepara um lote de paginas a buscar
            batch_pages = list(range(page, page + API_CONCURRENT_PAGES))
            tasks = [
                asyncio.create_task(_fetch_page(session, job, p, lookback_days, semaphore))
                for p in batch_pages
            ]
            results = await asyncio.gather(*tasks)

            for fetched_page, items in sorted(results, key=lambda x: x[0]):
                if fetched_page == START_PAGE or fetched_page % PAGE_LOG_INTERVAL == 0:
                    logger.info(
                        "%s | Pagina %s extraida com %s registros.",
                        job.target_table, fetched_page, len(items),
                    )
                yield fetched_page, items
                if not items or len(items) < PAGE_SIZE:
                    logger.info(
                        "%s | Paginacao encerrada na pagina %s.", job.target_table, fetched_page
                    )
                    finished = True
                    break

            page += API_CONCURRENT_PAGES


# ── Transformacao ────────────────────────────────────────────────────────

def transform_records(job: EtlJob, records: list[dict[str, Any]]) -> pd.DataFrame:
    dataframe = pd.json_normalize(records, sep="_")
    if dataframe.empty:
        return dataframe

    if job.business_key not in dataframe.columns and job.business_key_columns:
        missing = [c for c in job.business_key_columns if c not in dataframe.columns]
        if missing:
            raise KeyError(
                f"Colunas para chave composta ausentes em {job.target_table}: "
                f"{', '.join(missing)}"
            )
        dataframe[job.business_key] = (
            dataframe.loc[:, list(job.business_key_columns)]
            .fillna("")
            .astype(str)
            .agg("|".join, axis=1)
        )

    if job.business_key not in dataframe.columns:
        received = ", ".join(str(c) for c in dataframe.columns)
        raise KeyError(
            f"Coluna obrigatoria ausente: {job.business_key}. "
            f"Colunas recebidas em {job.target_table}: {received}"
        )

    return dataframe.drop_duplicates(subset=[job.business_key], keep="last")


# ── Escrita no staging via COPY (psycopg3) ───────────────────────────────

def _prepare_dataframe_for_copy(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas object/dict/list para string JSON de forma vetorizada."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            # Detecta se ha celulas que sao dict ou list (nao-escalares)
            sample = df[col].dropna().head(10)
            if any(isinstance(v, (dict, list)) for v in sample):
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                )
    return df


def _copy_dataframe_to_staging(
    dsn: str,
    job: EtlJob,
    df: pd.DataFrame,
    if_exists: str,
) -> None:
    """Usa psycopg3 COPY para inserir o DataFrame no staging — ordens de magnitude
    mais rapido que INSERT para volumes altos.

    'if_exists' pode ser 'replace' (primeira chamada) ou 'append'.
    """
    if df.empty:
        return

    df = _prepare_dataframe_for_copy(df)
    staging_fqn = qualified_table(TARGET_SCHEMA, job.staging_table)
    columns_sql = ", ".join(quote_identifier(c) for c in df.columns)
    col_types = {c: str(df[c].dtype) for c in df.columns}

    with psycopg.connect(dsn) as conn:
        if if_exists == "replace":
            # Recria a tabela de staging com os tipos corretos
            col_defs = []
            for col, dtype in col_types.items():
                if "int" in dtype:
                    pg_type = "BIGINT"
                elif "float" in dtype:
                    pg_type = "DOUBLE PRECISION"
                elif "bool" in dtype:
                    pg_type = "BOOLEAN"
                elif "datetime" in dtype:
                    pg_type = "TIMESTAMPTZ"
                else:
                    pg_type = "TEXT"
                col_defs.append(f"{quote_identifier(col)} {pg_type}")

            conn.execute(f"DROP TABLE IF EXISTS {staging_fqn}")
            conn.execute(
                f"CREATE TABLE {staging_fqn} ({', '.join(col_defs)})"
            )

        # COPY com texto delimitado por tabulacao — evita overhead de parsing JSON
        copy_sql = (
            f"COPY {staging_fqn} ({columns_sql}) "
            f"FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
        )

        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in df.itertuples(index=False, name=None):
                    formatted: list[str] = []
                    for val in row:
                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            formatted.append("\\N")
                        else:
                            # Escapa tabulacao e nova linha para o formato TEXT do COPY
                            formatted.append(
                                str(val)
                                .replace("\\", "\\\\")
                                .replace("\t", "\\t")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r")
                            )
                    copy.write_row(formatted)

        conn.commit()

    logger.info(
        "%s | COPY concluido: %s registros -> %s.",
        job.target_table, len(df), job.staging_table,
    )


# ── DDL e Upsert ─────────────────────────────────────────────────────────

def create_schema(connection: Connection) -> None:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(TARGET_SCHEMA)}"))


def drop_staging_tables(connection: Connection, job: EtlJob) -> None:
    connection.execute(
        text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.dedup_staging_table)}")
    )
    connection.execute(
        text(f"DROP TABLE IF EXISTS {qualified_table(TARGET_SCHEMA, job.staging_table)}")
    )


def create_target_from_staging(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    staging = qualified_table(TARGET_SCHEMA, job.staging_table)
    connection.execute(
        text(f"CREATE TABLE IF NOT EXISTS {target} AS TABLE {staging} WITH NO DATA")
    )


def add_missing_target_columns(connection: Connection, engine: Engine, job: EtlJob) -> None:
    inspector = inspect(connection)
    target_columns = {
        col["name"]
        for col in inspector.get_columns(job.target_table, schema=TARGET_SCHEMA)
    }
    for col in inspector.get_columns(job.staging_table, schema=TARGET_SCHEMA):
        col_name = col["name"]
        if col_name in target_columns:
            continue
        col_type = col["type"].compile(dialect=engine.dialect)
        connection.execute(
            text(
                f"ALTER TABLE {qualified_table(TARGET_SCHEMA, job.target_table)} "
                f"ADD COLUMN IF NOT EXISTS {quote_identifier(col_name)} {col_type}"
            )
        )


def ensure_unique_constraint(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    constraint_name = f"{job.target_table}_{job.business_key}_uk"
    constraint = quote_identifier(constraint_name)
    key = quote_identifier(job.business_key)

    exists = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE c.conname = :cn AND t.relname = :tn AND n.nspname = :sn
            )
            """
        ),
        {"cn": constraint_name, "tn": job.target_table, "sn": TARGET_SCHEMA},
    ).scalar_one()

    if not exists:
        connection.execute(
            text(f"ALTER TABLE {target} ADD CONSTRAINT {constraint} UNIQUE ({key})")
        )


def create_dedup_staging(connection: Connection, job: EtlJob, columns: list[str]) -> None:
    quoted_cols = ", ".join(quote_identifier(c) for c in columns)
    source = qualified_table(TARGET_SCHEMA, job.staging_table)
    dedup = qualified_table(TARGET_SCHEMA, job.dedup_staging_table)
    key = quote_identifier(job.business_key)

    connection.execute(
        text(f"DROP TABLE IF EXISTS {dedup}")
    )
    connection.execute(
        text(
            f"""
            CREATE TABLE {dedup} AS
            SELECT {quoted_cols}
            FROM (
                SELECT {quoted_cols},
                       ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY ctid DESC) AS __rn
                FROM {source}
            ) s
            WHERE __rn = 1
            """
        )
    )


def upsert_from_staging(connection: Connection, job: EtlJob, columns: list[str]) -> None:
    """Upsert em UMA unica instrucao SQL — sem loop de OFFSET.

    A versao anterior paginava por OFFSET dentro do loop Python, o que
    forçava o PostgreSQL a varrer a tabela de staging N vezes. Aqui
    delegamos toda a operacao ao Postgres em um unico round-trip.
    """
    quoted_cols = ", ".join(quote_identifier(c) for c in columns)
    update_cols = [c for c in columns if c != job.business_key]
    key = quote_identifier(job.business_key)
    dedup = qualified_table(TARGET_SCHEMA, job.dedup_staging_table)
    target = qualified_table(TARGET_SCHEMA, job.target_table)

    if update_cols:
        assignments = ", ".join(
            f"{quote_identifier(c)} = EXCLUDED.{quote_identifier(c)}"
            for c in update_cols
        )
        conflict_action = f"DO UPDATE SET {assignments}"
    else:
        conflict_action = "DO NOTHING"

    upsert_sql = f"""
        INSERT INTO {target} ({quoted_cols})
        SELECT {quoted_cols}
        FROM {dedup}
        ON CONFLICT ({key}) {conflict_action}
    """
    result = connection.execute(text(upsert_sql))
    logger.info(
        "%s | UPSERT concluido: %s linhas afetadas.",
        job.target_table,
        result.rowcount,
    )


def finalize_load(engine: Engine, job: EtlJob, columns: list[str]) -> None:
    with engine.begin() as conn:
        create_target_from_staging(conn, job)
        add_missing_target_columns(conn, engine, job)
        ensure_unique_constraint(conn, job)
        create_dedup_staging(conn, job, columns)
        upsert_from_staging(conn, job, columns)
        drop_staging_tables(conn, job)


# ── Orquestrador por job ─────────────────────────────────────────────────

def run_job(engine: Engine, job: EtlJob, lookback_days: int | None) -> None:
    started_at = time.perf_counter()
    logger.info("Iniciando job request_id=%s target_table=%s.", job.request_id, job.target_table)

    dsn = _normalize_dsn(require_env(DATABASE_URL_ENV))
    extracted_records = 0
    staged_records = 0
    columns: list[str] | None = None
    staging_mode = "replace"
    buffer_frames: list[pd.DataFrame] = []
    buffer_records = 0

    async def _run() -> None:
        nonlocal extracted_records, staged_records, columns, staging_mode
        nonlocal buffer_frames, buffer_records

        async for page, records in iter_api_pages_async(job, lookback_days):
            extracted_records += len(records)
            if not records:
                continue

            df = transform_records(job, records)
            if df.empty:
                continue

            if columns is None:
                columns = list(df.columns)

            buffer_frames.append(df)
            buffer_records += len(df)

            if buffer_records >= STAGING_FLUSH_RECORDS:
                merged = pd.concat(buffer_frames, ignore_index=True)
                _copy_dataframe_to_staging(dsn, job, merged, staging_mode)
                staged_records += len(merged)
                staging_mode = "append"
                buffer_frames.clear()
                buffer_records = 0

            if page == START_PAGE or page % PAGE_LOG_INTERVAL == 0:
                logger.info(
                    "%s | Progresso: pagina %s | extraidos %s | staging %s.",
                    job.target_table, page, extracted_records, staged_records,
                )

    try:
        with engine.begin() as conn:
            create_schema(conn)
            drop_staging_tables(conn, job)

        asyncio.run(_run())

        if columns is None:
            logger.info("%s | Nenhum registro retornado; carga dispensada.", job.target_table)
            return

        # Flush do buffer residual
        if buffer_frames:
            merged = pd.concat(buffer_frames, ignore_index=True)
            _copy_dataframe_to_staging(dsn, job, merged, staging_mode)
            staged_records += len(merged)
            buffer_frames.clear()

        finalize_load(engine, job, columns)

    except SQLAlchemyError:
        logger.exception("%s | Falha durante a carga no PostgreSQL.", job.target_table)
        raise

    elapsed = time.perf_counter() - started_at
    logger.info(
        "%s | Carga concluida em %s.%s | extraidos=%s staging=%s tempo=%.1fs.",
        job.target_table, TARGET_SCHEMA, job.target_table,
        extracted_records, staged_records, elapsed,
    )


# ── CLI e entrypoint ──────────────────────────────────────────────────────

def chunk_jobs(jobs: list[EtlJob], chunk_index: int, chunk_size: int) -> list[EtlJob]:
    if chunk_index < 0:
        raise RuntimeError("chunk_index nao pode ser negativo.")
    if chunk_size <= 0:
        raise RuntimeError("chunk_size deve ser maior que zero.")
    start = chunk_index * chunk_size
    return jobs[start: start + chunk_size]


def filter_jobs_by_request_id(jobs: list[EtlJob], request_id: int | None) -> list[EtlJob]:
    if request_id is None or request_id == 0:
        return jobs
    selected = [j for j in jobs if j.request_id == request_id]
    if not selected:
        available = ", ".join(str(j.request_id) for j in jobs)
        raise RuntimeError(
            f"Nenhum job encontrado para request_id={request_id}. "
            f"IDs disponiveis: {available}"
        )
    return selected


def parse_request_id_list(raw: str | None) -> set[int]:
    if not raw:
        return set()
    ids: set[int] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.add(int(item))
        except ValueError as exc:
            raise RuntimeError(f"Lista de request_ids invalida: {raw!r}") from exc
    return ids


def exclude_jobs_by_request_ids(jobs: list[EtlJob], ids: set[int]) -> list[EtlJob]:
    return [j for j in jobs if j.request_id not in ids] if ids else jobs


def chunk_matrix(jobs: list[EtlJob], chunk_size: int) -> str:
    count = (len(jobs) + chunk_size - 1) // chunk_size
    return json.dumps({"chunk_index": list(range(count))}, separators=(",", ":"))


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
    parser.add_argument(
        "--exclude-request-ids",
        default="",
        help="Lista separada por virgulas de request_ids que devem ser ignorados.",
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
    jobs = exclude_jobs_by_request_ids(jobs, parse_request_id_list(args.exclude_request_ids))
    selected_jobs = chunk_jobs(jobs, args.chunk_index, args.chunk_size)

    if not selected_jobs:
        logger.info("Bloco %s sem jobs; nada a executar.", args.chunk_index)
        return

    # Pool de conexoes reutilizadas (QueuePool) — evita handshake SSL a cada flush
    engine = create_engine(
        require_postgres_database_url(),
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,
    )

    failed_jobs: dict[str, str] = {}
    succeeded_jobs: list[str] = []

    try:
        preflight_database_connection(engine)

        for job in selected_jobs:
            try:
                run_job(engine, job, lookback_days)
                succeeded_jobs.append(job.target_table)
            except Exception as exc:
                logger.exception(
                    "Job request_id=%s target_table=%s falhou.",
                    job.request_id, job.target_table,
                )
                failed_jobs[job.target_table] = f"{type(exc).__name__}: {exc}"

        logger.info(
            "Resumo: %s jobs com sucesso, %s jobs com falha.",
            len(succeeded_jobs), len(failed_jobs),
        )
        if failed_jobs:
            for tbl, err in failed_jobs.items():
                logger.error("%s | %s", tbl, err)
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
