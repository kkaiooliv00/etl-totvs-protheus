"""ETL incremental de requisicoes HyperSync TOTVS para PostgreSQL.

Decisoes de arquitetura:
- Extracao da API: requisicoes SEQUENCIAIS (uma pagina por vez via requests
  sincrono). O servidor TOTVS Protheus derruba conexoes com qualquer nivel
  de paralelismo (ServerDisconnectedError persistente mesmo com 3 workers).
  Throughput aceitavel pois o gargalo e o banco, nao a API.
- Escrita no staging: COPY via psycopg3 — ordens de magnitude mais rapido
  que INSERT/pandas.to_sql para volumes altos.
- Upsert final: uma unica instrucao SQL (sem loop de OFFSET).
- Pool de conexoes reutilizado (QueuePool) — sem handshake SSL repetido.
- Encoding: respostas lidas como bytes e decodificadas com fallback
  utf-8 -> latin-1 -> windows-1252 (API TOTVS retorna latin-1 em alguns campos).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import requests
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
API_USER_ENV = "API_PROTHEUS_USER"
API_PASSWORD_ENV = "API_PROTHEUS_PASSWORD"
DATABASE_URL_ENV = "DATABASE_URL"

JOB_CONFIG_PATH = Path(os.getenv("ETL_JOBS_FILE", "etl_jobs.yml"))
START_PAGE = 1
PAGE_SIZE = 200

# ── Tuning ───────────────────────────────────────────────────────────────────
# Requisicoes sequenciais — sem paralelismo (servidor TOTVS nao suporta).
# Timeout de CONEXAO separado do timeout de LEITURA:
#   - CONNECT_TIMEOUT_SECONDS (15s): tempo maximo para estabelecer o TCP/TLS.
#     Curto de proposito — se o servidor nao aceitar a conexao logo, falha
#     rapido e entra no retry exponencial, evitando travar por 60s.
#   - READ_TIMEOUT_SECONDS (90s): tempo maximo aguardando a resposta da API
#     apos a conexao estar estabelecida. Maior pois algumas queries sao lentas.
CONNECT_TIMEOUT_SECONDS = 15
READ_TIMEOUT_SECONDS = 90
REQUEST_RETRY_COUNT = 3

# Pausa entre jobs consecutivos (segundos). Evita rate limiting / cooldown
# do servidor TOTVS quando varias conexoes sao abertas em sequencia rapida.
# Pode ser sobrescrito via --job-delay-seconds 0 para desabilitar.
DEFAULT_JOB_DELAY_SECONDS = 15

# Registros acumulados antes de despejar no staging via COPY.
STAGING_FLUSH_RECORDS = 10_000

PAGE_LOG_INTERVAL = 10
BUSINESS_TIMEZONE = ZoneInfo("America/Fortaleza")
TARGET_SCHEMA = "totvs"
BUSINESS_KEY = "super_chave"
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("totvs_protheus_etl")


# ── Modelos ───────────────────────────────────────────────────────────────────

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


# ── Utilitarios ───────────────────────────────────────────────────────────────

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
            "postgresql+psycopg://usuario:senha@host:6543/postgres."
        )
    return database_url


def _normalize_dsn(database_url: str) -> str:
    return (
        database_url
        .replace("postgresql+psycopg://", "postgresql://")
        .replace("postgresql+psycopg2://", "postgresql://")
    )


def _sqlalchemy_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql+psycopg://", 1)
    return database_url


def preflight_database_connection(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def qualified_table(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


# ── Carregamento de jobs ──────────────────────────────────────────────────────

def load_jobs() -> list[EtlJob]:
    if not JOB_CONFIG_PATH.exists():
        raise RuntimeError(f"Arquivo de jobs nao encontrado: {JOB_CONFIG_PATH}")

    with JOB_CONFIG_PATH.open(encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    raw_jobs = config.get("jobs")
    if not isinstance(raw_jobs, list) or not raw_jobs:
        raise RuntimeError("etl_jobs.yml deve conter uma lista nao vazia em jobs.")

    jobs: list[EtlJob] = []
    for raw in raw_jobs:
        if not isinstance(raw, dict):
            raise RuntimeError("Cada job deve conter request_id e target_table.")
        try:
            request_id = int(raw["request_id"])
            target_table = str(raw["target_table"]).strip()
            date_parameter = raw.get("date_parameter")
            business_key = str(raw.get("business_key", BUSINESS_KEY)).strip()
            raw_bk_cols = raw.get("business_key_columns") or []
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Job invalido em {JOB_CONFIG_PATH}: {raw!r}") from exc

        if not target_table:
            raise RuntimeError("target_table nao pode ser vazio.")
        if not business_key:
            raise RuntimeError(f"business_key vazio em {target_table}.")
        if date_parameter is not None:
            date_parameter = str(date_parameter).strip()
        business_key_columns = tuple(
            str(c).strip() for c in raw_bk_cols if str(c).strip()
        )
        jobs.append(EtlJob(
            request_id=request_id,
            target_table=target_table,
            date_parameter=date_parameter,
            business_key=business_key,
            business_key_columns=business_key_columns,
        ))
    return jobs


# ── Extracao sincrona da API ──────────────────────────────────────────────────

def format_start_date(lookback_days: int) -> str:
    current_date = datetime.now(BUSINESS_TIMEZONE).date()
    return (current_date - timedelta(days=lookback_days)).strftime("%Y%m%d")


def _build_body(job: EtlJob, page: int, lookback_days: int | None) -> dict[str, Any]:
    data: dict[str, Any] = {"page": page, "pageSize": PAGE_SIZE}
    if job.date_parameter and lookback_days:
        start_date = format_start_date(lookback_days)
        params = f"{job.date_parameter} >= {start_date}"
        data["params"] = params
        if page == START_PAGE:
            logger.info(
                "%s | Filtro de data aplicado: params='%s' (lookback_days=%s).",
                job.target_table, params, lookback_days,
            )
    body = {"id": job.request_id, "data": data}
    if page == START_PAGE:
        logger.debug("%s | Body pagina 1: %s", job.target_table, json.dumps(body, ensure_ascii=False))
    return body


def _decode_response(raw_bytes: bytes) -> str:
    """Decodifica bytes com fallback: utf-8 -> latin-1 -> windows-1252.
    A API TOTVS Protheus retorna latin-1 em alguns endpoints sem declara-lo
    no Content-Type, causando UnicodeDecodeError quando assume-se UTF-8.
    """
    for encoding in ("utf-8", "latin-1", "windows-1252"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("latin-1", errors="replace")


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
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
    # Payload sem lista reconhecivel = fim de dados ou resposta de erro
    logger.debug("extract_items: nenhuma lista encontrada no payload: %s", str(payload)[:300])
    return []


def _fetch_page_sync(
    session: requests.Session,
    job: EtlJob,
    page: int,
    lookback_days: int | None,
) -> list[dict[str, Any]]:
    """Busca uma unica pagina de forma sincrona com retry exponencial."""
    body = _build_body(job, page, lookback_days)

    for attempt in range(1, REQUEST_RETRY_COUNT + 2):
        try:
            response = session.post(
                API_URL,
                json=body,
                timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
            )
            if response.status_code >= 400:
                logger.error(
                    "%s | HTTP %s na pagina %s: %s",
                    job.target_table, response.status_code, page,
                    response.text[:500],
                )

            # Erros 5xx sao transientes: aplica retry com backoff
            if response.status_code >= 500:
                exc_5xx = requests.HTTPError(
                    f"HTTP {response.status_code}", response=response
                )
                if attempt <= REQUEST_RETRY_COUNT:
                    wait = 4 * attempt  # backoff maior para erros de servidor
                    logger.warning(
                        "%s | Erro 5xx pagina %s tentativa %s/%s; aguardando %ss.",
                        job.target_table, page, attempt, REQUEST_RETRY_COUNT + 1, wait,
                    )
                    time.sleep(wait)
                    continue
                raise exc_5xx

            response.raise_for_status()

            text_body = _decode_response(response.content)
            stripped = text_body.strip()

            # Corpo vazio ou JSON nulo = fim de paginacao
            if not stripped or stripped in ("{}", "[]", "null"):
                logger.info(
                    "%s | Pagina %s retornou corpo vazio; fim de paginacao.",
                    job.target_table, page,
                )
                return []

            try:
                payload = json.loads(text_body)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Resposta nao e JSON valido na pagina {page}: {text_body[:200]}"
                ) from exc

            return extract_items(payload)

        except (requests.RequestException, ValueError) as exc:
            if attempt <= REQUEST_RETRY_COUNT:
                wait = 2 * attempt
                logger.warning(
                    "%s | Falha pagina %s tentativa %s/%s; aguardando %ss. Erro: %s",
                    job.target_table, page, attempt, REQUEST_RETRY_COUNT + 1,
                    wait, exc,
                )
                time.sleep(wait)
                continue
            logger.exception(
                "Falha definitiva pagina %s request_id %s.", page, job.request_id
            )
            raise

    return []  # nunca atingido


# Limite de alerta: se o job tem filtro de data e ultrapassa esse numero de
# registros acumulados, loga um aviso de que o filtro pode estar sendo ignorado.
_DATE_FILTER_SUSPICIOUS_RECORD_THRESHOLD = 150_000


def iter_api_pages(
    job: EtlJob,
    lookback_days: int | None,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Pagina a API sequencialmente, uma pagina por vez."""
    password = require_env(API_PASSWORD_ENV)
    session = requests.Session()
    session.auth = (require_env(API_USER_ENV), password)
    session.headers.update({"Content-Type": "application/json"})

    total_extracted = 0
    date_filter_warned = False

    try:
        for page in range(START_PAGE, 99_999):
            items = _fetch_page_sync(session, job, page, lookback_days)
            total_extracted += len(items)

            if page == START_PAGE or page % PAGE_LOG_INTERVAL == 0:
                logger.info(
                    "%s | Pagina %s extraida com %s registros (total acumulado: %s).",
                    job.target_table, page, len(items), total_extracted,
                )

            # Alerta: filtro de data configurado mas volume esta suspeitamente alto
            if (
                not date_filter_warned
                and job.date_parameter
                and lookback_days
                and total_extracted >= _DATE_FILTER_SUSPICIOUS_RECORD_THRESHOLD
            ):
                date_filter_warned = True
                logger.warning(
                    "%s | ATENCAO: filtro de data '%s' configurado com lookback_days=%s, "
                    "mas ja foram extraidos %s registros (>= %s). "
                    "Verifique se a API esta realmente aplicando o filtro.",
                    job.target_table, job.date_parameter, lookback_days,
                    total_extracted, _DATE_FILTER_SUSPICIOUS_RECORD_THRESHOLD,
                )

            yield page, items

            if not items:
                logger.info(
                    "%s | Paginacao encerrada: pagina %s retornou 0 registros.", 
                    job.target_table, page
                )
                break

            if len(items) < PAGE_SIZE:
                logger.warning(
                    "%s | Pagina %s retornou %s registros (menor que o pageSize %s). "
                    "Continuando para garantir a extração total.",
                    job.target_table, page, len(items), PAGE_SIZE
                )
    finally:
        session.close()


# ── Transformacao ─────────────────────────────────────────────────────────────

def transform_records(job: EtlJob, records: list[dict[str, Any]]) -> pd.DataFrame:
    """Normaliza os registros e garante a presenca da business key quando possivel.

    Se a business_key nao existir nos dados E nao houver business_key_columns
    configuradas, o DataFrame e retornado sem a coluna de chave. Nesse caso,
    finalize_load realizara um INSERT total (sem upsert) na tabela destino.

    A deduplicacao e feita em uma unica etapa no banco de dados via
    create_dedup_staging() (ROW_NUMBER + ctid DESC), antes do upsert final.
    """
    df = pd.json_normalize(records, sep="_")
    if df.empty:
        return df

    if job.business_key not in df.columns and job.business_key_columns:
        missing = [c for c in job.business_key_columns if c not in df.columns]
        if missing:
            raise KeyError(
                f"Colunas para chave composta ausentes em {job.target_table}: "
                f"{', '.join(missing)}"
            )
        df[job.business_key] = (
            df.loc[:, list(job.business_key_columns)]
            .fillna("")
            .astype(str)
            .agg("|".join, axis=1)
        )

    if job.business_key not in df.columns:
        logger.warning(
            "%s | business_key '%s' ausente nos dados e sem business_key_columns "
            "configuradas. Carga sera realizada como INSERT total (sem upsert). "
            "Colunas recebidas: %s",
            job.target_table, job.business_key,
            ", ".join(str(c) for c in df.columns),
        )

    return df


# ── Escrita no staging via COPY (psycopg3) ────────────────────────────────────

def _prepare_dataframe_for_copy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(10)
            if any(isinstance(v, (dict, list)) for v in sample):
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, ensure_ascii=False)
                    if isinstance(v, (dict, list)) else v
                )
    return df


def _copy_dataframe_to_staging(
    dsn: str,
    job: EtlJob,
    df: pd.DataFrame,
    if_exists: str,
) -> None:
    if df.empty:
        return

    df = _prepare_dataframe_for_copy(df)
    staging_fqn = qualified_table(TARGET_SCHEMA, job.staging_table)
    columns_sql = ", ".join(quote_identifier(c) for c in df.columns)

    with psycopg.connect(dsn) as conn:
        if if_exists == "replace":
            col_defs = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                if "int" in dtype or "float" in dtype:
                    pg_type = "NUMERIC"
                elif "bool" in dtype:
                    pg_type = "BOOLEAN"
                elif "datetime" in dtype:
                    pg_type = "TIMESTAMPTZ"
                else:
                    pg_type = "TEXT"
                col_defs.append(f"{quote_identifier(col)} {pg_type}")

            conn.execute(f"DROP TABLE IF EXISTS {staging_fqn}")
            conn.execute(f"CREATE TABLE {staging_fqn} ({', '.join(col_defs)})")

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


# ── DDL e Upsert ──────────────────────────────────────────────────────────────

def create_schema(connection: Connection) -> None:
    connection.execute(
        text(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(TARGET_SCHEMA)}")
    )


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


def add_missing_target_columns(
    connection: Connection, engine: Engine, job: EtlJob
) -> None:
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


def deduplicate_target_table(connection: Connection, job: EtlJob) -> None:
    """Remove duplicatas da tabela alvo antes de criar a UNIQUE constraint.

    Necessario quando a tabela ja existia sem constraint e acumulou
    duplicatas em execucoes anteriores. Mantem o registro de menor ctid
    (o mais antigo fisicamente) para cada business_key.
    Se a tabela ainda nao existir, nao faz nada.
    """
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    key = quote_identifier(job.business_key)

    # Verifica se a tabela existe antes de tentar limpar
    table_exists = connection.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE t.relname = :tn AND n.nspname = :sn
            )
            """
        ),
        {"tn": job.target_table, "sn": TARGET_SCHEMA},
    ).scalar_one()

    if not table_exists:
        return

    # DELETE ... USING com self-join e muito mais eficiente que NOT IN para
    # tabelas grandes: evita full scan duplo e nao estoura statement_timeout.
    result = connection.execute(
        text(
            f"""
            DELETE FROM {target} t1
            USING {target} t2
            WHERE t1.{key} = t2.{key}
              AND t1.ctid > t2.ctid
            """
        )
    )
    if result.rowcount > 0:
        logger.warning(
            "%s | %s registros duplicados removidos da tabela alvo antes de criar constraint.",
            job.target_table, result.rowcount,
        )


def ensure_unique_constraint(connection: Connection, job: EtlJob) -> None:
    target = qualified_table(TARGET_SCHEMA, job.target_table)
    constraint_name = f"{job.target_table}_{job.business_key}_uk"
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
            text(
                f"ALTER TABLE {target} ADD CONSTRAINT "
                f"{quote_identifier(constraint_name)} UNIQUE ({key})"
            )
        )


def create_dedup_staging(
    connection: Connection, job: EtlJob, columns: list[str]
) -> None:
    quoted_cols = ", ".join(quote_identifier(c) for c in columns)
    source = qualified_table(TARGET_SCHEMA, job.staging_table)
    dedup = qualified_table(TARGET_SCHEMA, job.dedup_staging_table)
    key = quote_identifier(job.business_key)

    connection.execute(text(f"DROP TABLE IF EXISTS {dedup}"))
    result = connection.execute(
        text(
            f"""
            CREATE TABLE {dedup} AS
            SELECT {quoted_cols}
            FROM (
                SELECT {quoted_cols},
                       ROW_NUMBER() OVER (
                           PARTITION BY {key} ORDER BY ctid DESC
                       ) AS __rn
                FROM {source}
            ) s
            WHERE __rn = 1
            """
        )
    )

    count_res = connection.execute(text(f"SELECT count(*) FROM {dedup}"))
    count = count_res.scalar_one()
    logger.info("%s | Staging deduplicado criado com %s registros.", job.target_table, count)


def upsert_from_staging(
    connection: Connection, job: EtlJob, columns: list[str]
) -> None:
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

    result = connection.execute(
        text(
            f"""
            INSERT INTO {target} ({quoted_cols})
            SELECT {quoted_cols} FROM {dedup}
            ON CONFLICT ({key}) {conflict_action}
            """
        )
    )
    logger.info(
        "%s | UPSERT concluido: %s linhas afetadas.",
        job.target_table, result.rowcount,
    )


def insert_all_from_staging(
    connection: Connection, job: EtlJob, columns: list[str]
) -> None:
    """Realiza INSERT total do staging para a tabela destino (sem upsert).

    Utilizado quando a tabela nao possui super_chave. A tabela destino e
    truncada antes do INSERT para evitar duplicatas entre execucoes.
    """
    quoted_cols = ", ".join(quote_identifier(c) for c in columns)
    staging = qualified_table(TARGET_SCHEMA, job.staging_table)
    target = qualified_table(TARGET_SCHEMA, job.target_table)

    connection.execute(text(f"TRUNCATE TABLE {target}"))
    result = connection.execute(
        text(
            f"INSERT INTO {target} ({quoted_cols}) "
            f"SELECT {quoted_cols} FROM {staging}"
        )
    )
    logger.info(
        "%s | INSERT total concluido (sem super_chave): %s linhas inseridas.",
        job.target_table, result.rowcount,
    )


def _staging_has_business_key(connection: Connection, job: EtlJob) -> bool:
    """Verifica se a coluna business_key existe na tabela de staging."""
    inspector = inspect(connection)
    staging_columns = {
        col["name"]
        for col in inspector.get_columns(job.staging_table, schema=TARGET_SCHEMA)
    }
    return job.business_key in staging_columns


def finalize_load(engine: Engine, job: EtlJob, columns: list[str]) -> None:
    logger.info("%s | Iniciando finalize_load.", job.target_table)
    try:
        with engine.begin() as conn:
            t0 = time.perf_counter()
            create_target_from_staging(conn, job)
            logger.info("%s | create_target_from_staging: %.1fs.", job.target_table, time.perf_counter() - t0)

            t0 = time.perf_counter()
            add_missing_target_columns(conn, engine, job)
            logger.info("%s | add_missing_target_columns: %.1fs.", job.target_table, time.perf_counter() - t0)

            has_key = _staging_has_business_key(conn, job)

            if not has_key:
                # Tabela sem super_chave: realiza INSERT total (TRUNCATE + INSERT)
                logger.info(
                    "%s | super_chave '%s' ausente na tabela de staging. "
                    "Executando INSERT total (TRUNCATE + INSERT).",
                    job.target_table, job.business_key,
                )
                t0 = time.perf_counter()
                insert_all_from_staging(conn, job, columns)
                logger.info("%s | insert_all_from_staging: %.1fs.", job.target_table, time.perf_counter() - t0)
            else:
                # Tabela com super_chave: fluxo normal de upsert
                t0 = time.perf_counter()
                deduplicate_target_table(conn, job)
                logger.info("%s | deduplicate_target_table: %.1fs.", job.target_table, time.perf_counter() - t0)

                t0 = time.perf_counter()
                ensure_unique_constraint(conn, job)
                logger.info("%s | ensure_unique_constraint: %.1fs.", job.target_table, time.perf_counter() - t0)

                t0 = time.perf_counter()
                create_dedup_staging(conn, job, columns)
                logger.info("%s | create_dedup_staging: %.1fs.", job.target_table, time.perf_counter() - t0)

                t0 = time.perf_counter()
                upsert_from_staging(conn, job, columns)
                logger.info("%s | upsert_from_staging: %.1fs.", job.target_table, time.perf_counter() - t0)

            t0 = time.perf_counter()
            drop_staging_tables(conn, job)
            logger.info("%s | drop_staging_tables: %.1fs.", job.target_table, time.perf_counter() - t0)

    except Exception:
        logger.exception(
            "%s | Falha em finalize_load. Tabelas de staging preservadas para inspecao: %s, %s.",
            job.target_table, job.staging_table, job.dedup_staging_table,
        )
        raise


# ── Orquestrador por job ──────────────────────────────────────────────────────

def run_job(engine: Engine, job: EtlJob, lookback_days: int | None) -> None:
    started_at = time.perf_counter()
    logger.info(
        "Iniciando job request_id=%s target_table=%s.",
        job.request_id, job.target_table,
    )

    dsn = _normalize_dsn(require_env(DATABASE_URL_ENV))
    extracted_records = 0
    staged_records = 0
    columns: list[str] | None = None
    staging_mode = "replace"
    buffer_frames: list[pd.DataFrame] = []
    buffer_records = 0

    with engine.begin() as conn:
        create_schema(conn)
        drop_staging_tables(conn, job)

    try:
        for page, records in iter_api_pages(job, lookback_days):
            extracted_records += len(records)
            if not records:
                continue

            df = transform_records(job, records)
            if df.empty:
                continue

            # Acumula a uniao de todas as colunas vistas ate agora.
            # Paginas posteriores da API podem retornar campos extras;
            # ignora-los causaria erro no COPY ou perda silenciosa de dados.
            new_cols = [c for c in df.columns if c not in (columns or [])]
            if new_cols:
                columns = list(columns or []) + new_cols

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

    except SQLAlchemyError:
        logger.exception("%s | Falha durante a carga no PostgreSQL.", job.target_table)
        raise

    if columns is None:
        logger.info(
            "%s | Nenhum registro retornado; carga dispensada.", job.target_table
        )
        return

    # Flush do buffer residual
    if buffer_frames:
        merged = pd.concat(buffer_frames, ignore_index=True)
        _copy_dataframe_to_staging(dsn, job, merged, staging_mode)
        staged_records += len(merged)

    finalize_load(engine, job, columns)

    elapsed = time.perf_counter() - started_at
    logger.info(
        "%s | Carga concluida em %s.%s | extraidos=%s staging=%s tempo=%.1fs.",
        job.target_table, TARGET_SCHEMA, job.target_table,
        extracted_records, staged_records, elapsed,
    )


# ── CLI e entrypoint ──────────────────────────────────────────────────────────

def chunk_jobs(jobs: list[EtlJob], chunk_index: int, chunk_size: int) -> list[EtlJob]:
    if chunk_index < 0:
        raise RuntimeError("chunk_index nao pode ser negativo.")
    if chunk_size <= 0:
        raise RuntimeError("chunk_size deve ser maior que zero.")
    return jobs[chunk_index * chunk_size: (chunk_index + 1) * chunk_size]


def filter_jobs_by_request_id(
    jobs: list[EtlJob], request_id: int | None
) -> list[EtlJob]:
    if not request_id:
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


def exclude_jobs_by_request_ids(
    jobs: list[EtlJob], ids: set[int]
) -> list[EtlJob]:
    return [j for j in jobs if j.request_id not in ids] if ids else jobs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Executa jobs ETL TOTVS configurados no YAML."
    )
    parser.add_argument("--chunk-index", type=int, default=0)
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Dias para filtro incremental. Use 0 para carga total sem filtro.",
    )
    parser.add_argument(
        "--request-id",
        type=int,
        default=0,
        help="Executa apenas o job com este request_id. Use 0 para todos.",
    )
    parser.add_argument(
        "--exclude-request-ids",
        default="",
        help="Lista separada por virgulas de request_ids a ignorar.",
    )
    parser.add_argument(
        "--job-delay-seconds",
        type=int,
        default=DEFAULT_JOB_DELAY_SECONDS,
        help=(
            "Pausa em segundos entre jobs consecutivos. "
            "Evita rate limiting do servidor TOTVS. Use 0 para desabilitar."
        ),
    )
    return parser.parse_args()


JOB_MAX_ATTEMPTS = 3


def _run_job_with_retry(
    job: EtlJob,
    lookback_days: int | None,
    job_index: int,
    total_jobs: int,
) -> str | None:
    """Executa um job com retry (ate JOB_MAX_ATTEMPTS tentativas).

    Cada tentativa cria e descarta seu proprio engine para garantir
    isolamento total de conexoes entre jobs.

    Retorna None em caso de sucesso, ou a mensagem de erro em caso de falha
    definitiva apos todas as tentativas.
    """
    last_exc: Exception | None = None

    for attempt in range(1, JOB_MAX_ATTEMPTS + 1):
        engine: Engine | None = None
        try:
            engine = create_engine(
                _sqlalchemy_database_url(require_postgres_database_url()),
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=2,
                pool_pre_ping=True,
            )
            run_job(engine, job, lookback_days)
            logger.info(
                "Job %s/%s | request_id=%s | tabela=%s concluido com sucesso"
                " na tentativa %s/%s.",
                job_index, total_jobs,
                job.request_id, job.target_table,
                attempt, JOB_MAX_ATTEMPTS,
            )
            return None  # sucesso

        except Exception as exc:
            last_exc = exc
            if attempt < JOB_MAX_ATTEMPTS:
                wait = 10 * attempt  # backoff: 10s na 1a falha, 20s na 2a
                logger.warning(
                    "Job request_id=%s target_table=%s falhou na tentativa"
                    " %s/%s. Aguardando %ss antes de tentar novamente. Erro: %s",
                    job.request_id, job.target_table,
                    attempt, JOB_MAX_ATTEMPTS, wait, exc,
                )
                time.sleep(wait)
            else:
                logger.exception(
                    "Job request_id=%s target_table=%s falhou em todas as"
                    " %s tentativas.",
                    job.request_id, job.target_table, JOB_MAX_ATTEMPTS,
                )

        finally:
            if engine is not None:
                engine.dispose()
                logger.debug(
                    "%s | Engine descartado apos tentativa %s.",
                    job.target_table, attempt,
                )

    return f"{type(last_exc).__name__}: {last_exc}"


def main() -> None:
    args = parse_args()
    lookback_days = None if args.lookback_days == 0 else args.lookback_days

    jobs = load_jobs()
    jobs = filter_jobs_by_request_id(jobs, args.request_id or None)
    jobs = exclude_jobs_by_request_ids(
        jobs, parse_request_id_list(args.exclude_request_ids)
    )
    selected_jobs = chunk_jobs(jobs, args.chunk_index, args.chunk_size)

    if not selected_jobs:
        logger.info("Nenhum job selecionado; nada a executar.")
        return

    # Valida conexao com o banco uma unica vez antes de comecar
    _preflight_engine = create_engine(
        _sqlalchemy_database_url(require_postgres_database_url()),
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        preflight_database_connection(_preflight_engine)
    finally:
        _preflight_engine.dispose()

    logger.info(
        "Jobs selecionados (%s no total): %s.",
        len(selected_jobs),
        [job.request_id for job in selected_jobs],
    )

    failed_jobs: dict[str, str] = {}
    succeeded_jobs: list[str] = []

    try:
        for job_index, job in enumerate(selected_jobs, start=1):
            logger.info(
                "%-60s", "=" * 60,
            )
            logger.info(
                "Iniciando job %s/%s | request_id=%s | tabela=%s.",
                job_index, len(selected_jobs), job.request_id, job.target_table,
            )

            error_msg = _run_job_with_retry(
                job, lookback_days, job_index, len(selected_jobs)
            )

            if error_msg is None:
                succeeded_jobs.append(job.target_table)
            else:
                failed_jobs[job.target_table] = error_msg

            # Pausa entre jobs para evitar rate limiting / cooldown do servidor
            if job_index < len(selected_jobs) and args.job_delay_seconds > 0:
                logger.info(
                    "Aguardando %ss antes do proximo job (--job-delay-seconds).",
                    args.job_delay_seconds,
                )
                time.sleep(args.job_delay_seconds)

        logger.info("%-60s", "=" * 60)
        logger.info(
            "Pipeline concluido: %s jobs com sucesso, %s jobs com falha.",
            len(succeeded_jobs), len(failed_jobs),
        )
        if succeeded_jobs:
            logger.info("Sucesso: %s.", ", ".join(succeeded_jobs))
        if failed_jobs:
            logger.error("Falhas ao final do pipeline:")
            for tbl, err in failed_jobs.items():
                logger.error("  %s => %s", tbl, err)
            raise RuntimeError(
                f"Jobs com falha: {', '.join(failed_jobs)}"
            )

    except (KeyError, RuntimeError):
        logger.exception("Pipeline interrompido.")
        raise
    except Exception:
        logger.exception("Pipeline ETL interrompido.")
        raise


if __name__ == "__main__":
    main()
