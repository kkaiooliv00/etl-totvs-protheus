# Documentacao do ETL TOTVS Protheus

Este projeto executa uma carga ETL da API HyperSync TOTVS Protheus para um banco PostgreSQL/Supabase.

O processo le os jobs configurados em `etl_jobs.yml`, busca os dados da API por paginacao, grava os dados em tabelas temporarias de staging no schema `totvs` e depois faz `UPSERT` nas tabelas finais usando uma chave de negocio.

## Arquivos principais

- `totvs_protheus_etl.py`: script principal do ETL.
- `etl_jobs.yml`: lista de tabelas/jobs que serao executados.
- `.github/workflows/totvs-protheus-etl.yml`: workflow do GitHub Actions.
- `requirements.txt`: dependencias Python.
- `.env`: variaveis locais de ambiente. Este arquivo nao deve subir para o GitHub.

## Variaveis de ambiente

O script precisa das seguintes variaveis:

```env
API_PROTHEUS_USER=usuario_da_api
API_PROTHEUS_PASSWORD=senha_da_api
DATABASE_URL=postgresql://postgres:SENHA@db.uedzntjrxgrkxjzdyaow.supabase.co:6543/postgres
```

No GitHub Actions, essas variaveis devem ser cadastradas como secrets do repositorio:

- `API_PROTHEUS_USER`
- `API_PROTHEUS_PASSWORD`
- `DATABASE_URL`

Caminho no GitHub:

`Settings > Secrets and variables > Actions > New repository secret`

Observacao: se a senha do banco tiver caracteres especiais, eles precisam estar escapados na URL. Por exemplo, `@` deve virar `%40`.

## Configuracao dos jobs

Os jobs ficam em `etl_jobs.yml`.

Formato basico:

```yaml
jobs:
  - request_id: 20
    target_table: SD1
    date_parameter: D1_EMISSAO
```

Campos:

- `request_id`: ID usado no body da API TOTVS.
- `target_table`: nome da tabela final no banco PostgreSQL, dentro do schema `totvs`.
- `date_parameter`: campo enviado para a API quando houver carga incremental por data.
- `business_key`: chave usada para deduplicacao e upsert. Se nao informado, usa `super_chave`.
- `business_key_columns`: lista de colunas usadas para montar uma chave composta quando a API nao retorna a chave pronta.

Exemplo com chave diferente:

```yaml
  - request_id: 30
    target_table: SYS_USR
    business_key: usr_id
```

Exemplo com carga sem filtro de data:

```yaml
  - request_id: 24
    target_table: SYS_COMPANY_FILIAIS
```

## Como adicionar uma nova tabela

Adicione um novo item em `etl_jobs.yml`:

```yaml
  - request_id: 99
    target_table: NOVA_TABELA
    date_parameter: CAMPO_DATA
```

Se a tabela nao tiver filtro de data, remova o `date_parameter`:

```yaml
  - request_id: 99
    target_table: NOVA_TABELA
```

Antes de colocar em execucao automatica, rode manualmente apenas esse `request_id` no GitHub Actions para validar a query da API.

## Como o filtro de data funciona

Quando o job tem `date_parameter` e o script roda com `--lookback-days 7`, o body enviado para a API inclui a data inicial no formato `YYYYMMDD`.

Exemplo:

```json
{
  "id": 20,
  "data": {
    "page": 1,
    "pageSize": 200,
    "D1_EMISSAO": "20260527"
  }
}
```

O calculo usa a data atual no fuso `America/Fortaleza`.

Se `--lookback-days 0` for usado, o script nao envia filtro de data. Isso representa carga total para os endpoints que nao filtram internamente.

## Paginacao

A paginacao atual esta configurada em:

```python
PAGE_SIZE = 200
```

O script inicia na pagina 1 e continua ate uma das condicoes abaixo:

- a API retornar uma pagina vazia;
- a API retornar menos registros que `PAGE_SIZE`;
- ocorrer erro definitivo apos as tentativas de retry.

## Carga no banco

Para cada job, o script segue esta ordem:

1. Cria o schema `totvs`, se nao existir.
2. Remove tabelas staging antigas do mesmo job.
3. Extrai paginas da API.
4. Transforma os registros em DataFrame.
5. Deduplica pelo campo de chave de negocio.
6. Envia os dados para a tabela staging via `COPY`, em blocos.
7. Cria a tabela final se ela ainda nao existir.
8. Adiciona colunas novas que aparecerem no staging e ainda nao existirem na tabela final.
9. Garante constraint unica na chave de negocio.
10. Cria staging deduplicado.
11. Faz `UPSERT` na tabela final.
12. Remove as tabelas staging.

As tabelas finais ficam em:

```text
totvs.NOME_DA_TABELA
```

## Comportamento com colunas novas

Se a API retornar uma coluna nova:

- a coluna aparece primeiro na tabela staging;
- antes do upsert, o script verifica a tabela final;
- se a coluna ainda nao existir na final, ela e criada com `ALTER TABLE ADD COLUMN IF NOT EXISTS`;
- em seguida os dados sao carregados normalmente.

Se a coluna existir na tabela final com tipo incompativel, o banco pode rejeitar a carga. Nesse caso, sera necessario ajustar manualmente o tipo da coluna no PostgreSQL ou tratar o tipo no script.

## Chave de negocio e duplicidade

Por padrao, o script usa `super_chave`.

Durante a carga:

- registros duplicados dentro da mesma pagina/bloco sao deduplicados;
- o staging final tambem e deduplicado por chave;
- a tabela final recebe uma constraint unica;
- o upsert usa `ON CONFLICT` para atualizar registros existentes e inserir novos.

Isso evita duplicidade quando a chave esta correta.

## Execucao local

Instale as dependencias:

```powershell
python -m pip install -r requirements.txt
```

Crie ou atualize o `.env` com:

```env
API_PROTHEUS_USER=usuario_da_api
API_PROTHEUS_PASSWORD=senha_da_api
DATABASE_URL=postgresql://postgres:SENHA@db.uedzntjrxgrkxjzdyaow.supabase.co:6543/postgres
```

Rodar todos os jobs com 7 dias:

```powershell
python totvs_protheus_etl.py --lookback-days 7
```

Rodar carga total:

```powershell
python totvs_protheus_etl.py --lookback-days 0
```

Rodar apenas uma tabela pelo `request_id`:

```powershell
python totvs_protheus_etl.py --lookback-days 7 --request-id 20
```

Ignorar um ou mais jobs:

```powershell
python totvs_protheus_etl.py --lookback-days 7 --exclude-request-ids 45,47
```

## Execucao no GitHub Actions

O workflow roda automaticamente todos os dias as 03:00 no horario de Recife/Fortaleza.

No GitHub Actions, o cron fica em UTC:

```yaml
- cron: "0 6 * * *"
```

Como Recife/Fortaleza e UTC-3, `06:00 UTC` equivale a `03:00` local.

No agendamento automatico, o workflow usa:

```text
lookback_days=7
```

Tambem e possivel executar manualmente:

`Actions > ETL TOTVS Protheus > Run workflow`

Parametros manuais:

- `lookback_days`: dias de historico. Use `0` para carga total.
- `request_id`: ID especifico de uma tabela. Use `0` para rodar todas.

## Logs

O script registra:

- inicio de cada job;
- paginas extraidas;
- progresso a cada intervalo de paginas;
- quantidade de registros enviados ao staging;
- resultado do `COPY`;
- resultado do `UPSERT`;
- resumo final de jobs com sucesso e falha.

Se um job falhar, o script continua para os proximos jobs e mostra no final quais tabelas falharam. O workflow ainda termina com erro se qualquer job falhar, para facilitar monitoramento.

## Falhas comuns

### Variavel de ambiente ausente

Exemplo:

```text
Variavel de ambiente obrigatoria nao definida: API_PROTHEUS_USER
```

Verifique se a secret existe no GitHub ou se a variavel esta no `.env` local.

### Erro HTTP 400 da API

Normalmente indica problema na query configurada no Protheus/HyperSync para aquele `request_id`.

Verifique o log retornado pela API. Erros comuns:

- coluna inexistente no SQL;
- sintaxe SQL invalida;
- filtro de data com nome de parametro incorreto.

### Timeout da API

Exemplo:

```text
Read timed out
```

Pode indicar lentidao no endpoint Protheus. O script tenta novamente com espera progressiva, mas se a API continuar sem responder, o job falha.

### Tipo incompativel no PostgreSQL

Exemplo:

```text
invalid input syntax for type bigint: "0.0"
```

Ocorre quando a tabela final ja tem uma coluna com tipo mais restrito que o valor vindo da API/staging. Ajuste o tipo no banco ou trate o campo no script.

## Performance e limites

Configuracoes atuais:

- API sequencial, sem paralelismo.
- `PAGE_SIZE = 200`.
- timeout de request: 60 segundos.
- retries: 3 tentativas extras.
- envio para staging a cada 10.000 registros.
- escrita em staging por `COPY`, mais rapida que inserts linha a linha.
- upsert final em lote.
- pool SQLAlchemy com `pool_size=5` e `max_overflow=2`.

O projeto evita paralelismo de API porque o servidor TOTVS apresentou instabilidade com requisicoes simultaneas.

## Checklist antes de subir alteracoes

1. Validar `etl_jobs.yml`.
2. Rodar localmente uma tabela pequena.
3. Rodar manualmente no GitHub apenas o `request_id` alterado.
4. Conferir logs de paginas, staging e upsert.
5. Verificar se a tabela final no Supabase recebeu os dados.
6. So depois liberar para execucao automatica.
