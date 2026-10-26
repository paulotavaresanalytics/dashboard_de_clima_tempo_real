# Dashboard de Clima em Tempo Real

Pipeline de streaming de dados climáticos com Kafka e PostgreSQL hospedado no Supabase. O producer coleta dados da API OpenWeather e publica eventos no Kafka local; o consumer grava as leituras no banco remoto; o dashboard Streamlit consulta o PostgreSQL.

## Arquitetura

```
OpenWeather API → Producer → Kafka local → Consumer → Supabase PostgreSQL → Streamlit Cloud
```

## Status

- Kafka local via Docker Compose (modo KRaft), tópico `clima-eventos`
- Producer: coleta dados de clima por cidade
- Consumer: persiste os eventos recebidos na tabela `public.clima_eventos`
- Dashboard: lê temperatura, umidade, descrição e horário no Supabase

O consumer cria a tabela e o índice se ainda não existirem. Ele habilita RLS e remove permissões das roles `anon` e `authenticated`; o acesso do consumer e do Streamlit usa a URI privada de PostgreSQL.

## Configuração

1. Crie um projeto Supabase.
2. Copie a URI PostgreSQL em **Connect → Direct → Session pooler → URI → Python**.
3. Copie `.env.example` para `.env` e preencha `OPENWEATHER_API_KEY` e `DATABASE_URL`.
4. No app Streamlit, salve a mesma URI nos **Secrets**:

```toml
DATABASE_URL = "sua_uri_postgresql_do_supabase"
```

Nunca envie `.env` ou a URI de conexão ao GitHub. O arquivo `.env` deve permanecer ignorado pelo Git.

## Como rodar localmente

```bash
docker compose up -d
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python producer/producer.py
python consumer/consumer.py
```

O producer e o consumer rodam em terminais separados. O Kafka continua local; o Supabase é o armazenamento remoto compartilhado com o Streamlit Cloud.

## Limites do dashboard

O dashboard consulta somente os últimos 30 dias e no máximo 5.000 eventos por atualização. Isso limita a leitura por consulta; a retenção do banco ainda precisa ser administrada separadamente.

## Stack

Apache Kafka · Python · Psycopg 3 · PostgreSQL (Supabase) · OpenWeather API · Streamlit
