# Dashboard de Clima em Tempo Real

Pipeline de dados climáticos com Kafka, CSVs diários, Supabase PostgreSQL e Streamlit. O consumer grava as leituras no banco e exporta uma cópia por dia em `results/`. Os arquivos CSV podem ser versionados no GitHub e importados no Supabase pelo GitHub Actions.

## Arquitetura

```
OpenWeather API → Producer → Kafka → Consumer → Supabase PostgreSQL → Streamlit Cloud
                                            └→ results/clima_eventos_YYYY-MM-DD.csv
GitHub Actions: results/*.csv → importador → Supabase PostgreSQL
```

## Componentes

- Producer: coleta dados climáticos por cidade e publica eventos no Kafka.
- Consumer: persiste cada evento no Supabase e acrescenta uma linha ao CSV diário.
- GitHub Actions: importa CSVs adicionados em `results/` para a tabela `public.clima_eventos`.
- Dashboard: lê o Supabase e permite visualizar 7, 30, 90 dias ou todo o histórico, com gráficos agregados por hora.

O importador ignora combinações de cidade e horário já existentes, permitindo reexecutar a importação. Os CSVs são lidos pelo GitHub Actions, que envia os dados ao Supabase; o próprio Supabase não busca arquivos no GitHub.

## Configuração

1. Crie um projeto Supabase e obtenha a URI PostgreSQL em **Connect → Direct → Session pooler → URI → Python**.
2. Copie `.env.example` para `.env` e configure `OPENWEATHER_API_KEY` e `DATABASE_URL` para a execução local.
3. Configure a mesma URI em **Streamlit → App settings → Secrets**:

```toml
DATABASE_URL = "sua_uri_postgresql_do_supabase"
```

4. No GitHub, abra **Settings → Secrets and variables → Actions → New repository secret** e crie o segredo `DATABASE_URL` com a URI PostgreSQL do Supabase. O workflow de importação depende desse segredo.
5. Não envie `.env`, senhas ou URIs de conexão ao repositório.

## CSVs históricos

Os arquivos precisam ficar em `results/` com o nome `clima_eventos_YYYY-MM-DD.csv` e as colunas:

```csv
cidade,temperatura,umidade,descricao,timestamp
```

O timestamp pode ser Unix em segundos (como o do consumer) ou data ISO 8601. Veja [results/README.md](results/README.md). Não há CSVs históricos nesta branch ainda; os arquivos começarão a ser gerados quando o consumer receber eventos. Dados anteriores ao início dessa exportação só podem ser adicionados a partir de exportações históricas reais.

Quando um CSV é enviado para `main`, o workflow `Importar CSVs históricos no Supabase` roda e insere seus registros. Também pode ser executado manualmente em **Actions → Importar CSVs históricos no Supabase → Run workflow**.

## Como rodar localmente

```bash
docker compose up -d
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python producer/producer.py
python consumer/consumer.py
```

O producer e o consumer rodam em terminais separados. O Kafka local precisa estar ativo para gerar novos CSVs. A importação dos CSVs para o Supabase é feita pelo workflow do GitHub Actions.

## Stack

Apache Kafka · Python · Psycopg 3 · PostgreSQL (Supabase) · OpenWeather API · Streamlit
