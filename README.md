# Dashboard de Clima em Tempo Real

Pipeline de streaming de dados climáticos com Kafka. Um producer coleta
dados de clima via API pública e publica eventos no Kafka; um consumer lê
esses eventos e os persiste em CSV, alimentando um dashboard em tempo real.

Kafka roda localmente via Docker (Confluent Platform Community), sem
dependência de serviços pagos.

## Arquitetura

```
OpenWeather API → Producer → Kafka → Consumer → CSV → Dashboard
```

## Status

**Concluído**
- Kafka local (Docker, modo KRaft) com o tópico `clima-eventos`
- Producer: coleta e publica dados de clima por cidade
- Consumer: lê os eventos e salva em `data/clima_eventos.csv`

**Em andamento**
- Tratamento de erros e reconexão no consumer
- Dashboard em Streamlit
- Documentação de variáveis de ambiente (`.env.example`)

## Como rodar

```bash
# 1. Subir o Kafka
docker compose up -d

# 2. Ambiente Python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Configurar .env
echo "OPENWEATHER_API_KEY=sua_chave_aqui" > .env

# 4. Executar
python producer/producer.py
python consumer/consumer.py   # em outro terminal
```

## Stack

Apache Kafka (Confluent Platform Community) · Python · OpenWeather API ·
GitHub Codespaces