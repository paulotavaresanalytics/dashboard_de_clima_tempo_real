# Arquivos CSV de clima

O consumer gera um arquivo por data local de São Paulo, com o padrão:

`clima_eventos_YYYY-MM-DD.csv`

Cabeçalho esperado:

```csv
cidade,temperatura,umidade,descricao,timestamp
```

Cada linha corresponde a um evento recebido do Kafka. A coluna `timestamp` é Unix em segundos (UTC).

Para importar arquivos históricos, coloque-os nesta pasta mantendo esse nome e cabeçalho e envie-os para a branch `main`. O workflow do GitHub Actions lê os CSVs e grava no Supabase usando o segredo `DATABASE_URL`. Registros já existentes são ignorados para evitar duplicatas.

Não inclua chaves de API, senhas ou URIs de conexão nos CSVs. Não há exportações históricas neste repositório ainda; o consumer começará a gerar os arquivos quando for executado.
