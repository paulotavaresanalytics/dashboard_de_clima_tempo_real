# importação das bibliotecas

import os
import time
import json
import requests
import confluent_kafka
from dotenv import load_dotenv

# configuração
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

TOPIC_NAME = "clima-eventos"
BOOTSTRAP_SERVERS = "localhost:9092" 

CIDADES = [
    "Sao Paulo",
    "Dubai",
    "Rio de Janeiro",
]

INTERVALO = 60 #Intervalos em segundos a cada coleta

producer = confluent_kafka.Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS
})

def buscar_clima(cidade):
    """
    Busca dados de clima de uma cidade na API do OpenWeather.
    Retorna um dicionário com os dados relevantes, ou None em caso de erro.
    """

    # URL base do endpoint de clima atual da OpenWeather.
    # É sempre a mesma, independente da cidade — o que muda são os parâmetros.
    url = "https://api.openweathermap.org/data/2.5/weather"

    # Em vez de montar a URL manualmente com "?q=...&appid=...&units=...",
    # o requests permite passar um dicionário "params" — ele monta a
    # query string pra você e já cuida de coisas como espaços em nomes
    # de cidade (o que resolveu seu erro de "Malformed input" no curl).
    params = {
        "q": cidade,        # nome da cidade recebido como argumento da função
        "appid": API_KEY,   # a chave carregada do .env
        "units": "metric",  # pra vir em Celsius, não Kelvin (padrão da API)
    }

    # Faz a chamada HTTP GET. O requests já une url + params automaticamente.
    resposta = requests.get(url, params=params)

    # status_code 200 = sucesso. Qualquer outro valor indica problema
    # (401 = chave inválida, 404 = cidade não encontrada, etc — foi o
    # mesmo 401 que você viu no curl antes da chave ativar).
    if resposta.status_code != 200:
        print(f"Erro ao buscar clima de {cidade}: {resposta.status_code} - {resposta.text}")
        return None  # sinaliza pro chamador que não deu certo

    # Converte o corpo da resposta (JSON em texto) em um dicionário Python.
    dados = resposta.json()

    # Aqui é onde "filtramos" o JSON gigante da API, pegando só o que
    # interessa pro nosso evento. A estrutura do JSON da OpenWeather é:
    # { "main": {"temp": ..., "humidity": ...},
    #   "weather": [ {"description": ...} ],  <- é uma LISTA, por isso o [0]
    #   ... resto que não usamos }
    evento = {
        "cidade": cidade,
        "temperatura": dados["main"]["temp"],
        "umidade": dados["main"]["humidity"],
        "descricao": dados["weather"][0]["description"],
        "timestamp": time.time(),  # timestamp Unix (número), fácil de serializar em JSON
    }

    return evento

print(buscar_clima("London"))