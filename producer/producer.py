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

def publicar_evento(evento):
    """
    Publica um evento (dicionário) no tópico do Kafka.
    """

    # O Kafka não entende dicionários Python — ele trafega dados como
    # bytes. Primeiro convertemos o dicionário em uma string JSON
    # (json.dumps faz essa conversão: dict -> texto no formato JSON).
    valor_json = json.dumps(evento)

    # confluent_kafka exige que o "value" seja bytes, não string.
    # .encode("utf-8") converte a string JSON em bytes.
    valor_bytes = valor_json.encode("utf-8")

    # Publica a mensagem no tópico. O parâmetro "value" é o conteúdo
    # do evento; poderíamos também usar "key" pra definir uma chave
    # de particionamento, mas não é necessário aqui (1 partição só).
    producer.produce(topic=TOPIC_NAME, value=valor_bytes)

    # Força o envio imediato da mensagem pro broker, em vez de deixar
    # bufferizada. flush() bloqueia até confirmar que foi entregue —
    # simples de entender e suficiente pro volume desse projeto.
    producer.flush()

    # Log simples pra você acompanhar no terminal o que está sendo publicado.
    print(f"Evento publicado: {evento}")

def main():
    """
    Loop principal: busca o clima de cada cidade e publica,
    repetindo a cada INTERVALO segundos.
    """
    # "while True" cria um loop infinito — o script fica rodando
    # continuamente até você interrompê-lo manualmente (Ctrl+C)
    # ou o processo ser encerrado.
    while True:

        # Percorre cada cidade da lista definida lá na configuração.
        for cidade in CIDADES:

            # Chama a função que busca os dados na API do OpenWeather.
            evento = buscar_clima(cidade)

            # buscar_clima retorna None quando algo deu errado
            # (status diferente de 200, erro de conexão, etc).
            # Só publicamos se o evento realmente veio preenchido —
            # isso evita que o script quebre tentando publicar "nada".
            if evento is not None:
                publicar_evento(evento)
            else:
                print(f"Pulando publicacao para {cidade} (erro na coleta)")

        # Depois de passar por todas as cidades, o script "dorme"
        # pelo tempo definido em INTERVALO (60 segundos, no seu caso)
        # antes de repetir o ciclo inteiro de novo.
        print(f"Aguardando {INTERVALO} segundos ate a proxima coleta...\n")
        time.sleep(INTERVALO)


# Esse "if" é um padrão do Python: o código dentro dele só roda
# quando o arquivo é executado diretamente (python producer/producer.py).
# Se, no futuro, você importar funções deste arquivo em outro script
# (ex: reaproveitar buscar_clima no consumer), o loop main() não vai
# disparar sozinho sem você chamar explicitamente.
if __name__ == "__main__":
    main()