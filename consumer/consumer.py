# consumer/consumer.py

import os
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Carrega variáveis do .env (mesmo padrão do producer, mesmo que o
# consumer ainda não use nenhuma chave de API — mantemos consistência).
load_dotenv()

# Mesmo tópico que o producer publica — precisa ser IDÊNTICO,
# senão o consumer nunca vai receber nada.
TOPIC_NAME = "clima-eventos"

# Mesmo endereço do broker usado no producer.
BOOTSTRAP_SERVERS = "localhost:9092"

# O "group.id" identifica um GRUPO de consumers. O Kafka usa isso pra
# saber até onde cada grupo já leu (offset). Se você rodar dois
# consumers com o MESMO group.id, eles dividem as mensagens entre si
# (paralelismo). Com group.id DIFERENTE, cada um lê tudo de novo,
# independentemente. Pro seu projeto, um único consumer com um
# group.id fixo é suficiente.
GROUP_ID = "clima-consumer-group"

# Cria a instância do Consumer com as configurações necessárias.
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    # "earliest" faz o consumer ler DESDE A PRIMEIRA mensagem do tópico
    # (mesmo as publicadas antes dele existir). Se fosse "latest",
    # ele só leria mensagens novas a partir do momento em que conectou.
    "auto.offset.reset": "earliest",
})

# Inscreve o consumer no tópico. Precisa ser uma lista, mesmo com
# um único tópico — a API permite se inscrever em vários ao mesmo tempo.
consumer.subscribe([TOPIC_NAME])


def main():
    """
    Loop principal: fica escutando o tópico continuamente e processa
    cada mensagem recebida.
    """
    print(f"Consumer iniciado. Escutando o topico '{TOPIC_NAME}'...\n")

    try:
        while True:
            # poll() verifica se chegou mensagem nova, esperando até
            # 1 segundo (timeout). Se nada chegar nesse intervalo,
            # retorna None e o loop simplesmente tenta de novo.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Nenhuma mensagem nova nesse ciclo — segue esperando.
                continue

            if msg.error():
                # Algo deu errado na leitura (não é o mesmo que "não
                # tem mensagem"). Log do erro e continua tentando.
                print(f"Erro no consumer: {msg.error()}")
                continue

            # msg.value() vem em bytes — precisamos decodificar de
            # volta pra string e depois converter de JSON pra dict
            # (processo inverso do que o producer fez).
            valor_bytes = msg.value()
            valor_json = valor_bytes.decode("utf-8")
            evento = json.loads(valor_json)

            # Por enquanto, só exibimos o evento recebido no terminal.
            # No próximo passo (persistência), vamos salvar isso em
            # CSV ou SQLite em vez de só imprimir.
            print(f"Evento recebido: {evento}")

    except KeyboardInterrupt:
        # Permite encerrar o script com Ctrl+C de forma limpa,
        # sem gerar um traceback feio no terminal.
        print("\nEncerrando consumer...")

    finally:
        # Fecha a conexão do consumer com o Kafka corretamente,
        # liberando recursos (importante mesmo em scripts simples).
        consumer.close()


if __name__ == "__main__":
    main()