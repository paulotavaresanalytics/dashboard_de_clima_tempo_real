import os

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

st.set_page_config(
    page_title="Clima em tempo real",
    page_icon="🌦️",
    layout="wide",
)

LIMITE_EVENTOS = 5000


def obter_database_url() -> str | None:
    """Usa Secrets no Streamlit Cloud e .env no desenvolvimento local."""
    try:
        database_url = st.secrets.get("DATABASE_URL")
    except Exception:
        database_url = None
    return database_url or os.getenv("DATABASE_URL")


@st.cache_data(ttl=10, show_spinner=False)
def carregar_eventos(database_url: str) -> pd.DataFrame:
    """Busca no máximo 5.000 eventos dos últimos 30 dias."""
    with psycopg.connect(database_url, connect_timeout=10) as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                SELECT cidade, temperatura, umidade, descricao, coletado_em
                FROM public.clima_eventos
                WHERE coletado_em >= now() - interval '30 days'
                ORDER BY coletado_em DESC
                LIMIT %s
                """,
                (LIMITE_EVENTOS,),
            )
            registros = cursor.fetchall()

    dados = pd.DataFrame(
        registros,
        columns=["cidade", "temperatura", "umidade", "descricao", "coletado_em"],
    )
    if dados.empty:
        return dados

    dados["temperatura"] = pd.to_numeric(dados["temperatura"], errors="coerce")
    dados["umidade"] = pd.to_numeric(dados["umidade"], errors="coerce")
    dados["horario"] = (
        pd.to_datetime(dados["coletado_em"], utc=True, errors="coerce")
        .dt.tz_convert("America/Sao_Paulo")
    )
    dados["cidade"] = dados["cidade"].astype("string").str.strip()
    dados["descricao"] = dados["descricao"].astype("string").str.strip()
    dados = dados.dropna(
        subset=["cidade", "temperatura", "umidade", "horario"]
    )
    return dados.sort_values("horario", ascending=False)


st.title("🌦️ Clima em tempo real")
st.caption(
    "Dados publicados no Kafka, gravados pelo consumer no Supabase "
    "e atualizados aqui automaticamente."
)


@st.fragment(run_every="15s")
def exibir_painel() -> None:
    database_url = obter_database_url()
    if not database_url:
        st.error(
            "Falta configurar DATABASE_URL em App settings → Secrets. "
            "Use a URI PostgreSQL do Supabase no formato TOML."
        )
        return

    try:
        dados = carregar_eventos(database_url)
    except Exception as erro:
        st.error(
            "Não consegui consultar public.clima_eventos no Supabase. "
            "Confira se DATABASE_URL está configurada e se a tabela existe."
        )
        st.caption(f"Tipo do erro: {type(erro).__name__}")
        return

    if dados.empty:
        st.info(
            "Ainda não há eventos dos últimos 30 dias. "
            "Inicie o Kafka e o consumer configurado com a mesma DATABASE_URL."
        )
        return

    cidades = sorted(dados["cidade"].dropna().unique().tolist())
    cidades_selecionadas = st.multiselect(
        "Cidades",
        options=cidades,
        default=cidades,
        key="filtro_cidades",
    )
    filtrados = dados[dados["cidade"].isin(cidades_selecionadas)].copy()

    if filtrados.empty:
        st.warning("Selecione ao menos uma cidade para visualizar os dados.")
        return

    leituras_atuais = (
        filtrados.sort_values("horario", ascending=False)
        .drop_duplicates(subset="cidade", keep="first")
        .sort_values("cidade")
    )

    st.subheader("Última leitura por cidade")
    colunas = st.columns(min(len(leituras_atuais), 3))
    for indice, (_, leitura) in enumerate(leituras_atuais.iterrows()):
        with colunas[indice % len(colunas)]:
            st.metric(
                label=str(leitura["cidade"]),
                value=f'{leitura["temperatura"]:.1f} °C',
                delta=f'{leitura["umidade"]:.0f}% de umidade',
                help=(
                    f'{leitura["descricao"]} · '
                    f'{leitura["horario"].strftime("%d/%m/%Y %H:%M:%S")}'
                ),
            )

    st.subheader("Histórico de temperatura")
    historico_temperatura = filtrados.pivot_table(
        index="horario",
        columns="cidade",
        values="temperatura",
        aggfunc="mean",
    ).sort_index()
    st.line_chart(historico_temperatura, y_label="Temperatura (°C)")

    st.subheader("Histórico de umidade")
    historico_umidade = filtrados.pivot_table(
        index="horario",
        columns="cidade",
        values="umidade",
        aggfunc="mean",
    ).sort_index()
    st.line_chart(historico_umidade, y_label="Umidade (%)")

    st.subheader("Eventos recebidos")
    tabela = filtrados.rename(
        columns={
            "cidade": "Cidade",
            "temperatura": "Temperatura (°C)",
            "umidade": "Umidade (%)",
            "descricao": "Condição",
            "horario": "Horário (São Paulo)",
        }
    )
    tabela = tabela[
        ["Horário (São Paulo)", "Cidade", "Temperatura (°C)", "Umidade (%)", "Condição"]
    ]
    st.dataframe(
        tabela.sort_values("Horário (São Paulo)", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        f"Consulta limitada aos últimos 30 dias e até {LIMITE_EVENTOS:,} eventos. "
        "Atualização automática a cada 15 segundos."
    )


exibir_painel()
