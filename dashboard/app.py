from datetime import datetime, timedelta, timezone
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


def obter_database_url() -> str | None:
    """Usa Secrets no Streamlit Cloud e .env no desenvolvimento local."""
    try:
        database_url = st.secrets.get("DATABASE_URL")
    except Exception:
        database_url = None
    return database_url or os.getenv("DATABASE_URL")


@st.cache_data(ttl=15, show_spinner=False)
def carregar_dados(
    database_url: str,
    inicio: datetime | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Busca leituras atuais, histórico horário e até 300 eventos recentes."""
    with psycopg.connect(database_url, connect_timeout=10) as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (cidade)
                    cidade, temperatura, umidade, descricao, coletado_em
                FROM public.clima_eventos
                ORDER BY cidade, coletado_em DESC
                """
            )
            leituras = cursor.fetchall()

            cursor.execute(
                """
                SELECT
                    date_trunc('hour', coletado_em) AS horario,
                    cidade,
                    avg(temperatura) AS temperatura,
                    avg(umidade) AS umidade
                FROM public.clima_eventos
                WHERE (%s::timestamptz IS NULL OR coletado_em >= %s)
                GROUP BY 1, 2
                ORDER BY 1 DESC
                """,
                (inicio, inicio),
            )
            historico = cursor.fetchall()

            cursor.execute(
                """
                SELECT cidade, temperatura, umidade, descricao, coletado_em
                FROM public.clima_eventos
                WHERE (%s::timestamptz IS NULL OR coletado_em >= %s)
                ORDER BY coletado_em DESC
                LIMIT 300
                """,
                (inicio, inicio),
            )
            eventos = cursor.fetchall()

    df_leituras = pd.DataFrame(
        leituras,
        columns=["cidade", "temperatura", "umidade", "descricao", "coletado_em"],
    )
    df_historico = pd.DataFrame(
        historico,
        columns=["horario", "cidade", "temperatura", "umidade"],
    )
    df_eventos = pd.DataFrame(
        eventos,
        columns=["cidade", "temperatura", "umidade", "descricao", "coletado_em"],
    )

    if not df_leituras.empty:
        df_leituras["horario"] = (
            pd.to_datetime(df_leituras["coletado_em"], utc=True, errors="coerce")
            .dt.tz_convert("America/Sao_Paulo")
        )
    if not df_historico.empty:
        df_historico["horario"] = (
            pd.to_datetime(df_historico["horario"], utc=True, errors="coerce")
            .dt.tz_convert("America/Sao_Paulo")
        )
    if not df_eventos.empty:
        df_eventos["horario"] = (
            pd.to_datetime(df_eventos["coletado_em"], utc=True, errors="coerce")
            .dt.tz_convert("America/Sao_Paulo")
        )

    return df_leituras, df_historico, df_eventos


st.title("🌦️ Clima em tempo real")
st.caption(
    "Leituras recentes do Supabase e histórico climático importado dos CSVs em results/."
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

    periodos = {
        "7 dias": 7,
        "30 dias": 30,
        "90 dias": 90,
        "Todo o histórico": None,
    }
    periodo = st.selectbox(
        "Período do histórico",
        options=list(periodos),
        index=1,
        key="periodo_historico",
    )
    dias = periodos[periodo]
    inicio = (
        datetime.now(timezone.utc) - timedelta(days=dias)
        if dias is not None
        else None
    )

    try:
        leituras, historico, eventos = carregar_dados(database_url, inicio)
    except Exception as erro:
        st.error(
            "Não consegui consultar public.clima_eventos no Supabase. "
            "Confira DATABASE_URL e se a tabela existe."
        )
        st.caption(f"Tipo do erro: {type(erro).__name__}")
        return

    if historico.empty:
        st.info(
            "Não há registros para esse período. Envie arquivos "
            "clima_eventos_YYYY-MM-DD.csv para results/ e aguarde a importação."
        )
        return

    cidades = sorted(historico["cidade"].dropna().unique().tolist())
    selecionadas = st.multiselect(
        "Cidades",
        options=cidades,
        default=cidades,
        key="filtro_cidades",
    )
    leituras = leituras[leituras["cidade"].isin(selecionadas)].copy()
    historico = historico[historico["cidade"].isin(selecionadas)].copy()
    eventos = eventos[eventos["cidade"].isin(selecionadas)].copy()

    if not selecionadas:
        st.warning("Selecione ao menos uma cidade para visualizar os dados.")
        return

    if not leituras.empty:
        st.subheader("Última leitura por cidade")
        colunas = st.columns(min(len(leituras), 3))
        for indice, (_, leitura) in enumerate(leituras.sort_values("cidade").iterrows()):
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
    historico_temperatura = historico.pivot_table(
        index="horario",
        columns="cidade",
        values="temperatura",
        aggfunc="mean",
    ).sort_index()
    st.line_chart(historico_temperatura, y_label="Temperatura média por hora (°C)")

    st.subheader("Histórico de umidade")
    historico_umidade = historico.pivot_table(
        index="horario",
        columns="cidade",
        values="umidade",
        aggfunc="mean",
    ).sort_index()
    st.line_chart(historico_umidade, y_label="Umidade média por hora (%)")

    st.subheader("Eventos recentes")
    if eventos.empty:
        st.info("Não há eventos para as cidades selecionadas nesse período.")
    else:
        tabela = eventos.rename(
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
            tabela,
            use_container_width=True,
            hide_index=True,
        )

    st.caption(
        "Os gráficos mostram médias horárias. A tabela lista até 300 eventos "
        "do período selecionado; escolha “Todo o histórico” para ver datas "
        "anteriores a 7 dias."
    )


exibir_painel()
