from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Clima em tempo real",
    page_icon="🌦️",
    layout="wide",
)

CSV_PATH = Path(__file__).resolve().parents[1] / "data" / "clima_eventos.csv"
COLUNAS_ESPERADAS = ["cidade", "temperatura", "umidade", "descricao", "timestamp"]


@st.cache_data(ttl=5)
def carregar_eventos(caminho: str) -> pd.DataFrame:
    """Lê o CSV que o consumer grava na pasta data/."""
    dados = pd.read_csv(caminho)

    faltantes = set(COLUNAS_ESPERADAS) - set(dados.columns)
    if faltantes:
        raise ValueError(
            "O CSV não contém as colunas esperadas: " + ", ".join(sorted(faltantes))
        )

    dados = dados[COLUNAS_ESPERADAS].copy()
    dados["temperatura"] = pd.to_numeric(dados["temperatura"], errors="coerce")
    dados["umidade"] = pd.to_numeric(dados["umidade"], errors="coerce")
    timestamp_unix = pd.to_numeric(dados["timestamp"], errors="coerce")
    dados["horario"] = (
        pd.to_datetime(timestamp_unix, unit="s", utc=True, errors="coerce")
        .dt.tz_convert("America/Sao_Paulo")
    )
    dados["cidade"] = dados["cidade"].astype("string").str.strip()
    dados["descricao"] = dados["descricao"].astype("string").str.strip()

    dados = dados.dropna(subset=["cidade", "temperatura", "umidade", "horario"])
    return dados.sort_values("horario", ascending=False)


st.title("🌦️ Clima em tempo real")
st.caption(
    "Leituras de temperatura e umidade coletadas pelo producer, "
    "publicadas no Kafka e gravadas pelo consumer em CSV."
)


@st.fragment(run_every="15s")
def exibir_painel() -> None:
    if not CSV_PATH.exists():
        st.info(
            f"Ainda não encontrei o arquivo {CSV_PATH.relative_to(CSV_PATH.parents[1])}. "
            "Inicie o Kafka e o consumer; os dados aparecerão aqui assim que "
            "o consumer gravar o primeiro evento."
        )
        return

    try:
        dados = carregar_eventos(str(CSV_PATH))
    except (OSError, pd.errors.EmptyDataError, pd.errors.ParserError, ValueError) as erro:
        st.error(f"Não foi possível ler os dados do consumer: {erro}")
        return

    if dados.empty:
        st.info("O CSV ainda não contém leituras válidas.")
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
        f"Arquivo: {CSV_PATH} · Atualização automática a cada 15 segundos · "
        f"{len(filtrados)} eventos no arquivo."
    )


exibir_painel()
