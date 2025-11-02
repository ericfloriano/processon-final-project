import gradio as gr
import json
import os
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import apuracao as ap
import csvx
import time

# Caminho absoluto para garantir leitura correta
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "output")

DB_CONFIG = {
    "host": "localhost",
    "port": "6060",  # ajuste se necessário
    "user": "postgres",
    "password": "leonardo",
    "db": "postgres",
}


def esperar_arquivo(path, tentativas=10, intervalo=1):
    """Espera o arquivo aparecer (para sincronizar com gravação de apuração)."""
    for _ in range(tentativas):
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        time.sleep(intervalo)
    return False


def executar_apuracao_e_agent(
    aliq_icms,
    aliq_iss,
    aliq_pis,
    aliq_cofins,
    aliq_irpj,
    aliq_csll,
    presuncao,
    enquadramento,
    progress=gr.Progress(track_tqdm=False),
):
    try:
        status_text = "🔧 Preparando parâmetros..."
        progress(0, desc=status_text)

        # Atualiza parâmetros da apuração
        ap.ALIQ_ICMS_DEFAULT = aliq_icms
        ap.ALIQ_ISS_DEFAULT = aliq_iss
        ap.ALIQ_PIS_CUM = aliq_pis
        ap.ALIQ_COFINS_CUM = aliq_cofins
        ap.ALIQUOTA_IRPJ = aliq_irpj
        ap.ALIQUOTA_CSLL = aliq_csll
        ap.PRESUNCAO = presuncao
        ap.ENQUADRAMENTO = enquadramento

        progress(0.1, desc="⚙️ Conectando ao banco PostgreSQL...")

        # Cria engine PostgreSQL
        url = URL.create(
            "postgresql+psycopg2",
            username=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["db"],
        )
        engine = create_engine(url)

        progress(0.2, desc="📥 Executando apuração fiscal...")
        ap.process_from_postgres(engine, OUT_DIR)

        progress(0.8, desc="🧾 Gerando arquivos e gráficos...")

        resumo_path = os.path.join(OUT_DIR, "resumo_apuracao.json")
        faturamento_mes_path = os.path.join(OUT_DIR, "faturamento_por_mes.csv")

        if not esperar_arquivo(resumo_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {resumo_path}")
        if not esperar_arquivo(faturamento_mes_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {faturamento_mes_path}")

        with open(resumo_path, "r", encoding="utf-8") as f:
            resumo = json.load(f)

        # Gera gráfico Plotly
        fig = None
        df_mes = pd.read_csv(faturamento_mes_path)
        if not df_mes.empty:
            fig = px.bar(
                df_mes,
                x="mes_ano",
                y="faturamento",
                title="📈 Faturamento por Mês",
                labels={"mes_ano": "Mês/Ano", "faturamento": "Faturamento (R$)"},
                text_auto=".2s",
            )
            fig.update_layout(template="plotly_white", showlegend=False)
            fig.update_traces(marker_color="#2A8AF7")

        progress(0.9, desc="🤖 Gerando análise AI...")
        status_text = "🤖 Executando agente de IA..."

        # Executa o agente CSVX (modo síncrono compatível com agno 2.1.3)
        agent = csvx.agent
        pergunta = (
            "Analise o resumo da apuração fiscal e destaque os tributos de maior impacto, com base nos arquivos do output "
            "alíquotas sensíveis e oportunidades de otimização tributária."
        )
        try:
            result = agent.run(pergunta)
            resposta_ai = getattr(result, "content", None) or getattr(result, "output_text", None) or str(result)
            resposta_ai = resposta_ai.strip()

        except Exception as ai_err:
            resposta_ai = f"(⚠️ Erro ao executar o agente AI: {ai_err})"

        progress(1.0, desc="✅ Apuração concluída!")
        status_text = "✅ Apuração concluída com sucesso!"
        time.sleep(0.3)

        return (
            json.dumps(resumo, indent=2, ensure_ascii=False),
            resposta_ai,
            fig,
            status_text,
        )

    except Exception as e:
        return f"❌ Erro: {e}", "", None, f"❌ Falha: {e}"


# ------------------------------
# INTERFACE GRADIO
# ------------------------------
with gr.Blocks(title="💼 Apuração Fiscal com AI e Plotly") as demo:
    gr.Markdown("# 💼 Apuração Fiscal + Inteligência Artificial")
    gr.Markdown(
        "Ajuste as alíquotas e o regime tributário e clique em **Executar Apuração e Análise**.\n"
        "Durante o processamento, o sistema exibirá o progresso e carregará o JSON, gráfico e análise assim que disponíveis."
    )

    with gr.Tab("⚙️ Parâmetros da Apuração"):
        with gr.Row():
            aliq_icms = gr.Slider(0, 0.25, value=0.18, step=0.005, label="Alíquota ICMS")
            aliq_iss = gr.Slider(0, 0.1, value=0.05, step=0.005, label="Alíquota ISS")
            aliq_pis = gr.Slider(0, 0.02, value=0.0065, step=0.001, label="Alíquota PIS")
            aliq_cofins = gr.Slider(0, 0.1, value=0.03, step=0.001, label="Alíquota COFINS")

        with gr.Row():
            aliq_irpj = gr.Slider(0, 0.3, value=0.15, step=0.01, label="Alíquota IRPJ")
            aliq_csll = gr.Slider(0, 0.2, value=0.09, step=0.01, label="Alíquota CSLL")
            presuncao = gr.Slider(0, 0.5, value=0.08, step=0.01, label="Presunção Lucro (%)")

        enquadramento = gr.Dropdown(
            ["lucro_presumido", "lucro_real", "simples_nacional"],
            value="lucro_presumido",
            label="Enquadramento Tributário",
        )

        executar_btn = gr.Button("🚀 Executar Apuração e Análise", variant="primary")

    with gr.Tab("📊 Resultados"):
        with gr.Row():
            resumo_output = gr.Code(label="Resumo da Apuração (JSON)", language="json")
            grafico_plotly = gr.Plot(label="Gráfico de Faturamento")

        agent_output = gr.Textbox(label="🧠 Análise do Agente (IA)", lines=12)
        status_box = gr.Markdown("⏳ Aguardando execução...")

    executar_btn.click(
        fn=executar_apuracao_e_agent,
        inputs=[
            aliq_icms,
            aliq_iss,
            aliq_pis,
            aliq_cofins,
            aliq_irpj,
            aliq_csll,
            presuncao,
            enquadramento,
        ],
        outputs=[resumo_output, agent_output, grafico_plotly, status_box],
    )

demo.launch(server_name="0.0.0.0", share=True, server_port=7860)
