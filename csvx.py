import pandas as pd
from pathlib import Path
import asyncio
from agno.knowledge.knowledge import Knowledge
from agno.vectordb.lancedb import LanceDb
from agno.agent import Agent
from agno.tools.csv_toolkit import CsvTools
from agno.tools.visualization import VisualizationTools
from agno.models.openai import OpenAIChat
from agno.db.sqlite import SqliteDb
from dotenv import load_dotenv
import numpy as np
load_dotenv()


# Adicione antes de criar Knowledge
contents_db = SqliteDb(
    db_file="tmp/contents.db",  # Arquivo SQLite local
    knowledge_table="knowledge_contents"
)

# Create Knowledge Instance
knowledge = Knowledge(
    name="Basic SDK Knowledge Base",
    description="Agno 2.0 Knowledge Implementation",
        vector_db=LanceDb(
        table_name="vectors",
        uri="tmp/lancedb",  # Caminho local para o banco
    ),
    contents_db=contents_db  # Adicione esta linha
)

# Add from local file to the knowledge base
asyncio.run(
    knowledge.add_content_async(
        name="Tributos",
        path="tributaria",
        metadata={"user_tag": "Reforma tributária"},
        # Only include Markdown files
        include=["*.md"],
    )
)
# Defina o caminho da pasta com os CSVs
csv_folder = Path(__file__).parent.joinpath("output")

# Liste todos os arquivos CSV na pasta
csv_files = list(csv_folder.glob("*.csv"))


agent = Agent(
    tools=[CsvTools(csvs=csv_files)],
    model=OpenAIChat(id="gpt-5-mini"),
    markdown=True,
    db=contents_db,
    enable_user_memories=True,  # Habilita memória
    description="Especialista na nova regra da reforma tributária Brasileira",
    knowledge=knowledge,
    search_knowledge=True,
    instructions=[
        "Você é um consultor tributário especializado em análise fiscal de empresas. Inclusive dentro da nova Reforma tributária",
        "Lista dos top 100 itens/CFOPs/NCMs"
        "Analise os dados disponíveis (faturamento, tributos, apuração) e gere um resumo estratégico.",
        "Foque em destacar tributos de maior impacto, oportunidades de economia, e riscos fiscais.",
        "Use linguagem clara, objetiva e voltada para executivos, sem listar comandos técnicos.",
        "Apresente resultados em formato markdown, com subtítulos e tópicos organizados.",
    ],
)

if __name__ == "__main__":
    agent.cli_app(stream=False)