# ProcessON: Automação Fiscal com IA 🤖💼

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![Agno SDK](https://img.shields.io/badge/Agno%20SDK-2.0-green.svg)](https://agno.ai/)

O **ProcessON** é uma solução de automação inteligente para apuração fiscal e tributária, com foco no cálculo da **CSLL** (Contribuição Social sobre o Lucro Líquido) e outros impostos correlatos (ICMS, ISS, PIS, COFINS, IRPJ).

Desenvolvido como projeto final do curso **I2A2 Agentes Inteligentes**, o sistema utiliza o framework **Agno SDK 2.0** para ler dados fiscais de um banco PostgreSQL, realizar cálculos complexos e gerar análises diagnósticas usando IA generativa (GPT-5-mini).

---

## 🎯 O Problema

Muitos escritórios de contabilidade no Brasil (mais de 70 mil) ainda realizam a apuração de tributos complexos, como a CSLL, de forma manual. Este processo é:

- **Repetitivo e Lento**: Consome tempo valioso de analistas qualificados
- **Suscetível a Erros**: A complexidade da legislação aumenta a probabilidade de erros humanos
- **Arriscado**: Erros geram riscos de não conformidade fiscal e retrabalho

O ProcessON visa automatizar essa tarefa, com estimativas de **redução de até 80% no tempo de apuração** e **eliminação de erros manuais**.

---

## ✨ Funcionalidades

### 🔧 Motor de Cálculo (`apuracao.py`)
- Conecta-se ao PostgreSQL e lê as tabelas `notas` e `itens` (processando em chunks para eficiência)
- Realiza a apuração estimada de **6 tributos**: ICMS, ISS, PIS, COFINS, IRPJ, CSLL
- Detecta automaticamente colunas (ex: `chave_acesso`, `valor_total`)
- Converte valores monetários em formato brasileiro (ex: "R$ 1.234,56") para float

### 📊 Geração de Outputs
- Salva os resultados em arquivos estruturados
- Gera `resumo_apuracao.json` com totalizadores
- Cria múltiplos pivôs em CSV:
  - `faturamento_por_mes.csv`
  - `faturamento_por_cfop.csv`
  - E outros relatórios analíticos

### 🤖 Agente de IA (`csvx.py`)
- Utiliza o **Agno SDK** e **GPT-5-mini**
- Analisa os CSVs gerados com base de conhecimento sobre a Reforma Tributária
- Identifica riscos e oportunidades fiscais automaticamente

### 🌐 Interface Web (`app_gradio.py`)
- Interface interativa com **Gradio**
- Permite ajustar alíquotas e selecionar regime tributário
- Executa a apuração e visualiza resultados em tempo real
- Exibe análise da IA e gráficos de faturamento com **Plotly**

---

## 🛠️ Arquitetura e Tecnologias

A solução utiliza uma stack moderna para processamento de dados e IA:

| Camada | Tecnologia |
|--------|-----------|
| **Framework IA** | Agno SDK 2.0 (LangChain-based) |
| **Modelo de Linguagem** | OpenAI GPT-5-mini |
| **Banco de Conhecimento** | SQLite + LanceDB |
| **Agente de Dados** | CsvTools |
| **Motor de Cálculo** | Python (Pandas, SQLAlchemy) |
| **Banco de Origem** | PostgreSQL |
| **Interface** | Gradio / Plotly |

---

## 🚀 Instalação e Setup

### 1. Clone o repositório

```bash
git clone https://github.com/ericfloriano/processon-final-project
cd processon-final-project
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### 3. Configure o Ambiente

Crie um arquivo `.env` na raiz do projeto e adicione sua chave da OpenAI:

```env
OPENAI_API_KEY="sk-..."
```

Ajuste as credenciais do seu banco de dados PostgreSQL no script `app_gradio.py` (variável `DB_CONFIG`):

```python
DB_CONFIG = {
    'host': 'SEU_HOST',
    'port': 6060,
    'user': 'SEU_USER',
    'password': 'SUA_SENHA',
    'database': 'SEU_DB'
}
```

### 4. Estrutura de Diretórios

Certifique-se de que os seguintes diretórios existam:

- `output/` - Onde os relatórios CSV/JSON serão salvos
- `tributaria/` - Adicione seus arquivos `.md` que servirão como base de conhecimento

---

## ▶️ Como Usar

### 1️⃣ Interface Web (Recomendado)

A forma mais fácil de usar a solução completa é através da interface Gradio:

```bash
python app_gradio.py
```

Acesse a interface no navegador (ex: `http://127.0.0.1:7860`), ajuste os parâmetros na aba **"Parâmetros da Apuração"** e clique em **"Executar Apuração e Análise"**.

### 2️⃣ Motor de Cálculo (Via CLI)

Execute apenas o motor de apuração de dados via linha de comando:

```bash
python apuracao.py --host SEU_HOST --port 6060 --user SEU_USER --password SUA_SENHA --db SEU_DB --out output
```

### 3️⃣ Agente de Análise (Via CLI)

Após executar a apuração e gerar os CSVs no diretório `output/`, você pode interagir com o agente de IA:

```bash
python csvx.py
```

---

## 👥 Autores

- **Eric Bueno**
- **Leonardo Santos**
- **Letícia Machado**
- **Marco Andrey**

---

## ⚖️ Licença

Este projeto está licenciado sob a **Licença MIT**.

```
MIT License

Copyright (c) 2025 ProcessON (Eric Bueno, Leonardo Santos, Letícia Machado, Marco Andrey)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## 📧 Contato

Para dúvidas ou sugestões, entre em contato com a equipe do ProcessON.

---

**Desenvolvido com ❤️ pela equipe ProcessON**
