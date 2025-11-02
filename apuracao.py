#!/usr/bin/env python3
"""
apuracao.py

Leitura direta do PostgreSQL (tabelas 'notas' e 'itens'), apuração estimada de:
ICMS, ISS, PIS, COFINS, IRPJ e CSLL (lucro presumido por padrão).

 - Detecta automaticamente colunas chave/valor (normaliza nomes)
 - Converte valores em formato brasileiro para float
 - Processa 'itens' em chunks
 - Gera CSV/JSON de resumo e pivôs

Uso:
  python apuracao.py --host HOST --user USER --password PWD --db DBNAME --out output_dir --chunk 20000
"""
import os
import re
import json
import argparse
from datetime import datetime
from collections import defaultdict

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from datetime import datetime, timezone

# -------------------------
# CONFIGURAÇÕES (edite se precisar)
# -------------------------
DEFAULT_CHUNK = 20000
OUT_DIR_DEFAULT = "output"

# alíquotas padrões (ajuste conforme necessário)
ALIQ_ICMS_DEFAULT = 0.18
ALIQ_ISS_DEFAULT = 0.05
ALIQ_PIS_CUM = 0.0065
ALIQ_COFINS_CUM = 0.03

# IRPJ / CSLL
ESTIMAR_IRPJ_CSLL = True
ENQUADRAMENTO = "lucro_presumido"  # lucro_presumido | lucro_real | simples_nacional
PRESUNCAO = 0.08     # presunção típica comércio; ajuste para serviços (0.32) se necessário
ALIQUOTA_IRPJ = 0.15
ALIQUOTA_CSLL = 0.09
ESTIMATED_MARGIN = 0.10  # usado somente se lucro_real estimado

# -------------------------
# FUNÇÕES AUXILIARES
# -------------------------
def normalize_colname(c: str) -> str:
    """Normaliza nome de coluna: lower, sem acentos, espaços->underscore."""
    if c is None:
        return ""
    s = str(c).strip().lower()
    # substituir acentos básicos
    s = s.replace("á", "a").replace("à", "a").replace("ã", "a").replace("â", "a")
    s = s.replace("é", "e").replace("ê", "e")
    s = s.replace("í", "i")
    s = s.replace("ó", "o").replace("ô", "o").replace("õ", "o")
    s = s.replace("ú", "u")
    s = s.replace("ç", "c")
    # substituir espaços e caracteres não alfanuméricos por underscore
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s

def find_best_column(cols, *keywords):
    """Encontra a coluna mais provável dentre cols que contenha todas/alguns keywords (em ordem).
       Retorna None se não achar."""
    lowcols = [normalize_colname(c) for c in cols]
    # procura coluna que contenha todos os keywords
    for c_orig, c_norm in zip(cols, lowcols):
        if all(k in c_norm for k in keywords):
            return c_orig
    # fallback: coluna que contenha pelo menos um keyword, priorizando combos
    for c_orig, c_norm in zip(cols, lowcols):
        if any(k in c_norm for k in keywords):
            return c_orig
    return None

def parse_valor_brasileiro(v):
    """Converte strings como '1.234,56' ou '2970,00' ou numérico para float seguro."""
    if pd.isna(v):
        return 0.0
    # se já numérico
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return 0.0
    s = str(v).strip()
    if s == "":
        return 0.0
    # remover possível prefixo 'R$' e espaços
    s = s.replace("R$", "").replace(" ", "")
    # algumas fontes usam notação científica estranha, tentar lidar
    # primeiro, se parece ser número com ponto decimal padrão, tentar float direto
    # mas priorizar conversão de formato brasileiro:
    # remover pontos de milhar e trocar vírgula decimal por ponto
    # se houver mais de one comma or dot anomalies, simplificar
    # remove qualquer caractere que não dígito, '.' ou ',' ou '-' 
    s_clean = re.sub(r"[^\d\-,\.eE+]", "", s)
    # heurística: se tiver ',' e não tiver '.', tratar como BR
    if "," in s_clean and "." not in s_clean:
        s2 = s_clean.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            pass
    # se tiver pontos e vírgulas (ex: '2.970,00'), remover pontos de milhar
    if "." in s_clean and "," in s_clean:
        s2 = s_clean.replace(".", "").replace(",", ".")
        try:
            return float(s2)
        except Exception:
            pass
    # se apenas ponto (ex: '2970.00') tentar direto
    try:
        return float(s_clean)
    except Exception:
        pass
    # fallback: extrair dígitos e última ocorrência de separador
    digits = re.findall(r"[\d]+", s_clean)
    if not digits:
        return 0.0
    try:
        # tentar última parte como centavos
        joined = "".join(digits)
        return float(joined)
    except Exception:
        return 0.0

# -------------------------
# LÓGICA PRINCIPAL
# -------------------------
def process_from_postgres(engine, outdir, chunk_size=DEFAULT_CHUNK):
    os.makedirs(outdir, exist_ok=True)
    print("🔹 Lendo tabela 'notas' ...")
    # lê todas as notas (espera-se tabela razoável em tamanho para caber em memória)
    notas_df = pd.read_sql("SELECT * FROM notas", engine)
    if notas_df.empty:
        raise SystemExit("Tabela 'notas' está vazia ou não encontrada.")

    # normalizar nomes de colunas e mapear orig->normal
    notas_cols_map = {c: normalize_colname(c) for c in notas_df.columns}
    notas_df = notas_df.rename(columns={orig: notas_cols_map[orig] for orig in notas_df.columns})

    # detectar coluna chave (ex: 'chave_de_acesso' ou 'chave')
    chave_col = find_best_column(list(notas_df.columns), "chave", "acesso") or find_best_column(list(notas_df.columns), "chave") 
    if chave_col is None:
        raise SystemExit("Não encontrei coluna de chave de acesso nas notas.")
    notas_df = notas_df.rename(columns={chave_col: "chave_acesso"})

    # detectar coluna valor total nas notas (prioridade: 'valor_total', 'valor', 'valor_nota', 'total')
    valor_cols_try = ["valor_total", "valor", "valor_nota", "total"]
    valor_col = None
    for tryname in valor_cols_try:
        found = find_best_column(list(notas_df.columns), tryname)
        if found:
            valor_col = found
            break
    if valor_col is None:
        # pegar qualquer coluna contendo 'valor' como fallback
        valor_col = find_best_column(list(notas_df.columns), "valor")
    if valor_col is None:
        raise SystemExit("Não encontrei coluna de valor nas notas.")
    notas_df = notas_df.rename(columns={valor_col: "valor_total"})

    # converter valor_total para float seguro
    notas_df["valor_total"] = notas_df["valor_total"].apply(parse_valor_brasileiro)

    # index auxiliar por chave
    if "chave_acesso" not in notas_df.columns:
        raise SystemExit("Coluna 'chave_acesso' faltando após normalização das notas.")
    notas_index = notas_df.set_index("chave_acesso", drop=False)

    # acumuladores e pivôs
    accum = {
        "total_rows_itens": 0,
        "faturamento_bruto": 0.0,
        "icms_estimado": 0.0,
        "iss_estimado": 0.0,
        "pis_estimado": 0.0,
        "cofins_estimado": 0.0,
        "irpj_estimado": 0.0,
        "csll_estimado": 0.0
    }
    faturamento_por_mes = defaultdict(float)
    faturamento_por_uf_emit = defaultdict(float)
    faturamento_por_cfop = defaultdict(float)
    faturamento_por_ncm = defaultdict(float)

    print("🔹 Processando 'itens' em chunks ...")
    # leitura chunked dos itens
    query_itens = "SELECT * FROM itens"
    reader = pd.read_sql(query_itens, engine, chunksize=chunk_size)

    chunk_i = 0
    for chunk in reader:
        chunk_i += 1
        print(f"  → Chunk {chunk_i} ({len(chunk)} linhas)")

        # normalizar colunas do chunk
        cols_map = {c: normalize_colname(c) for c in chunk.columns}
        chunk = chunk.rename(columns=cols_map)

        # detectar chave em items
        item_chave = find_best_column(list(chunk.columns), "chave", "acesso") or find_best_column(list(chunk.columns), "chave")
        if item_chave:
            chunk = chunk.rename(columns={item_chave: "chave_acesso"})
        else:
            # se não tiver chave, não dá para relacionar — armazenar zeros e pular
            print("    ⚠️  chunk sem coluna de chave, pulando chunk...")
            continue

        # detectar coluna valor do item (valor_total do item ou equivalente)
        item_val_col = find_best_column(list(chunk.columns), "valor_total") or find_best_column(list(chunk.columns), "valor_item") or find_best_column(list(chunk.columns), "valor")
        if item_val_col:
            chunk = chunk.rename(columns={item_val_col: "valor_total_item"})
        else:
            # tentar valor unitario * quantidade
            if "valor_unitario" in chunk.columns and "quantidade" in chunk.columns:
                chunk["valor_total_item"] = chunk["valor_unitario"].apply(parse_valor_brasileiro) * chunk["quantidade"].apply(lambda x: float(re.sub(r"[^\d\-\.]", "", str(x)) or 0))
            else:
                chunk["valor_total_item"] = 0.0

        # converter valor_total_item
        chunk["valor_total_item"] = chunk["valor_total_item"].apply(parse_valor_brasileiro)


        # merge com notas_index para puxar UF, data, etc.
        # notas_index tem 'chave_acesso' como coluna e índice
        # usar left join via coluna
        if notas_index.index.name == "chave_acesso":
            notas_index = notas_index.reset_index(drop=True)

            chunk["chave_acesso"] = chunk["chave_acesso"].astype(str)
            notas_index.index = notas_index.index.astype(str)


            # fallback: notas_index index is chave_acesso -> join on index
            chunk = chunk.merge(notas_index, left_on="chave_acesso", right_index=True, how="left", suffixes=("", "_nota"))
        else:
            chunk = chunk.merge(notas_index, on="chave_acesso", how="left", suffixes=("", "_nota"))

        # converter/normalizar colunas adicionais se existirem
        # detectar coluna data_emissao em chunk/notas
        if "data_emissao" in chunk.columns:
            chunk["data_emissao_parsed"] = pd.to_datetime(chunk["data_emissao"], errors="coerce", dayfirst=True)
        elif "data_emissao_nota" in chunk.columns:
            chunk["data_emissao_parsed"] = pd.to_datetime(chunk["data_emissao_nota"], errors="coerce")
        else:
            chunk["data_emissao_parsed"] = pd.NaT

        # detectar UF emitente
        uf_emit_col = find_best_column(list(chunk.columns), "uf_emitente") or find_best_column(list(chunk.columns), "uf_emit")
        if uf_emit_col and uf_emit_col not in ["uf_emitente", "uf_emit"]:
            chunk = chunk.rename(columns={uf_emit_col: "uf_emitente"})
        # se ainda não existir, manter vazio
        if "uf_emitente" not in chunk.columns:
            chunk["uf_emitente"] = ""

        # detectar CFOP e NCM
        cfop_col = find_best_column(list(chunk.columns), "cfop")
        if cfop_col and cfop_col != "cfop":
            chunk = chunk.rename(columns={cfop_col: "cfop"})
        if "cfop" not in chunk.columns:
            chunk["cfop"] = ""

        ncm_col = find_best_column(list(chunk.columns), "ncm")
        if ncm_col and ncm_col != "ncm":
            chunk = chunk.rename(columns={ncm_col: "ncm"})
        if "ncm" not in chunk.columns:
            chunk["ncm"] = ""

        # agora iterar por linhas do chunk (poderia vectorizar, mas iter fica claro)
        # vectorized aggregation would be faster for very large data
        chunk_sum = chunk["valor_total_item"].sum()
        accum["total_rows_itens"] += len(chunk)
        accum["faturamento_bruto"] += chunk_sum

        # tributos estimados por chunk (simples aplicacao de aliquotas sobre item totals)
        accum["icms_estimado"] += chunk_sum * ALIQ_ICMS_DEFAULT
        accum["iss_estimado"] += chunk_sum * ALIQ_ISS_DEFAULT
        accum["pis_estimado"] += chunk_sum * ALIQ_PIS_CUM
        accum["cofins_estimado"] += chunk_sum * ALIQ_COFINS_CUM

        # pivôs
        # faturamento por mes
        if "data_emissao_parsed" in chunk.columns:
            chunk["mes_ano"] = chunk["data_emissao_parsed"].dt.to_period("M").astype(str)
        else:
            chunk["mes_ano"] = "unknown"
        for mes, s in chunk.groupby("mes_ano")["valor_total_item"].sum().items():
            faturamento_por_mes[mes] += float(s)

        # faturamento por uf emitente
        for uf, s in chunk.groupby("uf_emitente")["valor_total_item"].sum().items():
            faturamento_por_uf_emit[uf] += float(s)

        # faturamento por cfop & ncm
        for cfop, s in chunk.groupby("cfop")["valor_total_item"].sum().items():
            faturamento_por_cfop[cfop] += float(s)
        for ncm, s in chunk.groupby("ncm")["valor_total_item"].sum().items():
            faturamento_por_ncm[ncm] += float(s)

    # fim chunks

    # calcular IRPJ / CSLL (se ativado)
    if ESTIMAR_IRPJ_CSLL:
        if ENQUADRAMENTO == "lucro_presumido":
            base_presumida = accum["faturamento_bruto"] * PRESUNCAO
            accum["csll_estimado"] = base_presumida * ALIQUOTA_CSLL
            accum["irpj_estimado"] = base_presumida * ALIQUOTA_IRPJ
        elif ENQUADRAMENTO == "lucro_real":
            lucro_contabil = accum["faturamento_bruto"] * ESTIMATED_MARGIN
            accum["csll_estimado"] = lucro_contabil * ALIQUOTA_CSLL
            accum["irpj_estimado"] = lucro_contabil * ALIQUOTA_IRPJ
        else:
            accum["csll_estimado"] = 0.0
            accum["irpj_estimado"] = 0.0
    else:
        accum["csll_estimado"] = 0.0
        accum["irpj_estimado"] = 0.0

    # gerar outputs
    resumo = {
        "total_rows_itens": accum["total_rows_itens"],
        "faturamento_bruto": accum["faturamento_bruto"],
        "icms_estimado": accum["icms_estimado"],
        "iss_estimado": accum["iss_estimado"],
        "pis_estimado": accum["pis_estimado"],
        "cofins_estimado": accum["cofins_estimado"],
        "irpj_estimado": accum.get("irpj_estimado", 0.0),
        "csll_estimado": accum.get("csll_estimado", 0.0),
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    # salvar arquivos
    with open(os.path.join(outdir, "resumo_apuracao.json"), "w", encoding="utf-8") as f:
        json.dump(resumo, f, ensure_ascii=False, indent=2)

    pd.DataFrame([resumo]).to_csv(os.path.join(outdir, "resumo_apuracao.csv"), index=False)

    # pivôs
    pd.DataFrame(sorted(faturamento_por_mes.items()), columns=["mes_ano", "faturamento"]).to_csv(
        os.path.join(outdir, "faturamento_por_mes.csv"), index=False
    )
    pd.DataFrame(sorted(faturamento_por_uf_emit.items()), columns=["uf_emit", "faturamento"]).to_csv(
        os.path.join(outdir, "faturamento_por_uf_emitente.csv"), index=False
    )
    pd.DataFrame(sorted(faturamento_por_cfop.items()), columns=["cfop", "faturamento"]).to_csv(
        os.path.join(outdir, "faturamento_por_cfop.csv"), index=False
    )
    pd.DataFrame(sorted(faturamento_por_ncm.items()), columns=["ncm", "faturamento"]).to_csv(
        os.path.join(outdir, "faturamento_por_ncm.csv"), index=False
    )

    print("\n✅ Apuração finalizada — arquivos gravados em:", outdir)
    print(json.dumps(resumo, indent=2, ensure_ascii=False))

# -------------------------
# CLI
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apuração fiscal a partir de tabelas Postgres (notas, itens)")
    parser.add_argument("--host", required=True, help="Postgres host")
    parser.add_argument("--port", default="6060", help="Postgres port")
    parser.add_argument("--user", required=True, help="DB user")
    parser.add_argument("--password", required=True, help="DB password")
    parser.add_argument("--db", required=True, help="DB name")
    parser.add_argument("--out", default=OUT_DIR_DEFAULT, help="Diretório de saída")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK, help="Chunk size")
    args = parser.parse_args()

    # cria engine
    url = URL.create(
        "postgresql+psycopg2",
        username=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        database=args.db,
    )
    engine = create_engine(url)

    process_from_postgres(engine, args.out, chunk_size=args.chunk)
