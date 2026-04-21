# 📦 DeepContext Output — Guia Prático

Este projeto gera um pacote (`output.zip`) contendo todo o conteúdo processado de um PDF já estruturado para uso em aplicações modernas, como:

* RAG (Retrieval-Augmented Generation)
* Agentes com IA
* Análise de documentos
* Extração de dados estruturados

👉 **Objetivo deste guia:** mostrar como você pode usar esse output na prática, mesmo que não trabalhe com IA no dia a dia.

---

# Estrutura do Output

Após extrair o arquivo `output.zip`, você terá:

```
output/
 ├── index.json
 ├── condensed.md
 ├── condensed.json
 └── chunks/
     ├── chunk_0000.md
     ├── chunk_0000.json
     ├── chunk_0001.md
     └── ...
```

### O que cada arquivo representa:

* `index.json` → mapa completo do documento
* `condensed.md` → resumo global inteligente
* `chunks/*.md` → partes do documento (texto)
* `chunks/*.json` → dados estruturados (tabelas, métricas, etc.)

---

# 1. RAG básico (busca em documentos)

Você pode implementar uma busca simples nos chunks:

```python
import json
from pathlib import Path

BASE_PATH = Path("output")

def load_index():
    return json.loads((BASE_PATH / "index.json").read_text())

def load_text(relative_path):
    return (BASE_PATH / relative_path).read_text()

def retrieve_chunks(query, index, top_k=3):
    results = []

    for key in index["llm_context"]["chunk_keys"]:
        relative = key.replace(f"jobs/{index['job_id']}/", "")
        text = load_text(relative)

        if query.lower() in text.lower():
            results.append(text)

    return results[:top_k]
```

👉 Isso já permite construir um sistema de perguntas e respostas simples.

---

# 2. RAG híbrido (resumo + detalhes)

Aqui está o diferencial do pipeline.

Você combina:

* visão global (`condensed.md`)
* detalhes (`chunks`)

```python
def hybrid_context(query, index):
    condensed_path = index["condensed_context"]["md_key"]
    condensed = load_text(condensed_path.replace(f"jobs/{index['job_id']}/", ""))

    chunks = []
    for key in index["llm_context"]["chunk_keys"]:
        relative = key.replace(f"jobs/{index['job_id']}/", "")
        text = load_text(relative)

        if query.lower() in text.lower():
            chunks.append(text)

    return f"""
Resumo global:
{condensed}

Detalhes relevantes:
{"\n\n".join(chunks[:3])}
"""
```

👉 Isso evita perder contexto geral ou focar só em partes isoladas.

---

# 3. Integração com LangChain

Transforme os chunks em documentos:

```python
from langchain.schema import Document

def load_documents(index):
    docs = []

    for key in index["llm_context"]["chunk_keys"]:
        relative = key.replace(f"jobs/{index['job_id']}/", "")
        text = load_text(relative)

        docs.append(
            Document(
                page_content=text,
                metadata={"source": relative}
            )
        )

    return docs
```

👉 Pronto para usar com embeddings, retrievers e chains.

---

# 4. Pipeline com LangGraph

Separando etapas de raciocínio:

```python
def node_global_context(state):
    condensed = load_text("condensed.md")
    return {"global_context": condensed}

def node_retrieve_chunks(state):
    query = state["query"]
    index = state["index"]

    chunks = []

    for key in index["llm_context"]["chunk_keys"]:
        relative = key.replace(f"jobs/{index['job_id']}/", "")
        text = load_text(relative)

        if query.lower() in text.lower():
            chunks.append(text)

    return {"chunks": chunks[:3]}
```

👉 Fluxo típico:

```
Pergunta → Resumo global → Busca por chunks → Resposta
```

---

# 5. Extração de dados estruturados

Você também pode acessar tabelas e dados:

```python
def load_json(relative_path):
    return json.loads((BASE_PATH / relative_path).read_text())

def extract_tables(index):
    tables = []

    for key in index["llm_context"]["structured_keys"]:
        relative = key.replace(f"jobs/{index['job_id']}/", "")
        data = load_json(relative)

        tables.extend(data.get("tables", []))

    return tables
```

👉 Ideal para:

* dashboards
* análises
* relatórios automatizados

---

# 6. Casos reais

## 🔹 Identificar temas principais

```python
index = load_index()
condensed = load_text("condensed.md")

prompt = f"""
Baseado no resumo abaixo, identifique os principais temas:

{condensed}
"""
```

---

## 🔹 Aprofundar em um tema

```python
index = load_index()

context = hybrid_context("cultura organizacional", index)

prompt = f"""
{context}

Explique de forma detalhada.
"""
```

---


# 💡 Por que isso é poderoso?

Você não está recebendo apenas um arquivo processado.

Você está recebendo um:

## 🧠 Pacote de Conhecimento Portátil

Que permite:

✅ uso offline
✅ integração com qualquer stack
✅ zero dependência de API
✅ base pronta para IA
✅ reutilização em múltiplos contextos

---

# Conclusão

Com esse output você pode:

* construir um chatbot sobre documentos
* criar um sistema de busca inteligente
* extrair insights automaticamente
* alimentar agentes e automações
* Testar POCs

👉 Tudo isso sem precisar montar pipeline complexo.

---

Se você já sabe trabalhar com arquivos JSON e texto…
você já consegue usar isso com IA.

Sem mágica. Sem vendor lock-in.
