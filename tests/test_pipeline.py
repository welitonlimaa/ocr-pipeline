"""
Teste de integração local do pipeline OCR.
Simula o fluxo completo sem Docker: S3 local + Redis + pipeline direto.

Uso:
    python tests/test_pipeline.py
"""

import io
import json
import sys
import time
import threading
import tempfile
from pathlib import Path

# Adiciona raiz do projeto ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

from app.config.settings import settings
from app.scripts.storage import storage
from app.scripts.job_state import registry, JobStatus
from app.scripts.extractor import extract_chunk, compute_chunks, get_pdf_page_count


def create_test_pdf(num_pages: int = 15) -> bytes:
    """Gera PDF sintético com texto, tabelas simuladas e dados variados."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    sections = [
        (
            "Introdução",
            "Este documento apresenta o relatório anual de operações da empresa.",
        ),
        (
            "Resultados Financeiros",
            "Receita total: R$ 12.450.000 | Lucro líquido: R$ 2.100.000",
        ),
        (
            "Análise de Mercado",
            "Crescimento de 18% no segmento B2B. Expansão para 3 novos estados.",
        ),
        (
            "Recursos Humanos",
            "Quadro atual: 847 colaboradores. Turnover: 6,2%. NPS interno: 72.",
        ),
        (
            "Projetos Estratégicos",
            "Migração cloud concluída. IA embarcada em produção desde Q2.",
        ),
        (
            "Riscos e Mitigações",
            "Risco cambial hedgeado. Compliance SOC2 Type II obtido.",
        ),
        ("Plano 2025", "Meta de receita: R$ 18M. Novos mercados: Argentina e Chile."),
    ]

    for page_num in range(1, num_pages + 1):
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, height - 60, f"Relatório Corporativo — Página {page_num}")

        c.setFont("Helvetica", 10)
        c.drawString(50, height - 85, f"Documento confidencial | Exercício 2024")

        # Conteúdo rotativo por seção
        section_title, section_body = sections[page_num % len(sections)]
        c.setFont("Helvetica-Bold", 13)
        c.drawString(50, height - 130, section_title)

        c.setFont("Helvetica", 11)
        y = height - 160
        words = section_body.split()
        line = ""
        for word in words:
            if len(line + " " + word) > 80:
                c.drawString(50, y, line.strip())
                y -= 18
                line = word
            else:
                line += " " + word
        if line:
            c.drawString(50, y, line.strip())
            y -= 18

        # Adiciona dados tabulares simulados nas páginas pares
        if page_num % 2 == 0:
            y -= 20
            c.setFont("Helvetica-Bold", 11)
            c.drawString(50, y, "Indicadores do Período:")
            y -= 20
            c.setFont("Courier", 10)
            rows = [
                ("Produto", "Vendas", "Meta", "% Atingido"),
                ("Produto A", "R$ 4.200.000", "R$ 4.000.000", "105%"),
                ("Produto B", "R$ 3.100.000", "R$ 3.500.000", "88,5%"),
                ("Produto C", "R$ 5.150.000", "R$ 4.950.000", "104%"),
            ]
            col_widths = [120, 110, 110, 90]
            for row in rows:
                x = 50
                for i, cell in enumerate(row):
                    c.drawString(x, y, cell)
                    x += col_widths[i]
                y -= 16

        # Rodapé
        c.setFont("Helvetica", 8)
        c.drawString(50, 30, f"Página {page_num} de {num_pages} | Confidencial © 2024")

        c.showPage()

    c.save()
    buf.seek(0)
    return buf.read()


# Pipeline local (sem Celery)


def run_pipeline_local(job_id: str, pdf_key: str):
    """Executa o pipeline completo localmente para testes."""
    from app.scripts.extractor import extract_chunk, compute_chunks, get_pdf_page_count

    state = registry.get(job_id)
    state.set_status(JobStatus.PROCESSING, "Baixando PDF...")

    pdf_bytes = storage.download_bytes(pdf_key)
    total_pages = get_pdf_page_count(pdf_bytes)
    chunks = compute_chunks(total_pages)

    state.set(total_pages=total_pages, total_chunks=len(chunks))
    state.set_status(JobStatus.EXTRACTING, f"Extraindo {total_pages} páginas...")

    chunk_results = []
    for idx, (start, end) in enumerate(chunks):
        result = extract_chunk(pdf_bytes, idx, start, end)

        md_key = f"jobs/{job_id}/chunks/chunk_{idx:04d}.md"
        storage.upload_text(md_key, result.markdown_combined)

        structured = {
            "job_id": job_id,
            "chunk_index": idx,
            "start_page": result.start_page,
            "end_page": result.end_page,
            "pages_count": len(result.pages),
            "tables_count": len(result.tables_combined),
            "tokens_estimate": result.summary_tokens_estimate,
            "tables": result.tables_combined,
            "pages_summary": [
                {
                    "page_num": p.page_num,
                    "word_count": p.word_count,
                    "has_tables": p.has_tables,
                }
                for p in result.pages
            ],
        }
        json_key = f"jobs/{job_id}/chunks/chunk_{idx:04d}.json"
        storage.upload_json(json_key, structured)

        chunk_summary = {
            "chunk_index": idx,
            "start_page": result.start_page,
            "end_page": result.end_page,
            "markdown_key": md_key,
            "json_key": json_key,
            "tokens_estimate": result.summary_tokens_estimate,
            "tables_count": len(result.tables_combined),
            "status": "done",
        }
        state.add_chunk_result(idx, chunk_summary)
        state.set_progress(result.end_page, total_pages)
        chunk_results.append(chunk_summary)

        print(
            f"  ✓ Chunk {idx+1}/{len(chunks)}: páginas {result.start_page}–{result.end_page} "
            f"({result.summary_tokens_estimate} tokens est.)"
        )

    # Finaliza
    state.set_status(JobStatus.INDEXING, "Consolidando índice...")
    total_tokens = sum(c.get("tokens_estimate", 0) for c in chunk_results)
    total_tables = sum(c.get("tables_count", 0) for c in chunk_results)

    index = {
        "job_id": job_id,
        "total_pages": total_pages,
        "total_chunks": len(chunks),
        "chunks_processed": len(chunk_results),
        "chunks_failed": 0,
        "total_tokens_estimate": total_tokens,
        "total_tables_extracted": total_tables,
        "chunks": chunk_results,
        "llm_context": {
            "format": "markdown",
            "chunk_keys": [c["markdown_key"] for c in chunk_results],
            "structured_keys": [c["json_key"] for c in chunk_results],
            "recommended_chunk_size_tokens": 8000,
            "total_estimated_tokens": total_tokens,
        },
    }
    index_key = f"jobs/{job_id}/index.json"
    storage.upload_json(index_key, index)

    state.set(
        status=JobStatus.COMPLETED.value,
        message=f"Concluído: {len(chunk_results)} chunks",
        progress_pct=100,
        progress_pages=total_pages,
        outputs={
            "index_key": index_key,
            "total_chunks": len(chunks),
            "chunks_ok": len(chunk_results),
            "total_tokens_estimate": total_tokens,
            "total_tables": total_tables,
        },
    )
    return index


# Teste principal


def test_full_pipeline():
    print("\n" + "=" * 60)
    print("  OCR PIPELINE — TESTE DE INTEGRAÇÃO LOCAL")
    print("=" * 60)

    # Gera PDF de teste
    print("\n[1/6] Gerando PDF de teste (15 páginas)...")
    try:
        pdf_bytes = create_test_pdf(num_pages=15)
        print(f"      PDF gerado: {len(pdf_bytes)/1024:.1f} KB")
    except ImportError:
        print("      reportlab não instalado — usando PDF mínimo")
        # PDF mínimo válido
        pdf_bytes = b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\nxref\n0 4\n0000000000 65535 f\n0000000009 00000 n\n0000000058 00000 n\n0000000115 00000 n\ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n190\n%%EOF"

    # Upload para S3
    print("\n[2/6] Fazendo upload para S3...")
    test_pdf_key = "test/pipeline-test.pdf"
    try:
        storage.upload_bytes(test_pdf_key, pdf_bytes, "application/pdf")
        print(f"      Armazenado em: S3://{settings.S3_BUCKET}/{test_pdf_key}")
    except Exception as e:
        print(f"      ERRO: {e}")
        print("      Certifique-se que o S3 está rodando: docker-compose up S3")
        return

    # Cria job no Redis
    print("\n[3/6] Criando job no Redis...")
    try:
        state = registry.create(
            filename="pipeline-test.pdf",
            metadata={"source": "test", "pages": 15},
        )
        job_data = state.get()
        job_id = job_data["job_id"]
        state.set(pdf_key=test_pdf_key)
        print(f"      Job ID: {job_id}")
        print(f"      Status: {job_data['status']}")
    except Exception as e:
        print(f"      ERRO Redis: {e}")
        print("      Certifique-se que o Redis está rodando: docker-compose up redis")
        return

    # Executa pipeline
    print("\n[4/6] Executando pipeline OCR...")
    t0 = time.time()
    try:
        index = run_pipeline_local(job_id, test_pdf_key)
        elapsed = round(time.time() - t0, 2)
        print(f"\n      Pipeline concluído em {elapsed}s")
    except Exception as e:
        print(f"      ERRO no pipeline: {e}")
        import traceback

        traceback.print_exc()
        return

    # Verifica outputs
    print("\n[5/6] Verificando outputs no S3...")
    objects = storage.list_objects(f"jobs/{job_id}/")
    print(f"      Objetos criados: {len(objects)}")
    for obj in sorted(objects):
        print(f"        • {obj}")

    # Exibe resultado final (JSON de resposta)
    print("\n[6/6] Resposta JSON do pipeline:")
    final_state = registry.get(job_id).get()
    response = {
        "job_id": job_id,
        "status": final_state["status"],
        "outputs": final_state["outputs"],
        "llm_context": index["llm_context"],
        "sample_chunk_0": {
            "key": index["chunks"][0]["markdown_key"],
            "pages": f"{index['chunks'][0]['start_page']}–{index['chunks'][0]['end_page']}",
            "tokens_estimate": index["chunks"][0]["tokens_estimate"],
        },
    }
    print(json.dumps(response, indent=2, ensure_ascii=False))

    # Mostra amostra do markdown do primeiro chunk
    print("\n" + "─" * 60)
    print("AMOSTRA — Chunk 0 (primeiras 500 chars do markdown):")
    print("─" * 60)
    md_sample = storage.download_text(index["chunks"][0]["markdown_key"])
    print(md_sample[:500])
    print("\n[OK] Teste concluído com sucesso!")


if __name__ == "__main__":
    test_full_pipeline()
