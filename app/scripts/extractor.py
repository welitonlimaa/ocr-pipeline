import io
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pdfplumber
from markitdown import MarkItDown

from app.config.settings import settings
from app.config.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class PageResult:
    page_num: int
    raw_text: str
    markdown: str
    tables: list[dict]
    word_count: int
    has_tables: bool
    has_images: bool


@dataclass
class ChunkResult:
    chunk_index: int
    start_page: int
    end_page: int
    pages: list[PageResult]
    markdown_combined: str
    tables_combined: list[dict]
    summary_tokens_estimate: int


@dataclass
class DocumentMeta:
    filename: str
    total_pages: int
    total_chunks: int
    chunk_size: int
    language_hint: str = "pt"
    has_tables: bool = False
    has_images: bool = False
    word_count_total: int = 0


def _extract_tables_from_page(page: pdfplumber.page.Page) -> list[dict]:
    """Extrai tabelas de uma página e converte para dicionário estruturado."""
    tables = []
    try:
        raw_tables = page.extract_tables()
    except Exception as exc:
        logger.warning(
            "Falha ao extrair tabelas de página",
            extra={
                "action": "table_extraction_failed",
                "page_num": getattr(page, "page_number", "unknown"),
                "error": str(exc),
            },
        )
        return []

    for idx, table in enumerate(raw_tables):
        if not table or len(table) < 2:
            continue
        header = [str(h or "").strip() for h in table[0]]
        rows = []
        for row in table[1:]:
            cleaned = [str(c or "").strip() for c in row]
            if any(cleaned):
                rows.append(dict(zip(header, cleaned)))
        tables.append(
            {
                "table_index": idx,
                "headers": header,
                "rows": rows,
                "row_count": len(rows),
            }
        )
    return tables


def _table_to_markdown(table: dict) -> str:
    """Converte tabela estruturada em markdown."""
    headers = table["headers"]
    if not headers:
        return ""
    sep = "|".join(["---"] * len(headers))
    header_row = "| " + " | ".join(headers) + " |"
    sep_row = "| " + sep + " |"
    lines = [header_row, sep_row]
    for row in table["rows"]:
        cells = [str(row.get(h, "")) for h in headers]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _clean_text(text: str) -> str:
    """Remove artefatos comuns de extração PDF."""
    if not text:
        return ""

    text = re.sub(r"-\n(\w)", r"\1", text)
    text = re.sub(r" {2,}", " ", text)
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue

        if len(re.findall(r"[a-zA-ZÀ-ÿ]", stripped)) >= 3:
            lines.append(stripped)
    return "\n".join(lines).strip()


def _page_to_markdown(page_num: int, text: str, tables: list[dict]) -> str:
    """Monta markdown estruturado de uma página com texto e tabelas."""
    sections = [f"## Página {page_num}\n"]

    clean = _clean_text(text)
    if clean:
        sections.append(clean)

    for table in tables:
        md_table = _table_to_markdown(table)
        if md_table:
            sections.append(f"\n**Tabela {table['table_index'] + 1}:**\n{md_table}")

    return "\n\n".join(sections)


def extract_chunk(
    pdf_bytes: bytes,
    chunk_index: int,
    start_page: int,
    end_page: int,
) -> ChunkResult:
    """
    Extrai texto e tabelas de um intervalo de páginas do PDF.
    Usa pdfplumber para extração granular e markitdown como fallback/normalização.
    """
    ctx = {
        "chunk_index": chunk_index,
        "start_page": start_page + 1,
        "end_page": end_page + 1,
    }

    page_results: list[PageResult] = []
    all_tables: list[dict] = []
    pages_with_no_text = 0

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_idx in range(start_page, min(end_page + 1, len(pdf.pages))):
                page = pdf.pages[page_idx]
                page_num = page_idx + 1

                try:
                    raw_text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                except Exception as exc:
                    logger.warning(
                        "Falha ao extrair texto de página individual",
                        extra={
                            **ctx,
                            "action": "page_text_extraction_failed",
                            "page_num": page_num,
                            "error": str(exc),
                        },
                    )
                    raw_text = ""

                if not raw_text.strip():
                    pages_with_no_text += 1

                tables = _extract_tables_from_page(page)
                all_tables.extend(tables)

                has_images = len(page.images) > 0
                markdown = _page_to_markdown(page_num, raw_text, tables)
                word_count = len(raw_text.split())

                page_results.append(
                    PageResult(
                        page_num=page_num,
                        raw_text=raw_text,
                        markdown=markdown,
                        tables=tables,
                        word_count=word_count,
                        has_tables=len(tables) > 0,
                        has_images=has_images,
                    )
                )

    except Exception as exc:
        logger.error(
            "Falha ao abrir ou percorrer PDF com pdfplumber",
            extra={
                **ctx,
                "action": "pdfplumber_open_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise

    if pages_with_no_text > 0:
        logger.debug(
            "Páginas sem texto detectadas no chunk",
            extra={
                **ctx,
                "action": "pages_no_text",
                "pages_with_no_text": pages_with_no_text,
                "total_pages_in_chunk": len(page_results),
            },
        )

    # Fallback para markitdown se nenhuma página retornou texto
    if all(not p.raw_text.strip() for p in page_results):
        logger.warning(
            "Nenhuma página retornou texto via pdfplumber — ativando fallback markitdown",
            extra={
                **ctx,
                "action": "markitdown_fallback_triggered",
                "pages_in_chunk": len(page_results),
            },
        )
        t_fallback = time.time()
        page_results = _fallback_markitdown(
            pdf_bytes, chunk_index, start_page, end_page
        )
        logger.info(
            "Fallback markitdown concluído",
            extra={
                **ctx,
                "action": "markitdown_fallback_done",
                "elapsed_seconds": round(time.time() - t_fallback, 2),
                "pages_recovered": len(page_results),
            },
        )

    markdown_combined = _build_chunk_markdown(chunk_index, page_results)
    total_words = sum(p.word_count for p in page_results)
    token_estimate = int(total_words * 0.75)

    return ChunkResult(
        chunk_index=chunk_index,
        start_page=start_page + 1,
        end_page=min(end_page + 1, start_page + len(page_results)),
        pages=page_results,
        markdown_combined=markdown_combined,
        tables_combined=all_tables,
        summary_tokens_estimate=token_estimate,
    )


def _fallback_markitdown(
    pdf_bytes: bytes,
    chunk_index: int,
    start_page: int,
    end_page: int,
) -> list[PageResult]:
    """Fallback via markitdown para PDFs que não têm texto extraível."""
    md_converter = MarkItDown()
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        result = md_converter.convert(tmp_path)
        text = result.text_content or ""
        pages = text.split("\f")
        page_results = []
        for i, page_text in enumerate(pages[start_page : end_page + 1]):
            page_num = start_page + i + 1
            cleaned = _clean_text(page_text)
            page_results.append(
                PageResult(
                    page_num=page_num,
                    raw_text=cleaned,
                    markdown=f"## Página {page_num}\n\n{cleaned}",
                    tables=[],
                    word_count=len(cleaned.split()),
                    has_tables=False,
                    has_images=False,
                )
            )
        return page_results
    except Exception as exc:
        logger.error(
            "Falha no fallback markitdown",
            extra={
                "action": "markitdown_fallback_failed",
                "chunk_index": chunk_index,
                "start_page": start_page + 1,
                "end_page": end_page + 1,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise
    finally:
        if tmp_path:
            Path(tmp_path).unlink(missing_ok=True)


def _build_chunk_markdown(chunk_index: int, pages: list[PageResult]) -> str:
    """Constrói markdown consolidado do chunk otimizado para contexto LLM."""
    header = f"# Chunk {chunk_index + 1} | Páginas {pages[0].page_num}–{pages[-1].page_num}\n"
    sections = [header]
    for page in pages:
        if page.markdown.strip():
            sections.append(page.markdown)
    return "\n\n---\n\n".join(sections)


def compute_chunks(total_pages: int, chunk_size: int = None) -> list[tuple[int, int]]:
    """
    Retorna lista de (start_page, end_page) 0-indexed para cada chunk.
    chunk_size padrão vem das configurações.
    """
    size = chunk_size or settings.CHUNK_SIZE_PAGES
    chunks = []
    for start in range(0, total_pages, size):
        end = min(start + size - 1, total_pages - 1)
        chunks.append((start, end))
    return chunks


def get_pdf_page_count(pdf_bytes: bytes) -> int:
    """Retorna número de páginas do PDF."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            count = len(pdf.pages)
        return count
    except Exception as exc:
        logger.error(
            "Falha ao obter contagem de páginas do PDF",
            extra={
                "action": "page_count_failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise
