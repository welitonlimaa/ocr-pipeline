import pdfplumber
from io import BytesIO


def get_pdf_page_count(file_bytes: bytes) -> int:
    with pdfplumber.open(BytesIO(file_bytes)) as pdf:
        return len(pdf.pages)
