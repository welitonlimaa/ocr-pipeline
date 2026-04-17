"""
Storage layer: AWS S3 via boto3.
Gerencia upload/download de PDFs, resultados de OCR e chunks extraídos.
"""

import json
from app.config.settings import settings
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


class StorageClient:
    def __init__(self):
        kwargs = dict(
            aws_access_key_id=settings.S3_access_key,
            aws_secret_access_key=settings.S3_secret_key,
            region_name=settings.S3_region,
            config=Config(signature_version="s3v4"),
        )

        if settings.S3_endpoint:
            scheme = "https" if settings.S3_secure else "http"
            kwargs["endpoint_url"] = f"{scheme}://{settings.S3_endpoint}"

        self.client = boto3.client("s3", **kwargs)
        self.bucket = settings.S3_bucket
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Garante que o bucket existe (cria se necessário, respeitando região)."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                create_kwargs = {"Bucket": self.bucket}
                # us-east-1 não aceita LocationConstraint
                if settings.S3_region and settings.S3_region != "us-east-1":
                    create_kwargs["CreateBucketConfiguration"] = {
                        "LocationConstraint": settings.S3_region
                    }
                self.client.create_bucket(**create_kwargs)
            else:
                raise RuntimeError(f"Erro ao verificar bucket S3: {e}")

    def upload_bytes(
        self,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Faz upload de bytes para o S3 e retorna o caminho no bucket."""
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=data,
            ContentType=content_type,
        )
        return f"s3://{self.bucket}/{object_key}"

    def upload_json(self, object_key: str, payload: dict) -> str:
        """Serializa dict para JSON e faz upload."""
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        return self.upload_bytes(object_key, raw, content_type="application/json")

    def upload_text(self, object_key: str, text: str) -> str:
        """Faz upload de texto puro (markdown, etc.)."""
        raw = text.encode("utf-8")
        return self.upload_bytes(
            object_key, raw, content_type="text/plain; charset=utf-8"
        )

    def upload_file(self, object_key: str, file_path: str) -> str:
        """Faz upload de arquivo local usando multipart automático do boto3."""
        self.client.upload_file(
            Filename=file_path,
            Bucket=self.bucket,
            Key=object_key,
        )
        return f"s3://{self.bucket}/{object_key}"

    def download_bytes(self, object_key: str) -> bytes:
        """Baixa objeto e retorna como bytes."""
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        return response["Body"].read()

    def download_json(self, object_key: str) -> dict:
        """Baixa e desserializa JSON."""
        raw = self.download_bytes(object_key)
        return json.loads(raw.decode("utf-8"))

    def download_text(self, object_key: str) -> str:
        """Baixa e retorna texto."""
        return self.download_bytes(object_key).decode("utf-8")

    def get_presigned_url(self, object_key: str, expires_hours: int = 24) -> str:
        """Gera URL pré-assinada para acesso temporário."""
        url = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": object_key},
            ExpiresIn=expires_hours * 3600,
        )
        return url

    def object_exists(self, object_key: str) -> bool:
        """Verifica se objeto existe no bucket."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError:
            return False

    def list_objects(self, prefix: str) -> list[str]:
        """Lista objetos com determinado prefixo (suporta paginação automática)."""
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys


storage = StorageClient()
