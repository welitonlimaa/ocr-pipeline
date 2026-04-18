import json
import time
from app.config.settings import settings
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from app.config.logging_config import get_logger


logger = get_logger(__name__)


class StorageClient:
    def __init__(self):
        kwargs = dict(
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION,
            config=Config(signature_version="s3v4"),
        )

        if settings.S3_ENDPOINT:
            scheme = "https" if settings.S3_SECURE else "http"
            kwargs["endpoint_url"] = f"{scheme}://{settings.S3_ENDPOINT}"

        self.client = boto3.client("s3", **kwargs)
        self.bucket = settings.S3_BUCKET
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Garante que o bucket existe (cria se necessário, respeitando região)."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            logger.info(
                "Bucket S3 verificado",
                extra={"action": "bucket_verified", "bucket": self.bucket},
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                logger.warning(
                    "Bucket não encontrado — criando",
                    extra={
                        "action": "bucket_creating",
                        "bucket": self.bucket,
                        "region": settings.S3_REGION,
                    },
                )
                create_kwargs = {"Bucket": self.bucket}
                if settings.S3_REGION and settings.S3_REGION != "us-east-1":
                    create_kwargs["CreateBucketConfiguration"] = {
                        "LocationConstraint": settings.S3_REGION
                    }
                self.client.create_bucket(**create_kwargs)
                logger.info(
                    "Bucket criado com sucesso",
                    extra={"action": "bucket_created", "bucket": self.bucket},
                )
            else:
                logger.critical(
                    "Erro ao verificar bucket S3 — serviço não pode iniciar",
                    extra={
                        "action": "bucket_check_failed",
                        "bucket": self.bucket,
                        "error_code": code,
                        "error": str(e),
                    },
                )
                raise RuntimeError(f"Erro ao verificar bucket S3: {e}")

    def upload_bytes(
        self,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Faz upload de bytes para o S3 e retorna o caminho no bucket."""
        t0 = time.time()
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=data,
                ContentType=content_type,
            )
            elapsed = round(time.time() - t0, 3)
            logger.debug(
                "Upload S3 concluído",
                extra={
                    "action": "s3_upload_ok",
                    "key": object_key,
                    "size_bytes": len(data),
                    "elapsed_seconds": elapsed,
                },
            )
            return f"s3://{self.bucket}/{object_key}"
        except ClientError as exc:
            logger.error(
                "Falha no upload para o S3",
                extra={
                    "action": "s3_upload_failed",
                    "key": object_key,
                    "size_bytes": len(data),
                    "error_code": exc.response["Error"]["Code"],
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

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
        t0 = time.time()
        try:
            self.client.upload_file(
                Filename=file_path,
                Bucket=self.bucket,
                Key=object_key,
            )
            logger.debug(
                "Upload de arquivo local concluído",
                extra={
                    "action": "s3_upload_file_ok",
                    "key": object_key,
                    "file_path": file_path,
                    "elapsed_seconds": round(time.time() - t0, 3),
                },
            )
            return f"s3://{self.bucket}/{object_key}"
        except ClientError as exc:
            logger.error(
                "Falha no upload de arquivo local para S3",
                extra={
                    "action": "s3_upload_file_failed",
                    "key": object_key,
                    "file_path": file_path,
                    "error_code": exc.response["Error"]["Code"],
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def download_bytes(self, object_key: str) -> bytes:
        """Baixa objeto e retorna como bytes."""
        t0 = time.time()
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=object_key)
            data = response["Body"].read()
            logger.debug(
                "Download S3 concluído",
                extra={
                    "action": "s3_download_ok",
                    "key": object_key,
                    "size_bytes": len(data),
                    "elapsed_seconds": round(time.time() - t0, 3),
                },
            )
            return data
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            level = logger.warning if error_code == "NoSuchKey" else logger.error
            level(
                "Falha no download do S3",
                extra={
                    "action": "s3_download_failed",
                    "key": object_key,
                    "error_code": error_code,
                    "error": str(exc),
                },
                exc_info=(error_code != "NoSuchKey"),
            )
            raise

    def download_json(self, object_key: str) -> dict:
        """Baixa e desserializa JSON."""
        raw = self.download_bytes(object_key)
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error(
                "Falha ao desserializar JSON baixado do S3",
                extra={
                    "action": "s3_json_parse_failed",
                    "key": object_key,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def download_text(self, object_key: str) -> str:
        """Baixa e retorna texto."""
        return self.download_bytes(object_key).decode("utf-8")

    def get_presigned_url(self, object_key: str, expires_hours: int = 24) -> str:
        """Gera URL pré-assinada para acesso temporário."""
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_key},
                ExpiresIn=expires_hours * 3600,
            )
            return url
        except ClientError as exc:
            logger.error(
                "Falha ao gerar URL pré-assinada",
                extra={
                    "action": "presigned_url_failed",
                    "key": object_key,
                    "expires_hours": expires_hours,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def object_exists(self, object_key: str) -> bool:
        """Verifica se objeto existe no bucket."""
        try:
            self.client.head_object(Bucket=self.bucket, Key=object_key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False

            logger.error(
                "Erro inesperado ao verificar existência de objeto S3",
                extra={
                    "action": "s3_head_failed",
                    "key": object_key,
                    "error_code": exc.response["Error"]["Code"],
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise

    def list_objects(self, prefix: str) -> list[str]:
        """Lista objetos com determinado prefixo (suporta paginação automática)."""
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            logger.debug(
                "Listagem S3 concluída",
                extra={
                    "action": "s3_list_ok",
                    "prefix": prefix,
                    "object_count": len(keys),
                },
            )
        except ClientError as exc:
            logger.error(
                "Falha ao listar objetos no S3",
                extra={
                    "action": "s3_list_failed",
                    "prefix": prefix,
                    "error": str(exc),
                },
                exc_info=True,
            )
            raise
        return keys


storage = StorageClient()
