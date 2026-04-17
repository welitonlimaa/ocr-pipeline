import zipfile
import tempfile
from pathlib import Path


def create_zip_from_keys(job_id: str, storage, keys: list[str]):
    prefix = f"jobs/{job_id}/"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        local_files = []

        for key in keys:
            try:
                if not key or not key.startswith(prefix):
                    continue

                if key.endswith(".zip"):
                    continue

                relative_path = key[len(prefix) :]

                if not relative_path or relative_path.endswith("/"):
                    continue

                local_file = tmp_path / relative_path
                local_file.parent.mkdir(parents=True, exist_ok=True)

                data = storage.download_bytes(key)

                with open(local_file, "wb") as f:
                    f.write(data)

                local_files.append((key, local_file))

            except Exception as e:
                print(f"Erro ao processar {key}: {e}")
                continue

        if not local_files:
            raise Exception("Nenhum arquivo válido para gerar ZIP")

        zip_path = tmp_path / f"{job_id}.zip"

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for key, local_file in local_files:
                try:
                    arcname = key[len(prefix) :]
                    zipf.write(local_file, arcname)
                except Exception:
                    continue

        zip_key = f"jobs/{job_id}/output.zip"
        zip_url = storage.upload_file(zip_key, str(zip_path))

    return zip_key, zip_url
