
from google.cloud import storage
from ingestion.config import PROJECT_ID, BUCKET

def _bucket():
    return storage.Client(project=PROJECT_ID).bucket(BUCKET)

def exists(path: str) -> bool:
    return _bucket().blob(path).exists()

def upload_file(local_path: str, path: str) -> None:
    _bucket().blob(path).upload_from_filename(local_path)