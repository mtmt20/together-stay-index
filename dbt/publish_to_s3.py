"""dbt run이 끝난 뒤 결과 .duckdb 파일을 S3에 올린다.

로컬 Docker에서 Streamlit(tsi-streamlit)은 같은 볼륨(./warehouse)을 마운트해
파일을 바로 읽으므로 이 스크립트가 필요 없지만, Streamlit Community Cloud는
로컬 파일을 마운트할 수 없어 S3에서 최신 스냅샷을 내려받아야 한다. dbt run
직후 이 스크립트로 항상 최신 상태를 S3에 올려둔다.
"""

from __future__ import annotations

import os

import boto3

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/data/together_stay.duckdb")
S3_BUCKET = os.environ["AWS_S3_BUCKET"]
S3_KEY = "gold-warehouse/together_stay.duckdb"


def main() -> None:
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-northeast-2"))
    s3.upload_file(DUCKDB_PATH, S3_BUCKET, S3_KEY)
    print(f"업로드 완료: s3://{S3_BUCKET}/{S3_KEY}")


if __name__ == "__main__":
    main()
