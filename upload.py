"""Создаёт бакет hh-lake и кладёт в него CSV с вакансиями.

python upload.py data/it_vacancies_2024.csv
"""
import hashlib
import sys
import time

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

import lake


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main(path):
    c = lake.s3()
    try:
        c.head_bucket(Bucket=lake.BUCKET)
        print(f"бакет {lake.BUCKET} уже есть")
    except ClientError:
        c.create_bucket(Bucket=lake.BUCKET)
        print(f"создан бакет {lake.BUCKET}")

    t = time.perf_counter()
    c.upload_file(path, lake.BUCKET, lake.RAW_KEY,
                  Config=TransferConfig(multipart_chunksize=64 << 20))
    print(f"загружен s3://{lake.BUCKET}/{lake.RAW_KEY} за {time.perf_counter() - t:.1f} с")

    head = c.head_object(Bucket=lake.BUCKET, Key=lake.RAW_KEY)
    print(f"размер в хранилище: {head['ContentLength']} байт, ETag {head['ETag']}")

    # контроль целостности: md5 локального файла и скачанного обратно объекта
    h = hashlib.md5()
    for block in c.get_object(Bucket=lake.BUCKET, Key=lake.RAW_KEY)["Body"].iter_chunks(8 << 20):
        h.update(block)
    print(f"md5 локального файла: {md5_file(path)}")
    print(f"md5 объекта в S3:     {h.hexdigest()}")


if __name__ == "__main__":
    main(sys.argv[1])
