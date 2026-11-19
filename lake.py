"""Общие настройки: подключение к RustFS, условие варианта, нормализация навыков."""
import os
import re

import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9100")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "rustfsadmin")

BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
PARQUET_KEY = "parquet/it_vacancies_2024.parquet"

# Вариант: программист 1С.
# Роль из справочника hh + упоминание 1С в названии вакансии
# (кириллическая или латинская «С», в любом регистре, допускается пробел: «1 С»).
ROLE = "Программист, разработчик"
NAME_RE = re.compile(r"1 ?[сc]")


def s3():
    return boto3.client(
        "s3", endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
    )


def is_variant(role, name):
    return role == ROLE and NAME_RE.search(name.lower()) is not None


_SPACES = re.compile(r"[ \t\u00a0]+")
_ONE_C = re.compile(r"1 ?[сc]")
_COLON = re.compile(r"1С:(?=[^ ])")


def norm_skill(s):
    """«1C:ERP», «1с: ERP» и «1С:  ERP» сводятся к одному виду «1С: erp»."""
    s = _SPACES.sub(" ", s.lower()).strip(" ")
    s = _ONE_C.sub("1С", s)
    return _COLON.sub("1С: ", s)


def skill_set(key_skills):
    """Уникальные нормализованные навыки вакансии в отсортированном виде."""
    if not key_skills:
        return []
    return sorted({t for t in (norm_skill(x) for x in key_skills.split(";")) if t})
