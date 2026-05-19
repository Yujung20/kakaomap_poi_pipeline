"""
카카오맵 POI 데이터 수집 → 전처리 → MySQL 적재 DAG
스케줄: 매일 새벽 2시 자동 실행
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# ── 설정 ──────────────────────────────────────────────────────────────────
KAKAO_API_KEY = os.getenv("KAKAO_API_KEY")

DB_CONFIG = {
    "host":     "host.docker.internal",
    "user":     "root",
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "port":     3306,
    "database": "kakao_poi",
}

KAKAO_URL  = "https://dapi.kakao.com/v2/local/search/keyword.json"
TABLE_NAME = "poi_places"


# ── Task 1: 카카오 API 호출 ────────────────────────────────────────────────
def fetch_poi(**context):
    headers  = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    keywords = ["카페", "음식점", "병원", "약국"]
    region   = "서울"
    all_docs = []

    for keyword in keywords:
        params = {"query": f"{region} {keyword}", "size": 15}
        resp   = requests.get(KAKAO_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
        all_docs.extend(docs)
        print(f"[{keyword}] {len(docs)}건 수집")

    print(f"총 {len(all_docs)}건 수집 완료")
    context["ti"].xcom_push(key="raw_documents", value=all_docs)


# ── Task 2: DataFrame 변환 ─────────────────────────────────────────────────
def preprocess(**context):
    raw_docs = context["ti"].xcom_pull(key="raw_documents", task_ids="fetch_poi")

    df = pd.DataFrame(raw_docs)
    df = df[[
        "id", "place_name", "category_group_name", "category_name",
        "phone", "road_address_name", "address_name", "x", "y", "place_url"
    ]].rename(columns={
        "id":                  "place_id",
        "place_name":          "place_name",
        "category_group_name": "category",
        "category_name":       "category_detail",
        "phone":               "phone",
        "road_address_name":   "road_address",
        "address_name":        "address",
        "x":                   "longitude",
        "y":                   "latitude",
        "place_url":           "place_url",
    })

    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df = df.drop_duplicates(subset=["place_id"])
    print(f"전처리 완료: {len(df)}건")

    context["ti"].xcom_push(key="clean_records", value=df.to_dict("records"))


# ── Task 3: MySQL 적재 ─────────────────────────────────────────────────────
def load_to_mysql(**context):
    records = context["ti"].xcom_pull(key="clean_records", task_ids="preprocess")

    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
            id              BIGINT AUTO_INCREMENT PRIMARY KEY,
            place_id        VARCHAR(50)   NOT NULL UNIQUE,
            place_name      VARCHAR(200)  NOT NULL,
            category        VARCHAR(100),
            category_detail VARCHAR(300),
            phone           VARCHAR(50),
            road_address    VARCHAR(300),
            address         VARCHAR(300),
            latitude        DECIMAL(10,7),
            longitude       DECIMAL(10,7),
            place_url       VARCHAR(500),
            created_at      DATETIME DEFAULT NOW(),
            updated_at      DATETIME DEFAULT NOW() ON UPDATE NOW()
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    sql = f"""
        INSERT INTO `{TABLE_NAME}`
            (place_id, place_name, category, category_detail,
             phone, road_address, address, latitude, longitude, place_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            place_name      = VALUES(place_name),
            category        = VALUES(category),
            category_detail = VALUES(category_detail),
            phone           = VALUES(phone),
            road_address    = VALUES(road_address),
            address         = VALUES(address),
            latitude        = VALUES(latitude),
            longitude       = VALUES(longitude),
            place_url       = VALUES(place_url),
            updated_at      = NOW()
    """

    rows = [(
        r["place_id"], r["place_name"], r["category"], r["category_detail"],
        r["phone"], r["road_address"], r["address"],
        r["latitude"], r["longitude"], r["place_url"]
    ) for r in records]

    cursor.executemany(sql, rows)
    conn.commit()
    print(f"MySQL 적재 완료: {len(rows)}건")

    cursor.close()
    conn.close()


# ── DAG 정의 ───────────────────────────────────────────────────────────────
default_args = {
    "owner":           "data-team",
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "start_date":      datetime(2024, 1, 1),
}

with DAG(
    dag_id="kakaomap_poi_pipeline",
    default_args=default_args,
    description="카카오맵 POI 수집 → 전처리 → MySQL 적재",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["kakaomap", "poi", "mysql"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_poi",
        python_callable=fetch_poi,
    )

    t2 = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess,
    )

    t3 = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql,
    )

    t1 >> t2 >> t3