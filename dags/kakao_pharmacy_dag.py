"""
서울 전체 약국 POI 데이터 수집 → 전처리 → MySQL 적재 DAG
행정동 단위로 순회하여 최대한 많은 약국 데이터 수집
스케줄: 매주 월요일 새벽 3시 자동 실행
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import mysql.connector
import os
import time
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
TABLE_NAME = "pharmacy_places"

# 서울 424개 행정동 목록
SEOUL_DONGS = [
    "가락동","가리봉동","가산동","가양동","가회동","갈현동","강일동","개봉동","개포동","거여동",
    "고덕동","고척동","공덕동","공릉동","관악동","관철동","광장동","교남동","구기동","구로동",
    "구수동","구의동","구파발동","군자동","궁동","금천동","길동","길음동","난곡동","난향동",
    "남가좌동","남현동","내발산동","노량진동","노원동","녹번동","녹산동","논현동","능동","답십리동",
    "당산동","대림동","대방동","대치동","대흥동","도곡동","도림동","도봉동","도화동","독산동",
    "독서당동","돈암동","동선동","동소문동","동작동","두모포동","둔촌동","등촌동","마곡동","마포동",
    "망원동","망우동","면목동","명동","명일동","목동","묵동","문래동","문정동","미아동",
    "반포동","방배동","방학동","방화동","배봉동","백련산동","번동","벽제동","보문동","봉천동",
    "부암동","불광동","사당동","삼각동","삼성동","삼전동","상계동","상도동","상봉동","상수동",
    "상암동","상일동","상천동","서교동","서빙고동","서초동","석관동","석촌동","성내동","성산동",
    "성수동","세곡동","세화동","소공동","송파동","수색동","수서동","수유동","숭인동","신대방동",
    "신내동","신당동","신도림동","신림동","신반포동","신사동","신설동","신수동","신영동","신원동",
    "신월동","신정동","신촌동","신화동","쌍문동","아현동","안암동","압구정동","양재동","양천동",
    "양평동","역삼동","연건동","연남동","연희동","염리동","영등포동","오금동","오류동","오목교동",
    "오산동","옥수동","온수동","와우산동","왕십리동","외발산동","용강동","용답동","용두동","용문동",
    "용산동","우면동","우이동","운니동","원서동","원지동","월계동","위례동","유등천동","은평동",
    "응암동","의주로동","이문동","이화동","인사동","인현동","일원동","임정동","자양동","잠실동",
    "잠원동","장위동","장지동","장충동","전농동","정릉동","정자동","종로동","종암동","주교동",
    "주성동","중계동","중곡동","중구동","중랑동","중림동","중화동","증산동","지봉동","지양동",
    "진관동","창2동","창동","창신동","천왕동","철산동","청담동","청운동","청진동","청파동",
    "초안산동","충신동","충정로동","충현동","태평로동","통의동","통인동","팔판동","평창동","풍납동",
    "하계동","하월곡동","한강로동","한남동","항동","행당동","행운동","헌릉동","혜화동","홍은동",
    "홍제동","화곡동","화양동","황학동","회기동","회현동","후암동","휘경동","흑석동",
]


# ── Task 1: 카카오 API 호출 ────────────────────────────────────────────────
def fetch_pharmacy(**context):
    headers  = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
    all_docs = []
    seen_ids = set()

    for dong in SEOUL_DONGS:
        for page in range(1, 4):   # 1~3페이지
            params = {
                "query": f"서울 {dong} 약국",
                "size":  15,
                "page":  page,
            }
            try:
                resp = requests.get(KAKAO_URL, headers=headers, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                docs = data.get("documents", [])

                for doc in docs:
                    if doc["id"] not in seen_ids:
                        seen_ids.add(doc["id"])
                        all_docs.append(doc)

                # 마지막 페이지면 조기 종료
                if data.get("meta", {}).get("is_end"):
                    break

                time.sleep(0.1)   # API 호출 제한 방지

            except Exception as e:
                print(f"[{dong}] page {page} 오류: {e}")
                continue

        print(f"[{dong}] 누적 수집: {len(all_docs)}건")

    print(f"총 {len(all_docs)}건 수집 완료")
    context["ti"].xcom_push(key="raw_documents", value=all_docs)


# ── Task 2: DataFrame 변환 ─────────────────────────────────────────────────
def preprocess(**context):
    raw_docs = context["ti"].xcom_pull(key="raw_documents", task_ids="fetch_pharmacy")

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
    "owner":       "data-team",
    "retries":     1,
    "retry_delay": timedelta(minutes=5),
    "start_date":  datetime(2024, 1, 1),
}

with DAG(
    dag_id="kakao_pharmacy_pipeline",
    default_args=default_args,
    description="서울 전체 약국 POI 수집 → 전처리 → MySQL 적재",
    schedule_interval="0 3 * * 1",  # 매주 월요일 새벽 3시
    catchup=False,
    tags=["kakaomap", "pharmacy", "mysql"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_pharmacy",
        python_callable=fetch_pharmacy,
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