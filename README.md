# KAKAOMAP_POI_PIPELINE

카카오맵 API 기반 POI 데이터 수집 및 자동화 파이프라인 프로젝트<br>
**(ENG) Kakao Map API-based POI Data Collection and Automation Pipeline Project**

---

## 🛠 Skills

![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=mysql&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat-square&logo=pandas&logoColor=white)
![KakaoMap API](https://img.shields.io/badge/KakaoMap_API-FFCD00?style=flat-square&logo=kakao&logoColor=black)

---

## 📌 Project Summary

카카오 로컬 API를 활용하여 POI(장소) 데이터를 수집하고,<br>
Pandas 기반 전처리 후 MySQL에 적재하는 파이프라인을 구성한 프로젝트입니다.<br>

Apache Airflow와 Docker를 활용하여<br>
데이터 수집부터 적재까지의 과정을 자동화했습니다.

**Eng ver.**

This project collects POI (Point of Interest) data using the Kakao Local API,
preprocesses it with Pandas, and loads it into MySQL.

The entire pipeline from data collection to storage is automated using Apache Airflow and Docker.

---

## 🎯 Purpose

- 카카오 REST API 활용 데이터 수집 경험
- Pandas 기반 데이터 전처리 및 재구조화 실습
- MySQL 데이터 적재 파이프라인 구성
- Apache Airflow DAG 기반 파이프라인 자동화 경험
- Docker 기반 Airflow 환경 구성 경험

**Eng ver.**

- Gain experience collecting data using Kakao REST API
- Practice data preprocessing and restructuring with Pandas
- Build a MySQL data loading pipeline
- Automate pipelines using Apache Airflow DAGs
- Configure Airflow environment using Docker

---

## 💡 Why I Built This

데이터 엔지니어링의 핵심인 데이터 수집, 전처리, 적재 흐름을 직접 경험해보고 싶어 진행한 프로젝트입니다.<br>

단순 데이터 처리뿐 아니라,
외부 API 연동, 데이터 파이프라인 설계,
그리고 Airflow를 활용한 자동화 환경 구성을 경험하는 데 목적이 있었습니다.<br>

또한 Docker 기반 환경 구성을 통해
실제 운영 환경과 유사한 파이프라인 구조를 직접 구현하고자 했습니다.

**Eng ver.**

I built this project to gain hands-on experience with the core data engineering workflow: collection, preprocessing, and storage.

Beyond basic data processing, I wanted to practice external API integration, data pipeline design, and automation using Airflow.

I also aimed to implement a pipeline structure similar to real production environments using Docker.

---

## 📂 Project Structure

```text
API_airflow/
├── dags/
│   ├── kakao_poi_dag.py
│   └── kakao_pharmacy_dag.py
├── logs/
├── plugins/
├── docker-compose.yaml
├── env.example
└── .gitignore
```

---

## ✨ Main Features

### 카카오 로컬 API 데이터 수집

- 카카오 REST API 키 기반 인증
- 키워드 기반 POI 데이터 수집 (카페, 음식점, 병원, 약국)
- 페이지네이션 처리

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Authentication using Kakao REST API key
- Keyword-based POI data collection (cafes, restaurants, hospitals, pharmacies)
- Pagination handling

### 서울 전체 약국 데이터 수집 (행정동 단위)

- 서울 전체 약국 수 확인 결과 총 5,150개
- 카카오 API 정책상 1회 호출당 최대 15건, 최대 3페이지(45건)까지 수집 가능
- 서울 424개 행정동 단위로 순회하여 최대 19,080건 수집 시도
- 중복 제거 후 최종 4,437건 적재 완료 (목표 대비 약 86% 수집률)
- 미수집 약 700건 원인 분석: 행정동 주소 오표기 문제 확인<br>
  (도로명 주소는 오표기 없이 정상 수집되나, 행정동 주소 기반 검색 시 일부 누락 발생)
- Upsert 방식으로 매주 월요일 새벽 3시 자동 갱신

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Confirmed total of 5,150 pharmacies in Seoul
- Kakao API allows maximum 15 results per call, up to 3 pages (45 results) per query
- Collected data by iterating through 424 administrative districts (dong) in Seoul, attempting up to 19,080 records
- Final 4,437 records loaded after deduplication (approximately 86% collection rate)
- Analyzed ~700 missing records: identified incorrect administrative district name encoding as root cause<br>
  (Road name addresses were collected without errors, while administrative district-based searches caused partial omissions)
- Automatically updated every Monday at 3 AM using Upsert strategy

### Pandas 기반 데이터 전처리

- 필요한 컬럼 선택 및 재구조화
- 좌표 데이터 타입 변환 (string → float)
- 중복 데이터 제거

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Column selection and data restructuring
- Coordinate data type conversion (string → float)
- Duplicate data removal

### MySQL 데이터 적재

- DB 및 테이블 자동 생성
- INSERT ON DUPLICATE KEY UPDATE 기반 Upsert 처리
- 배치 단위 데이터 적재

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Automatic DB and table creation
- Upsert handling using INSERT ON DUPLICATE KEY UPDATE
- Batch data loading

### Airflow DAG 기반 자동화

- PythonOperator 기반 Task 구성
- XCom 기반 Task 간 데이터 전달
- kakao_poi_pipeline: 매일 새벽 2시 자동 실행
- kakao_pharmacy_pipeline: 매주 월요일 새벽 3시 자동 실행

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Task configuration using PythonOperator
- Inter-task data passing using XCom
- kakao_poi_pipeline: Scheduled automatic execution at 2 AM daily
- kakao_pharmacy_pipeline: Scheduled automatic execution at 3 AM every Monday

### Docker 기반 Airflow 환경 구성

- docker-compose 기반 Airflow 환경 구성
- PostgreSQL 메타데이터 DB 연동
- 로컬 MySQL과의 연동 (host.docker.internal)

&nbsp;&nbsp;&nbsp;**Eng ver.**

- Airflow environment configuration using docker-compose
- PostgreSQL metadata DB integration
- Local MySQL integration via host.docker.internal

---

## ▶️ How to Run

```bash
# 1. 환경변수 설정
cp env.example .env
# .env 파일에 KAKAO_API_KEY, MYSQL_PASSWORD 입력

# 2. Airflow 초기화 (최초 1회)
docker-compose up airflow-init

# 3. Airflow 실행
docker-compose up airflow-scheduler airflow-webserver

# 4. Airflow UI 접속
# http://localhost:8080
# ID: admin / PW: admin
```

---

## 📖 What I Learned

- 카카오 REST API 연동 및 데이터 수집 흐름 이해
- API 호출 제한 정책을 고려한 수집 전략 설계 경험
- 행정동 단위 데이터 수집 시 주소 오표기로 인한 누락 문제 분석 경험
- Pandas 기반 데이터 전처리 및 재구조화 경험
- MySQL 파이프라인 설계 및 Upsert 처리 경험
- Apache Airflow DAG 구조 및 Task 의존성 이해
- XCom 기반 Task 간 데이터 전달 방식 이해
- Docker 기반 서비스 환경 구성 경험

**Eng ver.**

- Understanding Kakao REST API integration and data collection workflows
- Experience designing data collection strategies considering API rate limits
- Experience analyzing data omission caused by incorrect administrative district name encoding
- Experience with Pandas-based data preprocessing and restructuring
- Experience designing MySQL pipelines and handling Upsert operations
- Understanding Apache Airflow DAG structure and task dependencies
- Understanding inter-task data passing using XCom
- Experience configuring service environments using Docker