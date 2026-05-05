<p align="center">
  <img src="https://img.shields.io/badge/NewsLens-Real--Time%20News%20Intelligence-blueviolet?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCI+PHBhdGggZD0iTTEyIDJMMyA3djEwbDkgNSA5LTV..." alt="NewsLens">
</p>

<h1 align="center">🔍 NewsLens</h1>
<h3 align="center">Real-Time AI-Powered News Intelligence Pipeline</h3>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Apache%20Flink-E6526F?style=flat-square&logo=apacheflink&logoColor=white" alt="Flink">
  <img src="https://img.shields.io/badge/Weaviate-00C29A?style=flat-square&logo=weaviate&logoColor=white" alt="Weaviate">
  <img src="https://img.shields.io/badge/Ollama-000000?style=flat-square&logo=ollama&logoColor=white" alt="Ollama">
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker">
  <img src="https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white" alt="Airflow">
</p>

<p align="center">
  <em>An end-to-end streaming pipeline that ingests news from multiple sources, enriches articles with AI (NLP + LLM), validates quality, stores in a vector database, and serves a real-time interactive dashboard with semantic search & RAG-powered Q&A.</em>
</p>

---

## 📐 Architecture — Animated Data Flow

<p align="center">
  <img src="assets/pipeline-flow.svg" alt="NewsLens Pipeline Animation" width="100%">
</p>

<p align="center"><em>↑ Watch the data flow through the pipeline in real-time (animated SVG — dots represent articles moving through stages)</em></p>

---

## 📐 Architecture — Static Diagram

```mermaid
flowchart LR
    %% ───────── SOURCES ─────────
    subgraph SOURCES["📡 Data Sources"]
        direction TB
        GU["🇬🇧 The Guardian API"]
        RSS["📰 RSS Feeds"]
        HN["🟠 HackerNews"]
        BS["🦋 Bluesky"]
    end

    %% ───────── INGESTION ─────────
    subgraph INGESTION["⚡ Ingestion Layer"]
        direction TB
        PROD["🐍 Producer\n(Python)"]
    end

    %% ───────── STREAMING ─────────
    subgraph STREAMING["📬 Event Streaming"]
        direction TB
        K1["raw-news\n(3 partitions)"]
        K2["tech-news"]
        K3["finance-news"]
        K4["world-news"]
        K5["enriched-news"]
        K6["validated-news"]
        KF["news-failed ❌"]
    end

    %% ───────── PROCESSING ─────────
    subgraph PROCESSING["⚡ Stream Processing"]
        direction TB
        PROC["Processor\n(Category Router)"]
    end

    %% ───────── ENRICHMENT ─────────
    subgraph ENRICHMENT["🧠 AI Enrichment"]
        direction TB
        EW["Enrichment Worker"]
        OL["🦙 Ollama / Mistral"]
    end

    %% ───────── QUALITY ─────────
    subgraph QUALITY["✅ Quality Gate"]
        direction TB
        QG["Quality Gate Worker"]
        GX["Great Expectations"]
        EMB["🔢 Embedding\n(all-MiniLM-L6-v2)"]
    end

    %% ───────── STORAGE ─────────
    subgraph STORAGE["💾 Vector Storage"]
        direction TB
        WV["Weaviate\n(HNSW + Cosine)"]
    end

    %% ───────── DASHBOARD ─────────
    subgraph DASHBOARD["📊 Dashboard"]
        direction TB
        ST["Streamlit App"]
        LF["Live Feed"]
        QA["Ask NewsLens\n(RAG Q&A)"]
        PM["Pipeline Metrics"]
    end

    %% ───────── OBSERVABILITY ─────────
    subgraph OBS["🔭 Observability"]
        direction TB
        AF["Airflow\n(Health DAGs)"]
        MQ["Marquez\n(Data Lineage)"]
        KUI["Kafka UI"]
        FL["Flink Dashboard"]
    end

    %% ───────── CONNECTIONS ─────────
    GU --> PROD
    RSS --> PROD
    HN --> PROD
    BS --> PROD

    PROD -->|"publish all"| K1

    K1 -->|"consume"| PROC
    PROC -->|"tech"| K2
    PROC -->|"finance"| K3
    PROC -->|"world"| K4

    K2 --> EW
    K3 --> EW
    K4 --> EW
    EW <-->|"LLM calls"| OL
    EW -->|"enriched"| K5
    EW -.->|"dead-letter"| KF

    K5 --> QG
    QG --> GX
    QG --> EMB
    QG -->|"validated +\nembedding"| K6
    QG -.->|"dead-letter"| KF

    K6 --> WV

    WV --> ST
    ST --> LF
    ST --> QA
    ST --> PM
    OL -.->|"RAG answers"| QA

    AF -.->|"monitors"| K1
    AF -.->|"monitors"| WV
    AF -.->|"monitors"| OL
    MQ -.->|"lineage"| K1

    %% ───────── STYLES ─────────
    classDef source fill:#e8f5e9,stroke:#4caf50,stroke-width:2px
    classDef kafka fill:#fff3e0,stroke:#ff9800,stroke-width:2px
    classDef ai fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px
    classDef quality fill:#e3f2fd,stroke:#2196f3,stroke-width:2px
    classDef storage fill:#e0f2f1,stroke:#009688,stroke-width:2px
    classDef dash fill:#fce4ec,stroke:#e91e63,stroke-width:2px
    classDef obs fill:#f5f5f5,stroke:#9e9e9e,stroke-width:1px,stroke-dasharray:5

    class SOURCES source
    class STREAMING kafka
    class ENRICHMENT ai
    class QUALITY quality
    class STORAGE storage
    class DASHBOARD dash
    class OBS obs
```

---

## 🚀 Pipeline Stages — Step by Step

<table>
<tr>
<td width="80" align="center">

**Stage**
</td>
<td width="200">

**Component**
</td>
<td>

**What Happens**
</td>
</tr>

<tr>
<td align="center">

### 1️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Producer-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Producer">
</td>
<td>

**Multi-Source Ingestion** — Polls The Guardian API, RSS feeds, HackerNews, and Bluesky every **15 minutes**. Deduplicates articles, routes to category-specific Kafka topics (`tech-news`, `finance-news`, `world-news`).
</td>
</tr>

<tr>
<td align="center">

### 2️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka">
</td>
<td>

**Event Streaming** — Apache Kafka (KRaft mode, no ZooKeeper) with 7 topics acts as the central nervous system. Decouples producers from consumers, enables replay, and provides backpressure handling.
</td>
</tr>

<tr>
<td align="center">

### 3️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Ollama-000000?style=for-the-badge&logo=ollama&logoColor=white" alt="Ollama">
</td>
<td>

**AI Enrichment** — Enrichment worker consumes raw articles, calls **Mistral** (via Ollama) to generate: summary, sentiment analysis (Positive/Negative/Neutral + reason), entity extraction, and domain classification (AI, Crypto, Regulation, Geopolitics, Earnings, Climate, Cybersecurity, Other).
</td>
</tr>

<tr>
<td align="center">

### 4️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Quality%20Gate-2196F3?style=for-the-badge&logo=greatexpectations&logoColor=white" alt="Quality Gate">
</td>
<td>

**Validation + Embedding** — Validates enriched articles with Great Expectations (recency check, schema, completeness). Generates **384-dim vectors** using `all-MiniLM-L6-v2` sentence-transformer. Passes valid articles to `validated-news`; dead-letters failures.
</td>
</tr>

<tr>
<td align="center">

### 5️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Weaviate-00C29A?style=for-the-badge&logo=weaviate&logoColor=white" alt="Weaviate">
</td>
<td>

**Vector Storage** — Ingests validated articles into Weaviate's `NewsArticle` collection with HNSW cosine index. Enables lightning-fast semantic search and aggregation queries for the dashboard.
</td>
</tr>

<tr>
<td align="center">

### 6️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit">
</td>
<td>

**Interactive Dashboard** — Three views: **Live Feed** (latest articles with filters), **Ask NewsLens** (vector search + RAG Q&A with streaming LLM responses), **Pipeline Metrics** (KPIs, sentiment/tag/section charts, health links).
</td>
</tr>

<tr>
<td align="center">

### 7️⃣
</td>
<td>

<img src="https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow">
<img src="https://img.shields.io/badge/Marquez-4B0082?style=for-the-badge" alt="Marquez">
</td>
<td>

**Observability** — Airflow runs health-check DAGs every 15 min (validates message flow, article counts, consumer groups, model availability). Marquez tracks OpenLineage metadata for full data lineage. Discord alerts on failure.
</td>
</tr>
</table>

---

## 🧬 Detailed Data Flow

```mermaid
sequenceDiagram
    participant S as 📡 Sources
    participant P as 🐍 Producer
    participant K as 📬 Kafka
    participant R as ⚡ Processor
    participant E as 🧠 Enrichment
    participant LLM as 🦙 Ollama
    participant Q as ✅ Quality Gate
    participant W as 💾 Weaviate
    participant D as 📊 Dashboard
    participant U as 👤 User

    S->>P: Fetch articles (Guardian, RSS, HN, Bluesky)
    P->>P: Deduplicate & normalize
    P->>K: Publish to raw-news (all articles)
    
    K->>R: Consume from raw-news
    R->>K: Route → tech-news / finance-news / world-news

    K->>E: Consume from category topics
    E->>LLM: Prompt for summary, sentiment, entities, tag
    LLM-->>E: JSON response
    E->>E: Validate & normalize tags
    E->>K: Publish to enriched-news

    K->>Q: Consume from enriched-news
    Q->>Q: Great Expectations validation
    Q->>Q: Generate embedding (384-dim)
    Q->>K: Publish to validated-news

    K->>W: Consume from validated-news
    W->>W: HNSW index insertion

    U->>D: Open dashboard
    D->>W: Query articles / vector search
    W-->>D: Results
    D->>LLM: RAG prompt with context (streaming)
    LLM-->>D: Token-by-token response
    D-->>U: Live feed / AI answer / metrics
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Ingestion** | ![Python](https://img.shields.io/badge/-Python%203.11-3776AB?style=flat-square&logo=python&logoColor=white) | Multi-source producer with deduplication |
| **Streaming** | ![Kafka](https://img.shields.io/badge/-Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white) | Event backbone (KRaft, 7 topics) |
| **Processing** | ![Flink](https://img.shields.io/badge/-Apache%20Flink-E6526F?style=flat-square&logo=apacheflink&logoColor=white) | Stream processing framework |
| **LLM** | ![Ollama](https://img.shields.io/badge/-Ollama%20+%20Mistral-000000?style=flat-square&logo=ollama&logoColor=white) | Article enrichment & RAG answers |
| **Embeddings** | ![HuggingFace](https://img.shields.io/badge/-sentence--transformers-FFD21E?style=flat-square&logo=huggingface&logoColor=black) | all-MiniLM-L6-v2 (384 dims) |
| **Validation** | ![GX](https://img.shields.io/badge/-Great%20Expectations-FF6F00?style=flat-square) | Data quality checks |
| **Vector DB** | ![Weaviate](https://img.shields.io/badge/-Weaviate%201.28-00C29A?style=flat-square&logo=weaviate&logoColor=white) | HNSW cosine similarity search |
| **Dashboard** | ![Streamlit](https://img.shields.io/badge/-Streamlit%201.45-FF4B4B?style=flat-square&logo=streamlit&logoColor=white) | Interactive UI (Live Feed, Q&A, Metrics) |
| **Orchestration** | ![Airflow](https://img.shields.io/badge/-Airflow%202.8-017CEE?style=flat-square&logo=apacheairflow&logoColor=white) | Health checks, scheduling, alerts |
| **Lineage** | ![Marquez](https://img.shields.io/badge/-Marquez%200.44-4B0082?style=flat-square) | OpenLineage metadata tracking |
| **Monitoring** | ![Kafka UI](https://img.shields.io/badge/-Kafbat%20UI-231F20?style=flat-square&logo=apachekafka&logoColor=white) | Topic inspection & consumer lag |
| **Infra** | ![Docker](https://img.shields.io/badge/-Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white) | 16 containerized services |

---

## 📂 Project Structure

```
newslens/
├── producer/                # 📡 Multi-source news fetcher (Guardian, RSS, HN, Bluesky)
├── enrichment/              # 🧠 LLM enrichment worker (Ollama/Mistral)
├── quality_gate/            # ✅ Great Expectations validation + embedding
├── weaviate_store/          # 💾 Weaviate ingestion consumer
├── dashboard/               # 📊 Streamlit app (Live Feed, Ask NewsLens, Metrics)
│   ├── app.py              #     Entry point & navigation
│   ├── views/              #     Page views (live_feed, ask_newslens, metrics)
│   ├── config.py           #     Environment-based configuration
│   ├── weaviate_helper.py  #     Weaviate query utilities
│   ├── Dockerfile          #     Optimized image (2.6 GB, CPU-only PyTorch)
│   └── tests/              #     Unit tests
├── airflow/
│   └── dags/               # 🔄 health_check DAG (message flow + consumer groups)
├── processor/               # ⚡ Flink stream processor
├── tests/                   # 🧪 Integration tests
├── docker-compose.yml       # 🐳 Full stack (16 services)
├── .env                     # 🔐 Configuration (poll interval, keys)
└── README.md                # 📖 You are here
```

---

## 🚦 Quick Start

```bash
# Clone
git clone https://github.com/intez20/newslens.git
cd newslens

# Configure
cp .env.example .env
# Edit .env with your Guardian API key, Discord webhook, etc.

# Launch all 16 services
docker compose up -d

# Pull Mistral model into Ollama
docker compose exec ollama ollama pull mistral

# Access the services
# 📊 Dashboard:       http://localhost:8501
# 📬 Kafka UI:        http://localhost:8080
# ⚡ Flink:           http://localhost:8081
# 🔄 Airflow:         http://localhost:8082
# 🔍 Marquez Lineage: http://localhost:3000
# 💾 Weaviate:        http://localhost:8085
```

---

## 📈 Pipeline Metrics at a Glance

| Metric | Value |
|--------|-------|
| **Sources** | 4 (Guardian, RSS, HackerNews, Bluesky) |
| **Poll Interval** | 15 minutes |
| **Kafka Topics** | 7 |
| **Embedding Dimensions** | 384 (all-MiniLM-L6-v2) |
| **Vector Index** | HNSW with cosine distance |
| **LLM** | Mistral 7B (via Ollama) |
| **Container Count** | 16 services |
| **Dashboard Image** | 2.6 GB (CPU-only PyTorch) |

---

## 🔐 Fault Tolerance

| Scenario | Handling |
|----------|----------|
| LLM returns invalid tag | Normalized to `"Other"` (no dead-letter) |
| Article older than 7 days | Dead-lettered by Quality Gate |
| Service down | Airflow detects in ≤15 min → Discord alert |
| Consumer crash | Kafka consumer group rebalances automatically |
| Weaviate unreachable | Ingestion retries with exponential backoff |

---

<p align="center">
  <img src="https://img.shields.io/badge/Built%20with-❤️%20and%20Kafka-231F20?style=for-the-badge" alt="Built with Kafka">
</p>
