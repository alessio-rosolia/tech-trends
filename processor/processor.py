import os
import json
import time
import re
from datetime import datetime
import requests
import clickhouse_connect
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-tech-stream")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://host.docker.internal:1234/v1/chat/completions")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen2.5-7b")

# REGOLE DI NORMALIZZAZIONE E FILTRAGGIO ENTITÀ

BLACKLIST = {
    "arxiv", "reddit", "hackernews", "bluesky", "github", "twitter", "x",
    "software", "programming language", "code", "programming", "developer",
    "computer science", "ai", "artificial intelligence", "tech", "web",
    "llm", "llms", "large language models", "large language model",
    "transformer", "transformers", "reinforcement learning", "machine learning",
    "deep learning", "rag", "retrieval-augmented generation", "retrieval augmented generation",
    "lora", "qlora", "boolean logic", "baidu", "alibaba", "tencent", "bytedance", "anthropic", "flock", 
    "perovskite", "paramount"
}

# Normalizzazione di framework, librerie, linguaggi e tool
SYNONYM_MAP = {
    "postgres": "PostgreSQL",
    "postgresql": "PostgreSQL",
    "k8s": "Kubernetes",
    "kubernetes": "Kubernetes",
    "js": "JavaScript",
    "javascript": "JavaScript",
    "ts": "TypeScript",
    "typescript": "TypeScript",
    "py": "Python",
    "python": "Python",
    "golang": "Go",
    "go": "Go",
    "docker container": "Docker",
    "docker": "Docker",
    "react.js": "React",
    "reactjs": "React",
    "vue.js": "Vue",
    "vuejs": "Vue",
    "node": "Node.js",
    "nodejs": "Node.js",
    "vllm": "vLLM"
}

def clean_and_normalize_entity(entity: str) -> str:
    raw = entity.strip()
    norm_key = raw.lower()

    if norm_key in BLACKLIST or len(raw) < 2:
        return ""

    if norm_key in SYNONYM_MAP:
        return SYNONYM_MAP[norm_key]

    return raw

# INIZIALIZZAZIONE INFRASTRUTTURA
def init_clickhouse():
    print(f"[Processor] Connessione a ClickHouse ({CLICKHOUSE_HOST}:{CLICKHOUSE_PORT})...")
    client = None
    for attempt in range(15):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username='default',
                password=''
            )
            client.command("""
            CREATE TABLE IF NOT EXISTS tech_mentions (
                source LowCardinality(String),
                author String,
                text String,
                url String,
                entity LowCardinality(String),
                timestamp DateTime
            ) ENGINE = MergeTree()
            ORDER BY (timestamp, entity, source);
            """)
            print("[Processor] Tabella 'tech_mentions' verificata/creata con successo.")
            return client
        except Exception as e:
            print(f"[Processor] ClickHouse non pronto (tentativo {attempt+1}/15): {e}")
            time.sleep(3)
    raise RuntimeError("Impossibile connettersi a ClickHouse.")

def init_kafka():
    print(f"[Processor] Connessione al broker Kafka ({KAFKA_BROKER})...")
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='tech-processor-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print(f"[Processor] Iscritto al topic: {KAFKA_TOPIC}")
            return consumer
        except Exception as e:
            print(f"[Processor] Kafka non pronto (tentativo {attempt+1}/15): {e}")
            time.sleep(3)
    raise RuntimeError("Impossibile connettersi a Kafka.")


# ESTRAZIONE ENTITÀ TRAMITE LM STUDIO
def extract_entities(text: str) -> list:
    if not text or len(text.strip()) < 5:
        return []

    prompt = (
        "Sei un estrattore esperto di tool software. Leggi il testo ed estrai ESCLUSIVAMENTE "
        "i nomi propri di prodotti software reali, librerie di codice, linguaggi di programmazione, "
        "database, framework o tool infrastrutturali (es. 'PyTorch', 'Docker', 'PostgreSQL', 'LangChain', 'Rust', 'vLLM'). "
        "NON estrarre concetti generici, acronimi astratti o metodologie matematico/algoritmiche "
        "(IGNORA SEMPRE: LLM, LLMs, AI, Machine Learning, Deep Learning, RAG, LoRA, Transformer, Transformers). "
        "Restituisci RIGOROSAMENTE SOLO un array JSON di stringhe (es. [\"PyTorch\", \"FastAPI\"]). "
        "Se non trovi tool software reali, restituisci rigorosamente []. Nessun commento o testo extra."
    )

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": text}
        ],
        "temperature": 0.0
    }

    try:
        response = requests.post(LM_STUDIO_URL, json=payload, timeout=20)
        if response.status_code == 200:
            try:
                res_data = response.json()
            except Exception:
                return []
                
            content = res_data.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
            match = re.search(r'\[(.*?)\]', content, re.DOTALL)
            if match:
                parsed = json.loads("[" + match.group(1) + "]")
                return [item.strip() for item in parsed if isinstance(item, str) and item.strip()]
    except Exception as e:
        print(f"[Processor] Errore chiamata LM Studio: {e}")
    return []

# PIPELINE RUNNER
def main():
    ch_client = init_clickhouse()
    consumer = init_kafka()

    print("[Processor] In ascolto su Kafka ed elaborazione attiva...")

    for message in consumer:
        record = message.value
        text = record.get("text", "")
        source = record.get("source", "unknown")
        author = record.get("author", "unknown")
        url = record.get("url", "")
        raw_ts = record.get("timestamp", int(time.time()))

        if not text:
            continue

        print(f"\n[Processor] Elaboro ({source}): {text[:50]}...")
        entities = extract_entities(text)
        print(f"[Processor] Entità grezze: {entities}")

        if entities:
            try:
                dt_obj = datetime.fromtimestamp(int(raw_ts))
            except Exception:
                dt_obj = datetime.now()

            rows = []
            for ent in entities:
                norm_ent = clean_and_normalize_entity(ent)
                if not norm_ent:
                    continue

                rows.append([
                    str(source),
                    str(author),
                    str(text),
                    str(url),
                    str(norm_ent),
                    dt_obj
                ])

            if rows:
                try:
                    ch_client.insert(
                        'tech_mentions',
                        rows,
                        column_names=['source', 'author', 'text', 'url', 'entity', 'timestamp']
                    )
                    print(f"[Processor] Salvate {len(rows)} righe software concrete su ClickHouse.")
                except Exception as e:
                    print(f"[Processor] Errore inserimento ClickHouse: {e}")

if __name__ == "__main__":
    main()