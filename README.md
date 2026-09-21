# Tech Trends Tracker

Sistema distribuito in tempo reale per l'acquisizione, l'estrazione semantica (NER) e l'analisi quantitativa dei trend tecnologici emergenti nell'ecosistema software e developer community.

Progetto per il corso di **Technologies for Advanced Programming (TAP)**  
**Autore:** Alessio Luca Rosolia

---

## 📌 Panoramica del Progetto

Il monitoraggio dell'ecosistema tecnologico è soggetto a un forte **information overload**: ogni mese nascono nuovi framework, librerie e linguaggi di programmazione. **Tech Trends Tracker** automatizza l'ascolto continuo delle discussioni tecniche, estraendo tramite Named Entity Recognition (NER) basata su LLM locali esclusivamente tool software reali, persistendoli su un database colonnare ad alte prestazioni per la rilevazione di picchi e spike di adozione in tempo reale.

---

## 🏗️ Architettura di Sistema

La pipeline è completamente containerizzata tramite Docker Compose e si articola in 5 stadi:
### Componenti Principali:
1. **Data Ingestion (Python & Fluentd):** Acquisizione continua e streaming di post e articoli tecnici da Reddit, Hacker News, Bluesky, GitHub trending e arXiv.
2. **Message Broker (Apache Kafka):** Disaccoppiamento asincrono produttori/consumatori, backpressure handling e garanzia di zero data-loss.
3. **Inference & Filter Layer (Python Worker):** 
   - **Pre-filtering euristico:** Scarto immediato di stringhe non informatiche senza impattare la GPU.
   - **Local LLM NER:** Estrazione del software tramite modello locale **Qwen 2.5 Coder 3B Instruct** in esecuzione su **LM Studio**.
   - **Post-processing deterministico:** Pulizia tramite `BLACKLIST` (aziende tech, hardware, metodologie AI astratte) e `SYNONYM_MAP` per la normalizzazione dei nomi.
4. **Analytical Storage (ClickHouse):** Database colonnare ottimizzato per serie temporali e aggregazioni su larga scala.
5. **Visualization (Grafana):** Dashboard analitica in tempo reale con tracciamento temporale, conteggi globali e spike detection.

---
## 🛠️ Requisiti

- [Docker](https://www.docker.com/) e Docker Compose v2+
- [LM Studio](https://lmstudio.ai/) installato sul sistema host con:
  - Modello caricato: **Qwen 2.5 Coder 7B Instruct** (GGUF quantizzato)
  - GPU Acceleration / Offload al 100%
  - Server locale avviato sulla porta predefinita `1234` con CORS abilitato

---

## 🚀 Installazione e Avvio Rapido

1. **Clonare il repository:**
   ```bash
   git clone [https://github.com/TUO_USERNAME/tech-trends-tracker.git](https://github.com/TUO_USERNAME/tech-trends-tracker.git)
   cd tech-trends-tracker
