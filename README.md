# Workday Web Content Extraction — Demo Flow

## Overview

Transform unstructured web content into governed, queryable, AI-enriched data using the Databricks AI platform.

**End-to-end flow:**

```
Live Web Pages ──> AI Gateway (LLM Extraction) ──> Raw UC Table ──> SQL AI Functions ──> Enriched UC Table
                        |                                                  |
                   Every call logged                              classify, summarize,
                   (tokens, cost, caller)                         extract, custom queries
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DATABRICKS AI PLATFORM                                 │
│                                                                                 │
│  ┌──────────┐    ┌─────────────────────────────┐    ┌────────────────────────┐  │
│  │          │    │       AI GATEWAY             │    │    Unity Catalog       │  │
│  │  Live    │───>│  ┌───────────────────────┐   │───>│                        │  │
│  │  URL     │    │  │ Usage Tracking        │   │    │  Volume: raw HTML      │  │
│  │  Fetch   │    │  │ Payload Logging       │   │    │  Volume: extracted JSON │  │
│  │          │    │  │ Rate Limits           │   │    │  Table: web_content_raw│  │
│  └──────────┘    │  │ Cost Attribution      │   │    │                        │  │
│                  │  │ Model Portability     │   │    └──────────┬─────────────┘  │
│                  │  └───────────────────────┘   │               │                │
│                  │       |          |           │               │                │
│                  │   Claude    Llama    GPT     │               ▼                │
│                  │   (swappable, zero code)     │    ┌────────────────────────┐  │
│                  └─────────────────────────────┘    │   SQL AI Functions      │  │
│                                                     │                        │  │
│                  ┌─────────────────────────────┐    │  ai_classify()         │  │
│                  │    System Tables             │    │  ai_summarize()        │  │
│                  │                              │    │  ai_extract()          │  │
│                  │  system.ai_gateway.usage     │    │  ai_query()            │  │
│                  │  - tokens per call           │    │                        │  │
│                  │  - caller identity           │    └──────────┬─────────────┘  │
│                  │  - latency                   │               │                │
│                  │  - cost                      │               ▼                │
│                  │                              │    ┌────────────────────────┐  │
│                  │  Inference Tables             │    │  web_content_enriched  │  │
│                  │  - full request payloads     │    │                        │  │
│                  │  - full response payloads    │    │  + topic classification│  │
│                  │                              │    │  + summary             │  │
│                  └─────────────────────────────┘    │  + entities            │  │
│                                                     │  + value proposition   │  │
│                                                     │  + competitive intel   │  │
│                                                     └────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Demo Steps

### Step 1: Fetch Web Content (2 min)
- Fetch 3 live Workday URLs directly in the notebook
- No pre-saved files — demonstrates real-time ingestion
- **Pages:**
  - Blog/Podcast: "The AI Adoption Playbook: A CIO's Guide"
  - Ebook TOC: "Workday Platform Technology" (8 chapters, 60+ sections)
  - Article: "AI Agents in Enterprise" (author bio, 8 agent types, use cases)

### Step 2: LLM Extraction via AI Gateway (5 min)
- One structured prompt handles all 3 page types
- Calls go through AI Gateway (`ai-gateway.cloud.databricks.com`)
- **Demo talking point:** Open the AI Gateway UI and show the 3 logged requests
- Each call shows: tokens, latency, caller identity, request/response payloads

### Step 3: Raw Table (1 min)
- Single `web_content_raw` table with all extracted fields
- Full JSON preserved alongside parsed columns
- Columns: source_url, title, content_type, authors, headline, full_text, quotes

### Step 4: AI Enrichment with SQL AI Functions (3 min)
- `ai_classify()` — categorize each page into topics
- `ai_summarize()` — generate concise summaries
- `ai_extract()` — pull out people, orgs, products, technologies
- `ai_query()` — custom analysis: CIO value prop, competitive mentions
- **Demo talking point:** These SQL functions also go through AI Gateway — more governed calls

### Step 5: Query Enriched Data (2 min)
- Show topic classifications across all pages
- Show extracted entities (people, companies, products)
- Show AI-generated value propositions and competitive analysis
- **Demo talking point:** This was raw HTML 10 minutes ago

### Step 6: Governance Payoff (3 min)
- Query `system.ai_gateway.usage` — show all LLM calls with tokens, cost, caller
- Show payload logging table — full request/response audit trail
- **Key message:** Every AI call — extraction AND SQL functions — is governed

---

## Key Demo Messages

> "Every AI call in your organization — whether from a notebook, a SQL query, or an application — flows through one governance layer. You see who called what model, when, with what data, and at what cost. No shadow AI."

> "One prompt extracts structured data from 3 completely different web page types. The output lands in a Delta table. SQL AI functions enrich it further. No Python ML pipelines needed."

> "Swap Claude for Llama or GPT by changing one variable. Rate limits prevent runaway costs. Payload logging gives you full audit trails. All built into the platform."

---

## Features Showcased

| Feature | What it does in this demo |
|---|---|
| **AI Gateway** | Routes LLM calls with logging, rate limits, cost tracking |
| **SQL AI Functions** | `ai_classify`, `ai_summarize`, `ai_extract`, `ai_query` in plain SQL |
| **Unity Catalog Volumes** | Store raw HTML and extracted JSON as governed files |
| **UC Tables** | Raw + enriched tables with full lineage |
| **System Tables** | `system.ai_gateway.usage` for usage analytics |
| **Inference Tables** | Full payload logging for audit/compliance |
| **AI Playground** | Test prompts interactively before embedding in code |
| **Model Portability** | Swap between Claude, Llama, GPT — zero code changes |

---

## Appendix: AI Gateway Governance Capabilities

| Capability | Description |
|---|---|
| **Usage Tracking** | Every call logged to `system.ai_gateway.usage` with tokens, latency, caller, model |
| **Payload Logging** | Full request/response bodies saved to inference tables for audit |
| **Rate Limiting** | Per-endpoint TPM (tokens per minute) and RPM (requests per minute) caps |
| **Cost Attribution** | Calls tagged to service principals or users — know who spent what |
| **Model Portability** | Swap underlying model without code changes — one config change |
| **Guardrails** | PII detection/redaction, content filtering at the endpoint level |
| **Centralized Auth** | Workspace IAM — no API keys in code, notebooks, or env vars |
| **Fallback Routes** | Automatic failover across providers if primary model is unavailable |

---

## Appendix: SQL AI Functions Reference

```sql
-- Classify text into categories
ai_classify('text', ARRAY('Category A', 'Category B', 'Category C'))
-- Returns: string (best matching category)

-- Summarize text
ai_summarize('long text here', 100)
-- Returns: string (summary in ~100 words)

-- Extract named entities
ai_extract('text', ARRAY('person', 'organization', 'product'))
-- Returns: JSON string with extracted entities

-- Custom LLM query
ai_query('model-endpoint-name', 'your prompt here')
-- Returns: string (model response)
```
