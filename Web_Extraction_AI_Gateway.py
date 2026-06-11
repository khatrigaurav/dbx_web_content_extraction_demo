# Databricks notebook source
# MAGIC %md
# MAGIC # Unstructured Web Content → Structured UC Tables via AI Gateway
# MAGIC
# MAGIC **Flow:** Live URL Fetch → AI Gateway (LLM Extraction) → JSON (Volume) → UC Tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC %md
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐    ┌───────────────────────────────────────┐    ┌─────────────────┐    ┌─────────────────┐
# MAGIC │                  │    │           AI Gateway                  │    │                 │    │                 │
# MAGIC │  Live URL Fetch  │───▶│  ┌─────────────────────────────────┐ │───▶│  JSON Volume    │───▶│  UC Tables      │
# MAGIC │  (3 web pages)   │    │  │ Logging │ Rate Limits │ Auth    │ │    │  (staging)      │    │  (6 tables)     │
# MAGIC │                  │    │  │ Audit   │ Guardrails  │ Routing │ │    │                 │    │                 │
# MAGIC └──────────────────┘    │  └─────────────────────────────────┘ │    └─────────────────┘    └─────────────────┘
# MAGIC                         │         ▼            ▼               │
# MAGIC                         │    Claude    or   Llama   or   GPT   │
# MAGIC                         │    (swappable without code changes)  │
# MAGIC                         └───────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "gaurav_catalog"     # ← CHANGE THIS
SCHEMA  = "default"            # ← CHANGE THIS

VOLUME_JSON = f"/Volumes/{CATALOG}/{SCHEMA}/extracted_json"

# Swap to "databricks-meta-llama-3-3-70b-instruct" or any external model endpoint
# and nothing below changes.
AI_GATEWAY_ENDPOINT = "databricks-claude-sonnet-4"

#List of urls
PAGES = {
    "podcast_ai_adoption": "https://blog.workday.com/en-us/fy26-ct-podcast-fow-blueprint-for-ai-adoption-enus.html",
    # "ebook_platform_toc":  "https://knowledge.workday.com/en-us-ebook/workday-platform-technology/table-of-contents",
    "article_ai_agents":   "https://www.workday.com/en-us/perspectives/ai/ai-agents-enterprise-how-will-they-change-way-we-work.html",
}

# COMMAND ----------

# DBTITLE 1,Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.extracted_json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Live Fetch — Pull HTML directly during the session
# MAGIC
# MAGIC No pre-saved files. We hit the URLs right now and capture the raw HTML.

# COMMAND ----------

import requests

def fetch_page(url: str) -> str:
    """Fetch a URL and return the raw HTML."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text

# COMMAND ----------

# DBTITLE 1,Fetch all 3 pages live
raw_html = {}
for name, url in PAGES.items():
    print(f"Fetching: {url}")
    raw_html[name] = fetch_page(url)
    print(f"  ✓ {len(raw_html[name]):,} characters captured")

print(f"\n✓ All {len(raw_html)} pages fetched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract structured data via AI Gateway
# MAGIC
# MAGIC Every LLM call below goes through AI Gateway, which means:
# MAGIC - Logged in the **Serving UI** (input/output tokens, latency, status code)
# MAGIC - **Attributed** to the calling service principal or user
# MAGIC - **Rate-limited** per your endpoint configuration
# MAGIC - **Model-portable** — change `AI_GATEWAY_ENDPOINT` above, zero code changes below
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extraction prompt — one prompt handles all 3 page types
EXTRACTION_PROMPT = """You are a structured data extraction engine. You will receive raw HTML from a web page. Extract ALL available information into the exact JSON schema below.

## Rules
1. Extract text EXACTLY as it appears on the page — do NOT translate, summarize, or paraphrase
2. If a field is not present on the page, set it to null
3. For array fields, return [] if none found
4. Return ONLY valid JSON — no markdown fencing, no explanation, no preamble
5. Detect the content_type from the page structure

## JSON Schema

{
  "page_metadata": {
    "source_url": "string",
    "title": "string",
    "language": "string (ISO 639-1)",
    "meta_description": "string | null",
    "publish_date": "string (YYYY-MM-DD) | null",
    "read_time": "string | null",
    "content_type": "blog_post | ebook_toc | article | podcast | report",
    "categories": ["string"],
    "tags": ["string"]
  },

  "authors": [
    {
      "name": "string",
      "title": "string | null",
      "organization": "string | null",
      "bio": "string | null"
    }
  ],

  "content_body": {
    "headline": "string",
    "subheadline": "string | null",
    "sections": [
      {
        "heading": "string",
        "body_text": "string",
        "key_points": ["string"]
      }
    ],
    "quotes": [
      {
        "text": "string (exact quote)",
        "speaker": "string",
        "speaker_title": "string | null"
      }
    ]
  },

  "ebook_structure": {
    "ebook_title": "string | null",
    "chapters": [
      {
        "chapter_number": "integer",
        "chapter_title": "string",
        "sections": ["string"]
      }
    ]
  },

  "calls_to_action": [
    {
      "cta_text": "string",
      "cta_url": "string | null",
      "cta_context": "string"
    }
  ],

  "related_content": [
    {
      "title": "string",
      "url": "string | null",
      "description": "string | null",
      "content_type": "string"
    }
  ],

  "embedded_media": [
    {
      "media_type": "podcast | video | image",
      "platform": "string | null",
      "title": "string | null",
      "embed_url": "string | null"
    }
  ],

  "compliance": {
    "privacy_policy_url": "string | null",
    "cookie_consent_required": "boolean",
    "third_party_services": ["string"]
  }
}"""

# COMMAND ----------

# DBTITLE 1,Call AI Gateway for each page
import json
import datetime
import requests as req
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()


def _build_messages(source_url: str, html_content: str) -> list:
    return [
        {"role": "system", "content": EXTRACTION_PROMPT},
        {"role": "user", "content": f"Source URL: {source_url}\n\nHTML:\n{html_content}"},
    ]


def _query_serving_endpoint(messages: list) -> str:
    """For pay-per-token / external model serving endpoints.

    The SDK's serving_endpoints.query() expects ChatMessage objects (it calls
    .as_dict() on each), so convert the plain role/content dicts here.
    """
    chat_messages = [
        ChatMessage(role=ChatMessageRole(m["role"]), content=m["content"])
        for m in messages
    ]
    response = w.serving_endpoints.query(
        name=AI_GATEWAY_ENDPOINT,
        messages=chat_messages,
        temperature=0.0,
        max_tokens=8192,
    )
    return response.choices[0].message.content


def _query_gateway_route(messages: list) -> str:
    """For AI Gateway routes (REST call using notebook-native token)."""
    import os

    # Inside a Databricks notebook, use the built-in notebook token
    host = spark.conf.get("spark.databricks.workspaceUrl", None)
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    if not host:
        # Fallback to SDK config
        config = w.config
        host = config.host.rstrip("/").replace("https://", "")
        token = config.token

    resp = req.post(
        f"https://{host}/serving-endpoints/{AI_GATEWAY_ENDPOINT}/invocations",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"messages": messages, "temperature": 0.0, "max_tokens": 8192},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _detect_endpoint_type() -> str:
    """Auto-detect whether the endpoint is a serving endpoint or an AI Gateway route."""
    try:
        ep = w.serving_endpoints.get(AI_GATEWAY_ENDPOINT)
        return "serving_endpoint"
    except Exception:
        return "gateway_route"


ENDPOINT_TYPE = _detect_endpoint_type()
print(f"Detected endpoint type: {ENDPOINT_TYPE} (endpoint: {AI_GATEWAY_ENDPOINT})")


def extract_page(html_content: str, source_url: str) -> dict:
    """Send HTML to the LLM endpoint, auto-routing based on endpoint type."""
    messages = _build_messages(source_url, html_content)

    if ENDPOINT_TYPE == "serving_endpoint":
        raw = _query_serving_endpoint(messages)
    else:
        raw = _query_gateway_route(messages)

    # Strip markdown fencing if the LLM adds it despite instructions
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0]

    return json.loads(raw)

# COMMAND ----------

# DBTITLE 1,Run extraction on all 3 pages
results = []

for name, url in PAGES.items():
    print(f"Extracting: {name} ...")
    extracted = extract_page(raw_html[name], url)

    # Tag with pipeline metadata for lineage
    extracted["_pipeline"] = {
        "source_key": name,
        "extraction_timestamp": datetime.datetime.utcnow().isoformat(),
        "model_endpoint": AI_GATEWAY_ENDPOINT,
    }
    results.append(extracted)

    # Persist to Volume
    output_path = f"{VOLUME_JSON}/{name}.json"
    with open(output_path, "w") as f:
        json.dump(extracted, f, indent=2)
    print(f"  ✓ Saved → {output_path}")

print(f"\n✓ Extracted {len(results)} pages via AI Gateway")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. JSON → UC Tables
# MAGIC
# MAGIC Parse the structured JSON into a normalized star schema — 6 governed tables from 3 raw web pages.

# COMMAND ----------

# DBTITLE 1,Load extracted JSON
from pyspark.sql import functions as F

df_raw = spark.read.option("multiline", True).json(VOLUME_JSON)
df_raw.cache()
print(f"Loaded {df_raw.count()} extracted documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: `extracted_pages` — one row per page (fact table)

# COMMAND ----------

df_pages = df_raw.select(
    F.col("_pipeline.source_key").alias("source_key"),
    F.col("_pipeline.extraction_timestamp").cast("timestamp").alias("extracted_at"),
    F.col("_pipeline.model_endpoint").alias("model_endpoint"),
    F.col("page_metadata.source_url").alias("url"),
    F.col("page_metadata.title"),
    F.col("page_metadata.content_type"),
    F.col("page_metadata.language"),
    F.col("page_metadata.publish_date").cast("date").alias("publish_date"),
    F.col("page_metadata.read_time"),
    F.col("page_metadata.categories"),
    F.col("page_metadata.tags"),
    F.col("content_body.headline"),
    F.col("content_body.subheadline"),
)

df_pages.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_pages")
display(df_pages)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: `extracted_authors` — one row per author per page

# COMMAND ----------

df_authors = df_raw.select(
    F.col("page_metadata.source_url").alias("page_url"),
    F.explode_outer("authors").alias("author"),
).select(
    "page_url",
    F.col("author.name").alias("author_name"),
    F.col("author.title").alias("author_title"),
    F.col("author.organization"),
    F.col("author.bio"),
)

df_authors.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_authors")
display(df_authors)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 3: `extracted_sections` — every content section across all pages

# COMMAND ----------

df_sections = df_raw.select(
    F.col("page_metadata.source_url").alias("page_url"),
    F.col("page_metadata.content_type"),
    F.posexplode_outer("content_body.sections").alias("section_order", "section"),
).select(
    "page_url",
    "content_type",
    "section_order",
    F.col("section.heading"),
    F.col("section.body_text"),
    F.col("section.key_points"),
)

df_sections.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_sections")
display(df_sections)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 4: `extracted_quotes` — executive quotes about AI

# COMMAND ----------

df_quotes = df_raw.select(
    F.col("page_metadata.source_url").alias("page_url"),
    F.explode_outer("content_body.quotes").alias("quote"),
).select(
    "page_url",
    F.col("quote.text").alias("quote_text"),
    F.col("quote.speaker"),
    F.col("quote.speaker_title"),
)

df_quotes.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_quotes")
display(df_quotes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 5: `extracted_ebook_chapters` — ebook table of contents structure

# COMMAND ----------

# DBTITLE 1,Cell 24
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

ebook_chapters_schema = StructType([
    StructField("page_url", StringType(), True),
    StructField("ebook_title", StringType(), True),
    StructField("chapter_number", IntegerType(), True),
    StructField("chapter_title", StringType(), True),
    StructField("section_titles", ArrayType(StringType()), True),
])

df_ebook = df_raw.filter(F.col("page_metadata.content_type") == "ebook_toc")

if df_ebook.count() > 0:
    df_chapters = df_ebook.select(
        F.col("page_metadata.source_url").alias("page_url"),
        F.col("ebook_structure.ebook_title"),
        F.explode_outer("ebook_structure.chapters").alias("chapter"),
    ).select(
        "page_url",
        "ebook_title",
        F.col("chapter.chapter_number"),
        F.col("chapter.chapter_title"),
        F.col("chapter.sections").alias("section_titles"),
    )
else:
    df_chapters = spark.createDataFrame([], ebook_chapters_schema)

df_chapters.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_ebook_chapters")
display(df_chapters)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 6: `extracted_ctas` — every call to action across all pages

# COMMAND ----------

df_ctas = df_raw.select(
    F.col("page_metadata.source_url").alias("page_url"),
    F.explode_outer("calls_to_action").alias("cta"),
).select(
    "page_url",
    F.col("cta.cta_text"),
    F.col("cta.cta_url"),
    F.col("cta.cta_context"),
)

df_ctas.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.extracted_ctas")
display(df_ctas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query — Unstructured web content is now governed and queryable

# COMMAND ----------

# MAGIC %md
# MAGIC ### What AI topics does Workday cover across all content types?

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}");
spark.sql(f"USE SCHEMA {SCHEMA}");



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.content_type,
# MAGIC   p.title AS page_title,
# MAGIC   s.heading AS section_topic,
# MAGIC   s.key_points
# MAGIC FROM extracted_pages p
# MAGIC JOIN extracted_sections s ON p.url = s.page_url
# MAGIC WHERE s.heading IS NOT NULL
# MAGIC ORDER BY p.content_type, s.section_order

# COMMAND ----------

# MAGIC %md
# MAGIC ### All executive quotes about AI

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT speaker, speaker_title, quote_text
# MAGIC FROM extracted_quotes
# MAGIC WHERE speaker IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-page content analysis: where do CTAs point?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   p.title AS source_page,
# MAGIC   c.cta_text,
# MAGIC   c.cta_url,
# MAGIC   c.cta_context
# MAGIC FROM extracted_ctas c
# MAGIC JOIN extracted_pages p ON c.page_url = p.url
# MAGIC ORDER BY p.title