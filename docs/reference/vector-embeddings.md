# Vector Embeddings with KalamDB EMBEDDING Type

**Version**: 0.1.3  
**Feature**: EMBEDDING(dimension) data type  
**Last Updated**: 2025-12-31

---

## Overview

KalamDB provides a native `EMBEDDING(dimension)` data type for storing vector embeddings from machine learning models. This guide demonstrates how to use KalamDB for semantic search, recommendation systems, and AI applications.

Note: Some snippets use a generic `client.execute(...)` / `client.query(...)` shape as pseudocode. The official JS/TS client documented here is `@kalamdb/client`.

**What are Vector Embeddings?**
- Numerical representations of text, images, or other data
- Enable semantic similarity search (find similar content)
- Power AI applications (chatbots, recommendations, search)

**KalamDB Storage**:
- Native `EMBEDDING(dimension)` type (FixedSizeList<Float32> in Arrow/Parquet)
- Efficient storage with 30-50% compression
- Sub-millisecond writes, fast batch inserts
- Compatible with popular ML frameworks (OpenAI, Hugging Face, Sentence Transformers)

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Common Use Cases](#common-use-cases)
   - [Semantic Document Search](#semantic-document-search)
   - [Chatbot Message History with Embeddings](#chatbot-message-history-with-embeddings)
   - [Product Recommendations](#product-recommendations)
   - [Image Similarity Search](#image-similarity-search)
3. [Integration Patterns](#integration-patterns)
  - [TypeScript SDK](#typescript-sdk)
4. [Performance Optimization](#performance-optimization)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

---

## Quick Start

### 1. Create Table with EMBEDDING Column

```sql
-- Semantic document search (MiniLM embeddings)
CREATE TABLE app.documents (
  doc_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding EMBEDDING(384),  -- MiniLM sentence embeddings
  created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');

-- OpenAI embeddings (text-embedding-3-small)
CREATE TABLE app.knowledge_base (
  article_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  category TEXT,
  text TEXT NOT NULL,
  embedding EMBEDDING(1536),  -- OpenAI text-embedding-3-small
  updated_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'SHARED');
```

### 2. Insert Embeddings from Application

There is no official Python SDK documented for KalamDB. If you're using Python, generate embeddings however you like, then insert via the REST SQL endpoint (`POST /v1/api/sql`).

```python
from sentence_transformers import SentenceTransformer
import requests

# Load model (downloads automatically)
model = SentenceTransformer('all-MiniLM-L6-v2')  # 384-dimensional embeddings

# Generate embedding for document
text = "KalamDB is a real-time database with native vector storage"
embedding = model.encode(text)  # numpy array with 384 floats

# Insert document with embedding (parameterized SQL uses $1, $2, ...)
payload = {
  "sql": "INSERT INTO app.documents (title, content, embedding) VALUES ($1, $2, $3)",
  "params": ["Introduction", text, embedding.tolist()],
}

resp = requests.post(
  "http://localhost:8080/v1/api/sql",
  auth=("user1", "pass"),
  json=payload,
  timeout=30,
)
resp.raise_for_status()
```

### 3. Query and Perform Similarity Search

```python
# Retrieve all documents
resp = requests.post(
  "http://localhost:8080/v1/api/sql",
  auth=("user1", "pass"),
  json={"sql": "SELECT doc_id, title, embedding FROM app.documents"},
  timeout=30,
)
resp.raise_for_status()
rows = resp.json()["results"][0]["rows"]

# Compute cosine similarity in application
import numpy as np

def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

query = "What is KalamDB?"
query_embedding = model.encode(query)

# Rank documents by similarity
results = []
for row in rows:
    similarity = cosine_similarity(query_embedding, np.array(row['embedding']))
    results.append({
        'doc_id': row['doc_id'],
        'title': row['title'],
        'score': similarity
    })

# Sort by similarity score (descending)
results.sort(key=lambda x: x['score'], reverse=True)

# Top 5 most similar documents
for i, result in enumerate(results[:5]):
    print(f"{i+1}. {result['title']} (score: {result['score']:.4f})")
```

---

## Common Use Cases

### Semantic Document Search

**Scenario**: Search internal documentation, knowledge bases, or research papers by meaning (not just keywords).

**Schema**:

```sql
CREATE TABLE kb.articles (
  article_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title TEXT NOT NULL,
  category TEXT,
  content TEXT NOT NULL,
  embedding EMBEDDING(768),  -- BERT-base embeddings
  author TEXT,
  published_at TIMESTAMP DEFAULT NOW(),
  views INT DEFAULT 0
) WITH (TYPE = 'SHARED');

-- Index for category filtering
CREATE INDEX idx_category ON kb.articles(category);
```

**Python Implementation**:

```python
from sentence_transformers import SentenceTransformer
import requests
import numpy as np

# Initialize
model = SentenceTransformer('bert-base-nli-mean-tokens')  # 768 dimensions

BASE_URL = "http://localhost:8080"
AUTH = ("admin", "secret")

def exec_sql(sql: str, params=None):
  payload = {"sql": sql}
  if params is not None:
    payload["params"] = params
  resp = requests.post(f"{BASE_URL}/v1/api/sql", auth=AUTH, json=payload, timeout=30)
  resp.raise_for_status()
  return resp.json()

# 1. Ingest articles with embeddings
def ingest_article(title, content, category):
    embedding = model.encode(content)
  exec_sql(
    "INSERT INTO kb.articles (title, content, category, embedding, author) VALUES ($1, $2, $3, $4, $5)",
    [title, content, category, embedding.tolist(), "admin"],
  )

ingest_article("Getting Started with KalamDB", "...", "tutorial")
ingest_article("Schema Consolidation Design", "...", "architecture")

# 2. Semantic search function
def semantic_search(query, category=None, top_k=10):
    query_embedding = model.encode(query)
    
    # Fetch candidates (optionally filter by category)
    sql = "SELECT article_id, title, category, embedding FROM kb.articles"
    if category:
      result = exec_sql(sql + " WHERE category = $1", [category])
    else:
      result = exec_sql(sql)

    rows = result["results"][0]["rows"]
    
    # Compute similarity scores
    results = []
    for row in rows:
        article_emb = np.array(row['embedding'])
        score = np.dot(query_embedding, article_emb) / (
            np.linalg.norm(query_embedding) * np.linalg.norm(article_emb)
        )
        results.append({
            'article_id': row['article_id'],
            'title': row['title'],
            'category': row['category'],
            'score': score
        })
    
    # Sort and return top K
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]

# 3. Search usage
results = semantic_search("How do I create tables in KalamDB?", category="tutorial")
for i, result in enumerate(results, 1):
    print(f"{i}. [{result['category']}] {result['title']} - {result['score']:.4f}")
```

**Expected Output**:

```
1. [tutorial] Getting Started with KalamDB - 0.8721
2. [architecture] Schema Consolidation Design - 0.6543
...
```

---

### Chatbot Message History with Embeddings

**Scenario**: Store chat messages with embeddings to enable context-aware responses and conversation search.

**Schema**:

```sql
CREATE TABLE chat.messages (
  message_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  conversation_id TEXT NOT NULL,
  user_id UUID NOT NULL,
  role TEXT NOT NULL,  -- 'user' or 'assistant'
  content TEXT NOT NULL,
  embedding EMBEDDING(1536),  -- OpenAI text-embedding-3-small
  timestamp TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER', FLUSH_POLICY = 'rows:1000');

CREATE INDEX idx_conversation ON chat.messages(conversation_id);
```

**TypeScript Implementation (OpenAI)**:

```typescript
import { Configuration, OpenAIApi } from 'openai';
import { createClient, Auth } from '@kalamdb/client';

const openai = new OpenAIApi(new Configuration({ apiKey: process.env.OPENAI_API_KEY }));
const kalam = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('chatbot', 'key')
});
await kalam.connect();

// 1. Store message with embedding
async function storeMessage(conversationId: string, userId: string, role: string, content: string) {
  // Generate embedding with OpenAI
  const response = await openai.createEmbedding({
    model: "text-embedding-3-small",
    input: content,
  });
  
  const embedding = response.data.data[0].embedding;  // 1536 floats
  
  // Store in KalamDB
  await kalam.query(
    'INSERT INTO chat.messages (conversation_id, user_id, role, content, embedding) VALUES ($1, $2, $3, $4, $5)',
    [conversationId, userId, role, content, embedding]
  );
}

// 2. Retrieve similar past messages (context search)
async function findSimilarMessages(conversationId: string, query: string, topK: number = 5) {
  // Generate query embedding
  const response = await openai.createEmbedding({
    model: "text-embedding-3-small",
    input: query,
  });
  const queryEmbedding = response.data.data[0].embedding;
  
  // Fetch conversation history
  const result = await kalam.query(`
    SELECT message_id, role, content, embedding 
    FROM chat.messages 
    WHERE conversation_id = $1 
    ORDER BY timestamp DESC 
    LIMIT 100
  `, [conversationId]);

  const rows = result.results[0]?.rows ?? [];
  
  // Compute cosine similarity
  const results = rows.map(row => {
    const dotProduct = queryEmbedding.reduce((sum, val, i) => sum + val * row.embedding[i], 0);
    const normA = Math.sqrt(queryEmbedding.reduce((sum, val) => sum + val * val, 0));
    const normB = Math.sqrt(row.embedding.reduce((sum, val) => sum + val * val, 0));
    const similarity = dotProduct / (normA * normB);
    
    return { ...row, similarity };
  });
  
  // Sort by similarity and return top K
  return results.sort((a, b) => b.similarity - a.similarity).slice(0, topK);
}

// 3. Usage in chatbot
async function handleUserMessage(conversationId: string, userId: string, userMessage: string) {
  // Store user message with embedding
  await storeMessage(conversationId, userId, 'user', userMessage);
  
  // Find similar past messages for context
  const similarMessages = await findSimilarMessages(conversationId, userMessage, 3);
  
  // Build context for LLM
  const context = similarMessages.map(m => `[${m.role}]: ${m.content}`).join('\n');
  
  // Generate response (with context)
  const response = await openai.createChatCompletion({
    model: "gpt-3.5-turbo",
    messages: [
      { role: "system", content: "You are a helpful assistant. Use the following context from past conversation:\n" + context },
      { role: "user", content: userMessage }
    ]
  });
  
  const assistantMessage = response.data.choices[0].message.content;
  
  // Store assistant response with embedding
  await storeMessage(conversationId, userId, 'assistant', assistantMessage);
  
  return assistantMessage;
}

// Example usage
await handleUserMessage('conv-123', 'user-uuid', 'What were we discussing about databases?');
```

---

### Product Recommendations

**Scenario**: Recommend products based on semantic similarity of descriptions.

**Schema**:

```sql
CREATE TABLE ecommerce.products (
  product_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  category TEXT,
  price DECIMAL(10, 2) NOT NULL,
  embedding EMBEDDING(384),  -- Product description embeddings
  inventory INT DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'SHARED');

CREATE INDEX idx_category ON ecommerce.products(category);
```

**Python Implementation**:

```python
from sentence_transformers import SentenceTransformer
import requests
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')

BASE_URL = "http://localhost:8080"
AUTH = ("shop", "key")

def exec_sql(sql: str, params=None):
  payload = {"sql": sql}
  if params is not None:
    payload["params"] = params
  resp = requests.post(f"{BASE_URL}/v1/api/sql", auth=AUTH, json=payload, timeout=30)
  resp.raise_for_status()
  return resp.json()

# 1. Add product with embedding
def add_product(name, description, category, price):
    embedding = model.encode(description)
  exec_sql(
    "INSERT INTO ecommerce.products (name, description, category, price, embedding) VALUES ($1, $2, $3, $4, $5)",
    [name, description, category, price, embedding.tolist()],
  )

# 2. Find similar products (recommendations)
def recommend_products(product_id, top_k=5):
    # Get target product embedding
  target_result = exec_sql(
    "SELECT name, description, embedding FROM ecommerce.products WHERE product_id = $1 LIMIT 1",
    [product_id],
  )
  target_rows = target_result["results"][0]["rows"]
  target = target_rows[0] if target_rows else None
    
    if not target:
        return []
    
    target_emb = np.array(target['embedding'])
    
    # Fetch all products (except target)
    rows_result = exec_sql(
      "SELECT product_id, name, description, price, embedding FROM ecommerce.products WHERE product_id != $1",
      [product_id],
    )
    rows = rows_result["results"][0]["rows"]
    
    # Compute similarity
    results = []
    for row in rows:
        product_emb = np.array(row['embedding'])
        similarity = np.dot(target_emb, product_emb) / (
            np.linalg.norm(target_emb) * np.linalg.norm(product_emb)
        )
        results.append({
            'product_id': row['product_id'],
            'name': row['name'],
            'price': row['price'],
            'score': similarity
        })
    
    # Sort and return top K
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]

# 3. Usage
add_product("Laptop Stand", "Adjustable aluminum laptop stand for ergonomic typing", "accessories", 49.99)
add_product("Wireless Keyboard", "Bluetooth keyboard with backlight and ergonomic design", "accessories", 79.99)

recommendations = recommend_products(product_id=1, top_k=3)
for i, rec in enumerate(recommendations, 1):
    print(f"{i}. {rec['name']} (${rec['price']}) - {rec['score']:.4f}")
```

---

### Image Similarity Search

**Scenario**: Find visually similar images using CLIP embeddings.

**Schema**:

```sql
CREATE TABLE media.images (
  image_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  file_path TEXT NOT NULL,
  description TEXT,
  embedding EMBEDDING(512),  -- CLIP ViT-B/32 embeddings
  uploaded_by UUID,
  uploaded_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'SHARED');
```

**Python Implementation (CLIP)**:

```python
import torch
import clip
from PIL import Image
import requests
import numpy as np

# Load CLIP model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)

BASE_URL = "http://localhost:8080"
AUTH = ("media", "key")

def exec_sql(sql: str, params=None):
  payload = {"sql": sql}
  if params is not None:
    payload["params"] = params
  resp = requests.post(f"{BASE_URL}/v1/api/sql", auth=AUTH, json=payload, timeout=30)
  resp.raise_for_status()
  return resp.json()

# 1. Upload image with embedding
def upload_image(file_path, description):
    image = preprocess(Image.open(file_path)).unsqueeze(0).to(device)
    
    with torch.no_grad():
        embedding = model.encode_image(image)
        embedding = embedding.cpu().numpy().flatten()  # 512 floats
    
    exec_sql(
      "INSERT INTO media.images (file_path, description, embedding) VALUES ($1, $2, $3)",
      [file_path, description, embedding.tolist()],
    )

# 2. Search by text query
def search_images_by_text(query, top_k=10):
    text = clip.tokenize([query]).to(device)
    
    with torch.no_grad():
        query_embedding = model.encode_text(text)
        query_embedding = query_embedding.cpu().numpy().flatten()
    
    # Fetch all images
    rows = exec_sql("SELECT image_id, file_path, description, embedding FROM media.images")["results"][0]["rows"]
    
    # Compute similarity
    results = []
    for row in rows:
        image_emb = np.array(row['embedding'])
        similarity = np.dot(query_embedding, image_emb) / (
            np.linalg.norm(query_embedding) * np.linalg.norm(image_emb)
        )
        results.append({
            'image_id': row['image_id'],
            'file_path': row['file_path'],
            'description': row['description'],
            'score': similarity
        })
    
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:top_k]

# 3. Usage
upload_image("/path/to/dog.jpg", "Golden retriever playing fetch")
upload_image("/path/to/cat.jpg", "Siamese cat sleeping on couch")

results = search_images_by_text("cute pet", top_k=5)
for i, result in enumerate(results, 1):
    print(f"{i}. {result['file_path']} - {result['description']} ({result['score']:.4f})")
```

---

## Integration Patterns

### Python with Sentence Transformers

**Install Dependencies**:

```bash
pip install sentence-transformers requests numpy
```

**Complete Example**:

```python
from sentence_transformers import SentenceTransformer
import requests
import numpy as np

# 1. Initialize
model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dimensions

BASE_URL = "http://localhost:8080"
AUTH = ("user", "pass")

def exec_sql(sql: str, params=None):
  payload = {"sql": sql}
  if params is not None:
    payload["params"] = params
  resp = requests.post(f"{BASE_URL}/v1/api/sql", auth=AUTH, json=payload, timeout=30)
  resp.raise_for_status()
  return resp.json()

# 2. Create table
exec_sql("""
  CREATE TABLE IF NOT EXISTS app.docs (
    doc_id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    text TEXT NOT NULL,
    embedding EMBEDDING(384)
  ) WITH (TYPE = 'USER')
""")

# 3. Insert documents
docs = [
    "KalamDB is a real-time database with native vector storage",
    "Apache Arrow provides efficient columnar data representation",
    "DataFusion is a query engine built on Apache Arrow"
]

for doc in docs:
    embedding = model.encode(doc)
  exec_sql(
    "INSERT INTO app.docs (text, embedding) VALUES ($1, $2)",
    [doc, embedding.tolist()],
  )

# 4. Search
query = "What is KalamDB?"
query_emb = model.encode(query)

rows = exec_sql("SELECT doc_id, text, embedding FROM app.docs")["results"][0]["rows"]
results = []
for row in rows:
    doc_emb = np.array(row['embedding'])
    score = np.dot(query_emb, doc_emb) / (np.linalg.norm(query_emb) * np.linalg.norm(doc_emb))
    results.append((row['text'], score))

results.sort(key=lambda x: x[1], reverse=True)
for text, score in results:
    print(f"{text[:50]}... - {score:.4f}")
```

---

### TypeScript SDK

Use the TypeScript SDK to execute SQL and pass embeddings as arrays of numbers.

```typescript
import { createClient, Auth } from '@kalamdb/client';

const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('user', 'pass')
});

await client.connect();

await client.query(`
  CREATE TABLE IF NOT EXISTS app.knowledge (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    text TEXT NOT NULL,
    embedding EMBEDDING(1536)
  ) WITH (TYPE = 'USER')
`);

// Insert text + embedding
await client.query(
  'INSERT INTO app.knowledge (text, embedding) VALUES ($1, $2)',
  ['KalamDB is a real-time database', embedding]
);
```

---

## Performance Optimization

### 1. Normalize Embeddings Before Storage

**Problem**: Cosine similarity requires computing norms every time.

**Solution**: Normalize embeddings to unit length before storage (enables dot product = cosine similarity).

```python
import numpy as np

def normalize(embedding):
    norm = np.linalg.norm(embedding)
    return embedding / norm if norm > 0 else embedding

# Store normalized embeddings
embedding = model.encode(text)
normalized_emb = normalize(embedding)
exec_sql("INSERT INTO app.docs (text, embedding) VALUES ($1, $2)", [text, normalized_emb.tolist()])

# Search with dot product (faster than cosine similarity)
query_emb = normalize(model.encode(query))
for row in rows:
    score = np.dot(query_emb, row['embedding'])  # No norm computation needed!
```

**Performance Gain**: ~30% faster similarity computation.

### 2. Batch Inserts for Multiple Embeddings

**Problem**: Inserting embeddings one-by-one is slow.

**Solution**: Use batch INSERT with multiple VALUES.

```python
# Batch insert 1000 documents
docs = [...]  # List of 1000 documents
embeddings = model.encode(docs)  # Generate all embeddings at once

values = ','.join([f"(${i*2+1}, ${i*2+2})" for i in range(len(docs))])
params = []
for doc, emb in zip(docs, embeddings):
    params.extend([doc, emb.tolist()])

exec_sql(f"INSERT INTO app.docs (text, embedding) VALUES {values}", params)
```

**Performance Gain**: 10-100× faster for bulk inserts.

### 3. Filter Before Similarity Computation

**Problem**: Computing similarity for all rows is expensive.

**Solution**: Use SQL WHERE clauses to filter candidates first.

```python
# Bad: Compute similarity for all 1 million rows
rows = exec_sql("SELECT * FROM app.docs")["results"][0]["rows"]

# Good: Filter by category first (reduces to 1000 rows)
rows = exec_sql("SELECT * FROM app.docs WHERE category = $1", ["tutorial"])["results"][0]["rows"]
```

**Performance Gain**: 100-1000× faster for large datasets.

### 4. Use Approximate Nearest Neighbor (ANN) Indexes

**Future**: KalamDB will support HNSW indexes for fast approximate similarity search (roadmap item).

**Current Workaround**: Use external vector databases (Pinecone, Weaviate, Milvus) for indexing, store full data in KalamDB.

---

## Best Practices

### 1. Choose Appropriate Embedding Dimension

| Model | Dimension | Use Case | Storage per 1M rows |
|-------|-----------|----------|---------------------|
| MiniLM | 384 | Fast semantic search, limited accuracy | ~1.5 GB |
| BERT-base | 768 | General NLP tasks, good accuracy | ~3.0 GB |
| OpenAI small | 1536 | Production embeddings, high quality | ~6.0 GB |
| OpenAI large | 3072 | Best quality, higher cost | ~12.0 GB |

**Recommendation**: Start with MiniLM (384) for prototyping, upgrade to BERT or OpenAI for production.

### 2. Separate Embedding Table for Large Datasets

**Problem**: Storing embeddings inline increases query latency when you don't need them.

**Solution**: Use a separate table for embeddings, join when needed.

```sql
-- Main table (frequent queries)
CREATE TABLE app.docs (
  doc_id BIGINT PRIMARY KEY,
  text TEXT NOT NULL,
  category TEXT
) WITH (TYPE = 'USER');

-- Embedding table (semantic search only)
CREATE TABLE app.doc_embeddings (
  doc_id BIGINT PRIMARY KEY,
  embedding EMBEDDING(384),
  FOREIGN KEY (doc_id) REFERENCES app.docs(doc_id)
) WITH (TYPE = 'USER');

-- Query with embeddings only when needed
SELECT d.doc_id, d.text, e.embedding 
FROM app.docs d 
JOIN app.doc_embeddings e ON d.doc_id = e.doc_id 
WHERE d.category = 'tutorial';
```

### 3. Compress Parquet Files

**Benefit**: 30-50% storage reduction with SNAPPY or ZSTD compression.

**Configuration**: Set compression in table options (default: SNAPPY).

```sql
CREATE TABLE app.docs (
  ...
  embedding EMBEDDING(384)
) WITH (TYPE = 'USER', COMPRESSION = 'ZSTD');  -- Best compression ratio
```

### 4. Monitor Embedding Quality

**Metric**: Average cosine similarity between query and top-K results.

**Expected**: >0.7 for good embeddings, >0.8 for excellent embeddings.

```python
def evaluate_search_quality(queries, expected_results, k=5):
    scores = []
    for query, expected in zip(queries, expected_results):
        results = semantic_search(query, top_k=k)
        if expected in [r['doc_id'] for r in results]:
            score = [r['score'] for r in results if r['doc_id'] == expected][0]
            scores.append(score)
    
    avg_score = sum(scores) / len(scores)
    print(f"Average similarity for top-{k}: {avg_score:.4f}")
    return avg_score
```

---

## Troubleshooting

### Issue: "Expected 384 floats, got 512"

**Cause**: Embedding dimension mismatch between table schema and model output.

**Solution**: Verify model dimension matches table definition.

```python
# Check model dimension
embedding = model.encode("test")
print(f"Model dimension: {len(embedding)}")  # Should match table EMBEDDING(dimension)

# Fix: Recreate table with correct dimension
exec_sql("DROP TABLE app.docs")
exec_sql(f"CREATE TABLE app.docs (..., embedding EMBEDDING({len(embedding)})) WITH (TYPE = 'USER')")
```

### Issue: Poor search quality (low similarity scores)

**Causes**:
1. Embeddings not normalized
2. Wrong model for use case
3. Insufficient training data

**Solutions**:
1. Normalize embeddings before storage (see Performance Optimization)
2. Try different models (BERT for general, CLIP for images, OpenAI for production)
3. Fine-tune model on domain-specific data

### Issue: Slow similarity computation

**Causes**:
1. Computing similarity for all rows
2. Not normalizing embeddings (requires norm computation)
3. Large embedding dimensions

**Solutions**:
1. Filter with SQL WHERE clauses first
2. Normalize embeddings (dot product instead of cosine similarity)
3. Use lower dimension models or PCA dimension reduction

---

## Resources

### Pre-trained Models

- **Sentence Transformers**: https://www.sbert.net/docs/pretrained_models.html
- **Hugging Face Hub**: https://huggingface.co/models?pipeline_tag=sentence-similarity
- **OpenAI Embeddings**: https://platform.openai.com/docs/guides/embeddings
- **CLIP Models**: https://github.com/openai/CLIP

### Tools & Libraries

- **Python**: sentence-transformers, transformers, torch
- **TypeScript**: openai, @tensorflow/tfjs, cohere-ai
- **Rust**: rust-bert, candle, tract

### Further Reading

- **Vector Databases**: https://www.pinecone.io/learn/vector-database/
- **Semantic Search**: https://www.elastic.co/what-is/semantic-search
- **HNSW Algorithm**: https://arxiv.org/abs/1603.09320

---

**Document Version**: 1.0  
**Last Updated**: November 3, 2025  
**Maintained by**: KalamDB Core Team
