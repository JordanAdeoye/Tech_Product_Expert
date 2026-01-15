from dotenv import load_dotenv
import os
import time
import re
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings,ChatOpenAI
from langchain_classic.prompts import PromptTemplate
from datetime import datetime
# from langchain_classic.memory import ConversationBufferMemory

"""
Online RAG retrieval module.

Responsibilities:
- Connect to the Chroma vector store via LangChain.
- Retrieve relevant YouTube transcript chunks for a user query.
- Apply time-aware re-ranking using `published_at` metadata.
- Format context with metadata (title, channel, published_at, link).
- Choose between a normal prompt and a time-aware prompt depending on the query.
- Call the LLM and return a final answer string for the UI.

This module is used by the Streamlit app as the main query interface.
"""


load_dotenv()
OPEN_API_KEY = os.getenv('OPEN_API_KEY')









"""
THIS SCRIPT HANDLES THE RETRIVAL PROCESS AND USES MY TIME-AWARE RETRIVAL LOGIC WITH THE PUBLISHED_AT KEY
"""

# --- Helpers --------------------------------------------------------

TIME_SENSITIVE_KEYWORDS = [
    "latest",
    "most recent",
    "recent",
    "as of now",
    "right now",
    "currently",
    "this year",
    "these days",
    "up to date",
    "newest",
]


def is_time_sensitive(query: str) -> bool:
    q = query.lower()

    # keyword-based check
    if any(kw in q for kw in TIME_SENSITIVE_KEYWORDS):
        return True

    # contains an explicit year like 2023, 2024, 2025, etc.
    year_matches = re.findall(r"\b(20[2-9][0-9])\b", q)
    if year_matches:
        return True

    return False


def parse_date_safe(date_str: str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None



def time_aware_sort(query: str, docs):
    """
    If the query is time-sensitive, sort docs by published_at (newest first).
    Otherwise, keep original order.
    """
    if not docs:
        return docs

    if not is_time_sensitive(query):
        # Non-time-sensitive question: keep similarity order
        return docs

    # Time-sensitive: sort by published_at descending
    def sort_key(doc):
        date_str = doc.metadata.get("published_at")
        dt = parse_date_safe(date_str)
        # Newest first: use timestamp, fallback to very old timestamp if missing
        return dt.timestamp() if dt else 0.0

    # sorted ascending by default → reverse=True for newest first
    docs_sorted = sorted(docs, key=sort_key, reverse=True)
    return docs_sorted


def format_docs_with_metadata(docs):
    """
    Turn Documents into a single context string with metadata headers.
    """
    chunks = []
    for doc in docs:
        meta = doc.metadata or {}
        published_at = meta.get("published_at", "Unknown date")
        channel = meta.get("channel_title", "Unknown channel")
        title = meta.get("video_title", "Unknown title")
        link = meta.get("video_link", "")

        header_parts = [
            f"Published At: {published_at}",
            f"Channel: {channel}",
            f"Title: {title}",
        ]
        if link:
            header_parts.append(f"Link: {link}")

        header = " | ".join(header_parts)

        chunk_text = f"[{header}]\n{doc.page_content}"
        chunks.append(chunk_text)

    return "\n\n---\n\n".join(chunks)


# --- Main RAG function ---------------------------------------------
DEFAULT_TEMPLATE = """
You are a tech product expert (phones, laptops, tablets, smartwatches, foldables, etc.).

You will be given context chunks from YouTube transcripts.
Each chunk may start with metadata such as title, channel, and published date.

Context:
{context}

Conversation so far:
{history}

Rules:
- If the question is NOT about tech products, say: "I don't know."
- If the context does not contain the answer, say: "I don't know."
- Do NOT use outside knowledge.
- You may use dates in the context to choose the most relevant information,
  but if the user does NOT ask about time, dates, or years, you generally
  should NOT mention specific dates or years in your answer. Just answer the
  question directly and concisely.
- Be concise.

Question: {question}

Answer:
"""
TIME_AWARE_TEMPLATE = """
You are a tech product expert (phones, laptops, tablets, smartwatches, foldables, etc.).

You will be given context chunks from YouTube transcripts.
Each chunk starts with metadata, including a "Published At" date (when the video was released).

Context:
{context}

Rules:
- If the question is NOT about tech products, say: "I don't know."
- If the context does not contain the answer, say: "I don't know."
- Do NOT use outside knowledge.
- Use the "Published At" dates when the user asks about time, years,
  "latest", "recent", or similar:
  - Prefer newer chunks for "latest"/"most recent"/"current" questions.
  - If older and newer chunks disagree, assume newer chunks are more up-to-date
    and say this explicitly.
  - When the question is about time (e.g., "in 2025", "over the years"),
    mention dates or years in your answer (e.g., "In a July 2025 review...").
- Be concise.

Question: {question}

Answer:
"""



# memory = ConversationBufferMemory(
#         memory_key="history",
#         return_messages=False,  # since you're using a string PromptTemplate
#     )



def query_data_rag(query, client,memory):

    

    time_sensitive = is_time_sensitive(query)

    # 2) Pick the right template
    template = TIME_AWARE_TEMPLATE if time_sensitive else DEFAULT_TEMPLATE

    prompt = PromptTemplate(
        template=template,
        input_variables=["context", "question"],
    )

    embeddings_retrival = OpenAIEmbeddings(
        model="text-embedding-3-small",
        api_key=OPEN_API_KEY,
    )

    vector_store = Chroma(
        client=client,                  # your chromadb client
        collection_name="youtube_transcripts",
        embedding_function=embeddings_retrival,
    )

    llm = ChatOpenAI(
        model="gpt-4.1-mini",
        api_key=OPEN_API_KEY,
        temperature=0.7,
    )

    # Base semantic retriever
    retriever = vector_store.as_retriever(
        search_type="similarity_score_threshold",
        search_kwargs={"k": 10, "score_threshold": 0.15},
    )

    # 1) Get semantically similar docs
    docs = retriever.invoke(query)

    # 2) Time-aware re-ranking using published_at metadata
    docs = time_aware_sort(query, docs)

    # # 3) Build context string with metadata injected
    context = format_docs_with_metadata(docs)

    # # 4) Run the LLM with our custom prompt
    # formatted_prompt = prompt.format(context=context, question=query)
    # llm_response = llm.invoke(formatted_prompt)

   

    # # return {
    # #     "answer": llm_response.content,
    # #     "source_documents": docs,
    # #     "time_sensitive": time_sensitive,
    # # }

    # for word in llm_response.content.split():
    #     yield word + " "
    #     time.sleep(0.05)

    # return llm_response.content

    history = memory.load_memory_variables({}).get("history", "")

    # 2) Inject history into prompt
    formatted_prompt = prompt.format(
        context=context,
        question=query,
        history=history,
    )

    llm_response = llm.invoke(formatted_prompt)

    # 3) Save the new turn
    memory.save_context(
        {"input": query},
        {"output": llm_response.content},
    )
    print(llm_response.content)
    # stream out answer
    # for word in llm_response.content.split():
    #     yield word + " "
    #     time.sleep(0.05)
    

    for chunk in re.split(r"(\s+)", llm_response.content):
        yield chunk
        time.sleep(0.02)


# query = "How is the battery life on the Samsung Z Fold 7?"
# query = "what phones came out recently?"
# # query = "whats are the states in nigeria"
# query_data(query,chunk_vector.client)
# print(query_data_rag(query,rag_indexing_pipeline.client))








