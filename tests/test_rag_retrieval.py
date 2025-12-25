from datetime import datetime
# from types import SimpleNamespace

# import rag_retrieval 

from rag_retrieval import is_time_sensitive,parse_date_safe,time_aware_sort,format_docs_with_metadata,query_data_rag
from unittest.mock import MagicMock,patch


def test_is_time_sensitive_keywords():
    assert is_time_sensitive("What is the latest iPhone?")
    assert is_time_sensitive("Best phones in 2025")
    assert not is_time_sensitive("What is RAM?")



def test_parse_date_safe():
    dt = parse_date_safe("2025-01-01T12:00:00Z")
    assert isinstance(dt, datetime)

    assert parse_date_safe("bad-date") is None
    assert parse_date_safe(None) is None


def test_time_aware_sort_newest_first():
    doc_old = MagicMock()
    doc_old.metadata = {"published_at": "2023-01-01T00:00:00Z"}

    doc_new = MagicMock()
    doc_new.metadata = {"published_at": "2025-01-01T00:00:00Z"}

    docs = [doc_old, doc_new]

    sorted_docs = time_aware_sort("latest phone", docs)

    assert sorted_docs[0] == doc_new


def test_format_docs_with_metadata():
    doc = MagicMock()
    doc.page_content = "This is a transcript chunk."
    doc.metadata = {
        "published_at": "2025-01-01",
        "channel_title": "Unbox Therapy",
        "video_title": "Best Phone",
        "video_link": "https://youtube.com/watch?v=abc",
    }

    result = format_docs_with_metadata([doc])

    assert "Unbox Therapy" in result
    assert "Best Phone" in result
    assert "This is a transcript chunk." in result




@patch("rag_retrieval.time.sleep")
@patch("rag_retrieval.ChatOpenAI")
@patch("rag_retrieval.Chroma")
@patch("rag_retrieval.OpenAIEmbeddings")
def test_query_data_rag_streams_output(
    mock_embed,
    mock_chroma,
    mock_llm,
    mock_sleep,
):
    # Fake retriever result
    fake_doc = MagicMock()
    fake_doc.page_content = "Test content"
    fake_doc.metadata = {}

    fake_retriever = MagicMock()
    fake_retriever.invoke.return_value = [fake_doc]

    fake_vector_store = MagicMock()
    fake_vector_store.as_retriever.return_value = fake_retriever
    mock_chroma.return_value = fake_vector_store

    # Fake LLM response
    fake_response = MagicMock()
    fake_response.content = "Hello world"

    mock_llm.return_value.invoke.return_value = fake_response

    client = MagicMock()

    result = list(query_data_rag("What is a phone?", client))

    assert "Hello" in "".join(result)

