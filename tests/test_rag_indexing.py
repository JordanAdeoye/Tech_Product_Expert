import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


def _import_module_safely():
    """
    Your rag_indexing_pipeline.py creates Supabase + Chroma clients at import time.
    This imports the module with those constructors patched so tests don't hit the network.
    """
    with patch("rag_indexing_pipeline.create_client") as mock_create_client, patch(
        "rag_indexing_pipeline.chromadb.CloudClient"
    ) as mock_cloud_client:
        # fake supabase client
        fake_sb = MagicMock()
        mock_create_client.return_value = fake_sb

        # fake chroma client -> collection
        fake_collection = MagicMock()
        fake_client = MagicMock()
        fake_client.get_or_create_collection.return_value = fake_collection
        mock_cloud_client.return_value = fake_client

        import rag_indexing_pipeline
        importlib.reload(rag_indexing_pipeline)

        return rag_indexing_pipeline


def test_clean_transcript_removes_brackets_and_sponsored_and_whitespace():
    m = _import_module_safely()

    raw = "Hello [Music]   sponsored by ACME.  \n\nThis   is   a test.   "
    cleaned = m.clean_transcript(raw)

    assert "[Music]" not in cleaned
    assert "sponsored by" not in cleaned.lower()
    assert cleaned == "Hello This is a test."


def test_indexing_adds_chunks_to_collection_with_metadata():
    m = _import_module_safely()

    # ---- arrange: nodes ----
    node0 = MagicMock()
    node1 = MagicMock()
    node0.get_content.return_value = "chunk 0 text"
    node1.get_content.return_value = "chunk 1 text"
    nodes = [node0, node1]

    transcript_path = "unboxtherapy/2025-12-14_Unbox_Therapy_L2fBJCDEEJk.txt"

    # ---- arrange: supabase query for video metadata ----
    # response.data[0] is used, with nested Channels dict
    video_row = {
        "video_id": "L2fBJCDEEJk",
        "video_link": "https://www.youtube.com/watch?v=L2fBJCDEEJk",
        "published_at": "2025-12-14T17:29:17Z",
        "title": "The World’s Thinnest Paper Tablet",
        "Channels": {"channel_title": "Unbox Therapy"},
    }

    sb = MagicMock()
    tbl = MagicMock()
    sb.table.return_value = tbl
    tbl.select.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=[video_row])

    # inject mocked globals into module
    m.supabase = sb

    # mock embed model
    embed = MagicMock()
    embed.get_text_embedding.side_effect = [[0.1, 0.2], [0.3, 0.4]]
    m.embed_model = embed

    # mock collection
    coll = MagicMock()
    m.collection = coll

    # ---- act ----
    ok = m.indexing(nodes, transcript_path)

    # ---- assert ----
    assert ok is True

    # embeddings called for each chunk
    assert embed.get_text_embedding.call_count == 2

    # collection.add called once with expected ids/documents/metadata
    assert coll.add.called
    kwargs = coll.add.call_args.kwargs

    assert kwargs["ids"] == ["L2fBJCDEEJk_0", "L2fBJCDEEJk_1"]
    assert kwargs["documents"] == ["chunk 0 text", "chunk 1 text"]
    assert kwargs["embeddings"] == [[0.1, 0.2], [0.3, 0.4]]

    metadatas = kwargs["metadatas"]
    assert len(metadatas) == 2
    assert metadatas[0]["video_id"] == "L2fBJCDEEJk"
    assert metadatas[0]["channel_title"] == "Unbox Therapy"
    assert metadatas[0]["video_title"] == "The World’s Thinnest Paper Tablet"
    assert metadatas[0]["chunk_index"] == 0
    assert metadatas[1]["chunk_index"] == 1


def test_chunk_and_index_happy_path_marks_video_indexed():
    m = _import_module_safely()

    # ---- arrange: supabase Videos select for unindexed fetched transcripts ----
    sb = MagicMock()
    videos_tbl = MagicMock()
    sb.table.return_value = videos_tbl

    # first query: select transcript_path where is_indexed=False and transcript_status='fetched'
    videos_tbl.select.return_value.eq.return_value.eq.return_value.execute.return_value = SimpleNamespace(
        data=[{"transcript_path": "unboxtherapy/2025-12-14_Unbox_Therapy_L2fBJCDEEJk.txt"}]
    )

    # storage download returns bytes
    storage = MagicMock()
    bucket = MagicMock()
    storage.from_.return_value = bucket
    bucket.download.return_value = b"Hello [Music] sponsored by ACME. This is a test."
    sb.storage = storage

    # update chain for marking indexed
    # update_q = MagicMock()
    videos_tbl.update.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=[])
    # (videos_tbl.update(...) returns query builder; we don't need more than that)

    m.supabase = sb

    # chunk_up returns nodes
    node0 = MagicMock()
    node0.get_content.return_value = "chunk 0"
    m.chunk_up = MagicMock(return_value=[node0])

    # indexing succeeds
    m.indexing = MagicMock(return_value=True)

    # ---- act ----
    m.chunk_and_index()

    # ---- assert ----
    m.chunk_up.assert_called_once()  # called with cleaned transcript
    m.indexing.assert_called_once()

    # update called to mark video indexed
    assert videos_tbl.update.called
    update_payload = videos_tbl.update.call_args[0][0]
    assert update_payload["is_indexed"] is True
    assert "indexed_at" in update_payload
