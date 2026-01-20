from llama_index.core import Document
from llama_index.core.node_parser import (
    SentenceSplitter,
    SemanticSplitterNodeParser,
)
from llama_index.embeddings.openai import OpenAIEmbedding
import re
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime, timezone
import chromadb




load_dotenv()
API_KEY_CHROMA =  os.getenv("API_KEY_CHROMA")
CHROMA_TENANT_ID=os.getenv("CHROMA_TENANT_ID")
CHROMA_DATABASE=os.getenv("CHROMA_DATABASE")
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(url, key)


"""
Stage 2 of the RAG data pipeline.

This script:
- Loads new raw transcripts (produced by the YouTube ingestion pipeline)
- Cleans and preprocesses transcripts
- Splits text into semantic chunks
- Embeds chunks using OpenAI
- Inserts chunks into the Chroma vector database
- Maintains indexing state to avoid reprocessing previously indexed videos

This script runs AFTER youtube_ingestion_pipeline.py.
"""





client = chromadb.CloudClient(
  api_key=API_KEY_CHROMA,
  tenant=CHROMA_TENANT_ID,
  database=CHROMA_DATABASE
)

collection = client.get_or_create_collection("youtube_transcripts")

def clean_transcript(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[.*?\]", "", text)

    text = re.sub(
        r"(sponsored by.*?\.)(\s|$)", 
        "", 
        text, 
        flags=re.IGNORECASE | re.DOTALL
    )

    text = re.sub(r"\s+", " ", text).strip()

    return text




base_splitter = SentenceSplitter(chunk_size=2000)  # you can tune size

def chunk_up(text: str):
    # First chunk the big transcript
    base_chunks = base_splitter.split_text(text)

    # Convert to documents so semantic splitter sees smaller pieces
    docs = [Document(text=c) for c in base_chunks]

    # Now apply semantic splitter safely
    nodes = splitter.get_nodes_from_documents(docs)
    return nodes



def indexing(nodes,transcript_path):
    length_node = len(nodes)
    chunked_text = [nodes[i].get_content() for i in range(length_node)]

    response = (
            supabase.table("Videos")
                .select("video_id",
                        "video_link",
                        "published_at",
                        "title",
                        "Channels(channel_title)").eq("transcript_path",transcript_path)
                .execute()
    )

    if not response.data:
        print(f"No video row found for transcript_path={transcript_path}")
        return False
    
    dict_data = response.data[0]
    # print(response.data[0])

    chunked_key =  [f"{dict_data['video_id']}_{i}" for i in range(length_node)] # used for ids in vectordb (e.g 'bMou1qUMHC4_0')
    # print(dict_data["video_id"])

    
    embeddings = [
        embed_model.get_text_embedding(text)   # or get_query_embedding(text)
        for text in chunked_text
    ]
    # building vector db
    collection.add(
        ids=chunked_key,
        documents=chunked_text,
        embeddings=embeddings,
        metadatas=[
            {
                "video_id": dict_data["video_id"],
                "channel_title": dict_data["Channels"]["channel_title"],
                "video_title": dict_data["title"],
                "published_at": dict_data["published_at"],
                "video_link": dict_data["video_link"],
                "chunk_index": idx,
                "source": "youtube",
            }
            for idx in range(length_node)
        ]
    )
    # print(collection.peek())
    return True



def chunk_and_index():
    # get all videos that have a fetched transcript but not yet indexe
    res_video = (
        supabase.table("Videos")
            .select("transcript_path").eq("is_indexed",False).eq("transcript_status","fetched")
            .execute()
    )
    if not res_video.data:
        print("no new video to index")
        return

    for row in res_video.data:
        transcript_path = row.get("transcript_path")
        if not transcript_path:
            continue
        
        print("Indexing:", transcript_path)

        # Download transcript from bucket
        res_bucket = (
            supabase.storage
            .from_("transcripts")
            .download(transcript_path)
        )

        data = res_bucket.decode("utf-8")
        # Clean + chunk        
        cleaned_transcript = clean_transcript(data)
        nodes = chunk_up(cleaned_transcript)

        # Index into Chroma
        result = indexing(nodes,transcript_path)
        
        # Mark as indexed if successful
        if result:
            indexed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            supabase.table("Videos") \
                .update({"is_indexed": True, "indexed_at":indexed_at}) \
                .eq("transcript_path", transcript_path) \
                .execute()
        else:
            print("Indexing failed for:", transcript_path)


if __name__ == "__main__":

    OPEN_API_KEY = os.getenv('OPEN_API_KEY')
    embed_model = OpenAIEmbedding(
        model="text-embedding-3-small",
        api_key=OPEN_API_KEY)

    splitter = SemanticSplitterNodeParser(
        buffer_size=1, breakpoint_percentile_threshold=95, embed_model=embed_model
    )

    chunk_and_index()


