from llama_index.core import Document
from llama_index.core.node_parser import (
    SentenceSplitter,
    SemanticSplitterNodeParser,
)
from llama_index.embeddings.openai import OpenAIEmbedding
import json
import tiktoken
import re
import os
from dotenv import load_dotenv
# import download_transcript
# import manifest


load_dotenv()

import chromadb
client = chromadb.PersistentClient("./chroma_data")

collection = client.get_or_create_collection(name="youtube_transcripts")

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


# OPEN_API_KEY = os.getenv('OPEN_API_KEY')
# embed_model = OpenAIEmbedding(
#     model="text-embedding-3-small",
#     api_key=OPEN_API_KEY)

# splitter = SemanticSplitterNodeParser(
#     buffer_size=1, breakpoint_percentile_threshold=95, embed_model=embed_model
# )

def chunk_up(text: str):

    doc = Document(text=text)
    nodes = splitter.get_nodes_from_documents([doc])
    return nodes



def indexing(nodes,filenames):
    length_node = len(nodes)
    chunked_text = [nodes[i].get_content() for i in range(length_node)]
    with open(filenames,"r",encoding="utf-8") as f:
        dict_data = json.load(f)

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
                "channel_title": dict_data["channel_title"],
                "video_title": dict_data["video_title"],
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
    txt_file_ext = ".txt"
    json_file_ext = ".json"
    youtube_channels = ["@mkbhd","@unboxtherapy"]
    raw = "raw"
    raw_text = "raw/raw_text"
    data_folder = "./data"

    for channel in youtube_channels:
        path = os.path.join(data_folder,channel,'state.json')

        # print(path)

        with open(path, "r", encoding="utf-8") as f:
                state = json.load(f)
        
        # make sure the key exists
        if "indexed_video_ids" not in state:
            state["indexed_video_ids"] = []

        print(state["recent_video_ids"])

        if state["recent_video_ids"]:   

            # Track videos indexed in THIS run
            indexed_this_run = []

            
            for i in state["recent_video_ids"]:
                if i in state["indexed_video_ids"]: # in this case if for any reason store_data() and index.py ever get out of sync, it’s nice to defensively skip already-indexed videos
                    continue
                path_transcript = os.path.join(data_folder, channel, "raw", "raw_text", i + ".txt")

                with open(path_transcript,"r", encoding="utf-8") as f:
                    data = f.read()
                
                cleaned_transcript = clean_transcript(data)
                nodes = chunk_up(cleaned_transcript)
                # indexing 
                filename_json = os.path.join(data_folder, channel, "raw" ,i+".json") # get the json data for the metadata in your vectordb "./data/@mkbhd/raw/2025-07-09_Marques_Brownlee_bMou1qUMHC4.json"
                result = indexing(nodes,filename_json)

                indexed_this_run.append(i)

            # Append all at once
            for vid in indexed_this_run:
                if vid not in state["indexed_video_ids"]:
                    state["indexed_video_ids"].append(vid)

            # Write state.json ONCE per channel
            with open(path, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)

        else:
            print("no new video to index")


if __name__ == "__main__":

    OPEN_API_KEY = os.getenv('OPEN_API_KEY')
    embed_model = OpenAIEmbedding(
        model="text-embedding-3-small",
        api_key=OPEN_API_KEY)

    splitter = SemanticSplitterNodeParser(
        buffer_size=1, breakpoint_percentile_threshold=95, embed_model=embed_model
    )

    chunk_and_index()

# from langchain_chroma import Chroma
# from langchain_openai import OpenAIEmbeddings,ChatOpenAI

# from langchain_classic.chains import RetrievalQA,create_retrieval_chain

# embeddings_retrival = OpenAIEmbeddings(model="text-embedding-3-small")


# vector_store = Chroma(
#     client=client,                  # your chromadb client
#     collection_name="youtube_transcripts",
#     embedding_function=embeddings_retrival,
#     persist_directory="./chroma_data"
# )


# def query_data(query):
    
#     llm = ChatOpenAI(model="gpt-4.1-mini",api_key=OPEN_API_KEY,temperature=0.2)
#     retriever = vector_store.as_retriever()
#     retrievalQA = RetrievalQA.from_llm(llm=llm, retriever=retriever,return_source_documents=True,)
#     response = retrievalQA.invoke(query)
#     print(response)

# query = "How is the battery life on the Samsung Z Fold 7?"
# query_data(query)

