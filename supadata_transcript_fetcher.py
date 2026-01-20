from supadata import Supadata
import os
from dotenv import load_dotenv
load_dotenv()

"""
Utility module for fetching YouTube transcripts from Supadata.

Responsibility:
- Given a YouTube video URL, request transcript text using Supadata API.
- Return transcript text as a string.
- Used by the ingestion pipeline to get raw transcripts.
"""
SUPADATA_KEY = os.getenv('SUPADATA_KEY')
def raw_transcript(url):
    supadata = Supadata(api_key=SUPADATA_KEY)

    # Get transcript or job ID
    transcript_result = supadata.transcript(
        url=url,
        lang="en",
        text=True,
        mode="native"  # 'native', 'auto', or 'generate'
    )
    return (transcript_result.content)










