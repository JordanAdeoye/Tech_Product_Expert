from supadata import Supadata
import os
from dotenv import load_dotenv
load_dotenv()

"""
THIS SCRIPTS USES SUPADATA API TO THE THE TRANSCRIPTS
"""
# supadata_transcript_fetcher.py
#saving_transcript.py
"""
Utility module for fetching YouTube transcripts from Supadata.

Responsibility:
- Given a YouTube video URL, request transcript text using Supadata API.
- Return transcript text as a string.
- This file does NOT handle storage, cleaning, chunking, or indexing.
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

    if hasattr(transcript_result, 'job_id'):
        # Check job result
        result = supadata.transcript.get_job_status(transcript_result.job_id)
        return f"Job status: {result.status}"
        if result.status == "completed":
            return result.content








