from supadata import Supadata
import os


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








