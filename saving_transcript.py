# from download_transcript import get_youtube_channel_id 

# result  = get_youtube_channel_id("@mkbhd")
# print(result)



from supadata import Supadata
import os
# def raw_transcript(url):
#     supadata = Supadata(api_key="sd_d8d43ca9779739cffe549fe5cb724e97")

#     # Get transcript or job ID
#     transcript_result = supadata.transcript(
#         url=url,
#         lang="en",
#         text=True,
#         mode="native"  # 'native', 'auto', or 'generate'
#     )
#     return (f"Got transcript result or job ID: {transcript_result}")

#     if hasattr(transcript_result, 'job_id'):
#         # Check job result
#         result = supadata.transcript.get_job_status(transcript_result.job_id)
#         return f"Job status: {result.status}"
#         if result.status == "completed":
#             return result.content


# print(raw_transcript("https://www.youtube.com/watch?v=w5k72A30kUc"))

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








