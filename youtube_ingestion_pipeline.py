from googleapiclient.discovery import build
#from google_auth_oauthlib.flow import InstalledAppFlow 1
#from google.auth.transport.requests import Request 2
#from io import BytesIO 3

from dotenv import load_dotenv
import os
#import json 4
from supadata_transcript_fetcher import raw_transcript
import time
# from langdetect import detect
from datetime import datetime, timezone
from langdetect import detect, DetectorFactory, LangDetectException
from supadata import errors as supadata_errors
import requests 

from supabase import create_client, Client
import logging
logger = logging.getLogger(__name__)

# url: str = os.getenv("SUPABASE_URL")
# key: str = os.getenv("SUPABASE_KEY")

# supabase: Client = create_client(url, key)

DetectorFactory.seed = 0  # makes results reproducible


# youtube channels usersnames
# youtube_channels = ["@mkbhd","@unboxtherapy","@CarterNolanMedia","@Mrwhosetheboss",
#                     "@JerryRigEverything","@austinevans","@CreatedbyEllaYT","@ShortCircuit",
#                     "@ScatterVolt","@paulshardware"]

youtube_channels = ["@mkbhd","@unboxtherapy"]



""" THIS IS THE MAIN PIPELINE OPF MY RAG THIS USING THE YOUTUBE API TO TO GET INFORMATION ON YOUTUBE VIDEOS 
CREATES A JSON(FOR DATA ON EACH VIDEO) AND TEXT(TO STORE TRASCRIPT OF EACH VIDEO)

IT CONTAINS THE LOGIC ON HOW TO STORES DATA AND  AVOID DUPLICATES AND ALSO ANYTIME IT RUNS IT CHECKS FOR NEW VIDEOS

IT ALSO CREATES A STATE.JSON TO INFORM ADMIN ABOUT THE NEW VIDEOS ADDED ON EACH RUN OR IF THERE WAS NO NEW VIDEO TO ADD

IT ALSO INFORMS ADMIN ON THE EACH FILES THAT HAS BEEN CLEANED/PROCESSED AND ADDED TO THE VECTORDB

AND IT ALSO USE "SAVING_TRANSCRIPT.PY" TO USE THE API OF SUPADAT TO DOWNKOADF TRANSCRIPTS FOR VIDEOS
"""

"""
Stage 1 of the RAG data pipeline.

This script:
- Uses YouTube + Supadata to discover new videos
- Stores raw transcripts and metadata locally
- Maintains state.json to avoid duplicates and track new videos
- Saves one JSON per video and one transcript text file
- Writes only new videos on each run

This script runs BEFORE rag_indexing_pipeline.py.
"""


load_dotenv()

video_starter_link = "https://www.youtube.com/watch?v="

API_KEY = os.getenv('API_KEY')

def get_youtube_channel_id(handle):

    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(
        api_service_name, api_version, developerKey=API_KEY)
    
    request = youtube.channels().list(
        part="contentDetails,contentOwnerDetails,id,snippet,contentDetails,statistics",
        forHandle=handle
    )
    response = request.execute()

    # uploads = response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"] # UUBJycsmduvYEL83R_U4JriQ
    username_id = response['items'][0]['id']
    title = response['items'][0]["snippet"]['title']
    return username_id,title





def get_youtube_channel_playlist(id,nextpage=None):
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(
        api_service_name, api_version, developerKey=API_KEY)
    
    request = youtube.playlistItems().list(
        part="contentDetails,id,snippet,status",
        maxResults=10,
        pageToken = nextpage,
        playlistId="UULF"+id[2:] # allows api to returns all videos except shorts
    )
    response = request.execute()

    
    return response

"""after creating the folders if , say there is something inside you dont want rewrite the whole folder, do you want to use the date to check 
if therer are any recently uploaded videos 

since the files name will be date_channel_name_videoid

i guess i can use the date to check if there is any new video uploaded to the youtube channel recently
"""


# get_youtube_channel_playlist(username_id,nextpage="EAAaHlBUOkNHUWlFRUZCUVRVMFJETTFSakJHTWpoRlFqaw")



# detects what languages the transcripts is in
def safe_detect_language(text):
    """Return detected language code (e.g., 'en') or None if detection fails."""
    if not text or len(text.strip()) < 20:  # skip very short or empty transcripts
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None





def supadata_error_handler(url, videoid):
    max_retries = 5
    delay = 5  # start with 5 seconds

    for attempt in range(max_retries):
        try:
            raw_text = raw_transcript(url)
            return raw_text  # success → exit immediately

        except supadata_errors.SupadataError as e:
            msg = str(e).lower()

            # 1) Rate limit → retry with backoff
            if "limit-exceeded" in msg:
                print(
                    f"[Supadata] Rate limit hit for {videoid}. "
                    f"Waiting {delay}s before retrying (attempt {attempt+1}/{max_retries})..."
                )
                time.sleep(delay)
                delay *= 2
                continue  # try again

            # 2) Transcript unavailable → normal "no transcript" case, don't crash
            if "transcript-unavailable" in msg:
                print(
                    f"[Supadata] Transcript unavailable for video {videoid}. "
                    f"Skipping this video."
                )
                return None

            # 3) Any other Supadata error → log + skip this video
            print(f"[Supadata] Error for video {videoid}: {e}. Skipping this video.")
            return None

        except requests.exceptions.HTTPError as e:
            # Handle 5xx server errors from Supadata (like your 502)
            status = e.response.status_code if e.response is not None else None

            if status is not None and 500 <= status < 600:
                # Treat as temporary → retry with backoff
                print(
                    f"[Supadata] HTTP {status} from API for {videoid}. "
                    f"Waiting {delay}s before retrying (attempt {attempt+1}/{max_retries})..."
                )
                time.sleep(delay)
                delay *= 2
                continue  # try again

            # 4xx or unknown: log + skip
            print(f"[Supadata] HTTP error for video {videoid}: {e}. Skipping this video.")
            return None

        except Exception as e:
            # Last-resort catch-all so a weird error on one video doesn't kill the run
            print(f"[Supadata] Unexpected error for video {videoid}: {e}. Skipping this video.")
            return None

    # If we exhausted all retries due to rate limit or 5xx
    print(f"[Supadata] Skipping video {videoid} after repeated errors.")
    return None





def supabase()-> Client | None:
    """
    Create and return Supabase client from environment variables.

    Returns:
        Supabase client if credentials are available, None otherwise
    """
     
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        logger.warning("Supabase credentials not found in environment variables")
        return None

    return create_client(url, key)








SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def upload_transcript_bytes(transcript_path: str, raw_text: str) -> str:
    """
    Upload transcript text directly to Supabase Storage (bucket: transcripts)
    using raw bytes, no local files.

    transcript_path: e.g. "UCBJycsmduvYEL83R_U4JriQ/raw/yWBz2qZJ8zY.txt"
    """
    if SUPABASE_URL is None or SUPABASE_KEY is None:
        raise RuntimeError("SUPABASE_URL or SUPABASE_KEY not set")

    url = f"{SUPABASE_URL}/storage/v1/object/transcripts/{transcript_path}"

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "apikey": SUPABASE_KEY,
            "Content-Type": "text/plain; charset=utf-8",
            "x-upsert": "false",  # don't overwrite existing transcripts
        },
        data=raw_text.encode("utf-8"),
        timeout=30,
    )
    # Handle "already exists" gracefully
    if resp.status_code == 409:
        return "exists"
    # because supabase returns a 400 error instead of a 409 error when an object already exist
    if resp.status_code == 400:
        body = (resp.text or "").lower()
        # match common “already exists” signals
        if "already exists" in body or "asset already exists" in body:
            return "exists"

    resp.raise_for_status()
    return "uploaded"
    




def store_data():
    sb = supabase()
    video_rows = []
    logs = []
    run_id = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    for handle in youtube_channels: # per channel

        
        username_id,title = get_youtube_channel_id(handle)

        # load youtube channel data into supabase database (Channel table)
        res_channel = (
        sb.table("Channels")
        .upsert([{"youtube_channel_id": username_id,"handle":handle,"channel_title":title}],
            on_conflict="youtube_channel_id",
            default_to_null=True)
        .execute()
        )

        previous = res_channel.data[0].get("latest_video_published_at")
        latest_new_published_at = previous  # fallback to existing value


        # get the pk for each row(youtube channle)
        channel_uuid = res_channel.data[0]["id"]
        # check for existing videos in the Video table filter by pk as it is fk in the videos 
        rows = sb.table("Videos").select("video_id").eq("channel_id", channel_uuid).execute().data

        existing_file = {row["video_id"] for row in rows}
        initial_videos = len(existing_file)


        # writing next page logic
        failed_transcript = 0
        nextpagevalue = None
        stop = False
        max_pages = 2
        for page in range(max_pages):
            response = get_youtube_channel_playlist(username_id,nextpagevalue)
            
            items_list = response.get("items", []) # returns an empty list if there no "items" key in the api return, this avoids keyerror exception
            for items in items_list: 

                published_at = items["snippet"]["publishedAt"]
                date_part = published_at[:10]
                channel_title = items["snippet"]["channelTitle"]
                safe_channel = channel_title.replace(" ", "_")
                video_id = items["contentDetails"]["videoId"]
                
                # Stop paging once we reach videos at or before the latest publish time
                # already recorded in the database 
                if previous is not None and published_at <= previous:
                    print("Hit existing item, stopping:", handle, date_part, video_id)
                    stop = True
                    break

                # Skip videos that are already present in the database(deduplication) "will be rarely check/ just a fail safe" in case youtube change the order of the GET, right now it is newest to oldest
                if video_id in existing_file:
                    continue



                time.sleep(2)
                url = video_starter_link+items["contentDetails"]["videoId"]

                raw_text = supadata_error_handler(url,items["contentDetails"]["videoId"])
                
                if raw_text is None:
                    failed_transcript+=1
                


        #         # save the raw transcript text to txt file in data/{channel}/raw/raw_text
                txt_filename = f"{date_part}_{safe_channel}_{video_id}.txt" # 2025-10-30_Marques_Brownlee_rU9aqBv0YdY
                

                now_utc = datetime.now(timezone.utc) # time transcript was fetched 
                # handle = handle.lstrip("@")
                transcript_path = f"{handle}/{txt_filename}" # store bucket path to text file
                # transcript_path = f"{username_id}/raw/{video_id}.txt"

                if raw_text: # if raw_text(transcript) is all good
                    try:
                        upload_transcript_bytes(transcript_path, raw_text)

                        status = "fetched" 
                        transcript_source = "supadata" # dont hard code this (if i use the function then)
                        transcript_fetched_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                    except requests.exceptions.RequestException as e:

                        # Covers HTTPError, Timeout, ConnectionError, etc.
                        print(f"[Storage] Upload failed for {video_id}: {e}")

                        status = "upload_failed"
                        transcript_source = "supadata"
                        transcript_fetched_at = None
                        transcript_path = None
                      

                else: #if raw_text fails for any reason
                    status = "unavailable"      # or "unavailable" if you won't retry
                    transcript_source = "unavailable"
                    transcript_fetched_at = None
                    transcript_path = None

                video_info = {
                    "channel_id":channel_uuid,
                    "language": "en" if raw_text else None,#
                    "title": items["snippet"]["title"],
                    "description": items["snippet"]["description"], #
                    "published_at": published_at,#
                    "video_id": video_id, #
                    "transcript_source": transcript_source,#
                    "transcript_fetched_at": transcript_fetched_at, #
                    "transcript_status": status,
                    "video_link": video_starter_link + items["contentDetails"]["videoId"],#
                    "transcript_path": transcript_path,   # prefer this key over embedding text
                    "is_indexed": False
                }
                ingested_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                
                log_info = {
                    "channel_id":channel_uuid,
                    "video_id": video_id, #
                    "ingested_at": ingested_utc,
                    "run_id": run_id
                }
        #         # print("\n")
        #         # print(video_info["raw_transcript"]) 
                video_rows.append(video_info)

                logs.append(log_info)
                

                existing_file.add(video_id)
                # updates latest_new_published_at in channel table
                if latest_new_published_at is None or published_at > latest_new_published_at:
                    latest_new_published_at = published_at


            # if video already exist no need to check next page condition    
            if stop: # if the video already exist in storage
                break   

            # handle next page logic
            nextpagevalue = response.get("nextPageToken")
            if not nextpagevalue:
                print("No more pages available.",handle)
                break
        


        new_videos_pulled = len(existing_file) - initial_videos # how many new videos were pulled for a particular channel
        print(
            f"Channel {handle}: {new_videos_pulled} new videos downloaded, "
             f"{failed_transcript} transcripts unavailable."
        )

        #insight for every new run for each channel
        state_now_utc = datetime.now(timezone.utc)
        last_checked_at = state_now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # print(video_rows)

        # Channels
        res_channel = (
            sb.table("Channels")
            .upsert([{"youtube_channel_id": username_id,"handle":handle,"channel_title":title,
                      "latest_video_published_at":latest_new_published_at,"last_checked_at":last_checked_at}],
                on_conflict="youtube_channel_id",
                default_to_null=True)
            .execute()
            )
        
    # bulk upload of videos data to the video table  
    if video_rows:   
        sb.table("Videos").upsert(video_rows,
                            on_conflict="video_id",
                        default_to_null=True).execute()

# logs
    if logs:
        sb.table("Logs").upsert(logs,
                            on_conflict="video_id",
                            ignore_duplicates=True,
                        default_to_null=True).execute()
                    
    

if __name__ == "__main__":
    store_data()
        


