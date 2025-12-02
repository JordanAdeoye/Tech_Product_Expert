from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import os
import json
from supadata_transcript_fetcher import raw_transcript
import time
# from langdetect import detect
from datetime import datetime, timezone
from langdetect import detect, DetectorFactory, LangDetectException
from supadata import errors as supadata_errors

DetectorFactory.seed = 0  # makes results reproducible

# UCBJycsmduvYEL83R_U4JriQ - marquees brownlee id
# UULFBJycsmduvYEL83R_U4JriQ - playlist id

# youtube channels usersnames
# youtube_channels = ["@mkbhd","@unboxtherapy","@CarterNolanMedia","@Mrwhosetheboss",
#                     "@JerryRigEverything","@austinevans","@CreatedbyEllaYT","@ShortCircuit",
#                     "@ScatterVolt","@paulshardware"]

youtube_channels = ["@austinevans","@CreatedbyEllaYT","@ShortCircuit",
                    "@ScatterVolt","@paulshardware"]



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



# youtube_channels = ["@mkbhd","@unboxtherapy","@CarterNolanMedia"]

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
    return username_id










def get_youtube_channel_playlist(id,nextpage=None):
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(
        api_service_name, api_version, developerKey=API_KEY)
    
    request = youtube.playlistItems().list(
        part="contentDetails,id,snippet,status",
        maxResults=50,
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





#  retries transcript(supadat api for the transcript) multiple times before moving on if it hits a 429 error(rate limit error) technique is called backoff
# def supadata_error_handler(url,videoid):
#     max_retries = 1 #5
#     delay = 5  # start with 5 seconds
#     for attempt in range(max_retries):
#         try:
#             raw_text = raw_transcript(url)
#             break  # success → exit loop
#         except supadata_errors.SupadataError as e:
#             if "limit-exceeded" in str(e).lower():
#                 print(f"Rate limit hit. Waiting {delay}s before retrying (attempt {attempt+1})...")
#                 time.sleep(delay)
#                 delay *= 2  # exponential backoff
#             else:
#                 raise  # rethrow if it's a different Supadata error
#     else:
#         raw_text = None  # all retries failed
#         print(f"Skipping video {videoid} after repeated 429s.")
#     return raw_text#,failed_transcript

# def supadata_error_handler(url, videoid):
#     max_retries = 5  # you can set this back to 1 if you want
#     delay = 5        # start with 5 seconds

#     for attempt in range(max_retries):
#         try:
#             raw_text = raw_transcript(url)
#             return raw_text  # success → exit immediately
#         except supadata_errors.SupadataError as e:
#             msg = str(e).lower()

#             # 1) Rate limit → retry with backoff
#             if "limit-exceeded" in msg:
#                 print(
#                     f"[Supadata] Rate limit hit for {videoid}. "
#                     f"Waiting {delay}s before retrying (attempt {attempt+1}/{max_retries})..."
#                 )
#                 time.sleep(delay)
#                 delay *= 2
#                 continue  # try again

#             # 2) Transcript unavailable → normal case, just skip
#             if "transcript-unavailable" in msg:
#                 print(
#                     f"[Supadata] Transcript unavailable for video {videoid}. "
#                     f"Skipping this video."
#                 )
#                 return None

#             # 3) Any other error → log + skip
#             print(f"[Supadata] Error for video {videoid}: {e}. Skipping this video.")
#             return None

#     # If we exhausted all retries due to rate limit and still failed:
#     print(f"[Supadata] Skipping video {videoid} after repeated rate-limit errors.")
#     return None


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






def store_data():
    folder_path = "/Users/adeoyedipo/Projects/Tech_Product_Expert/data"

    run_id = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
    for handle in youtube_channels: # per channel

        channel_folder = os.path.join(folder_path, handle) # each youtube channel e.g mkhbd,ksi
        raw_dir       = os.path.join(channel_folder, "raw")
        raw_text_dir  = os.path.join(raw_dir, "raw_text")

        
        os.makedirs(channel_folder, exist_ok=True) # folder for each channel
        os.makedirs(raw_dir,exist_ok=True) # json files here
        os.makedirs(raw_text_dir,exist_ok=True) # stores raw transcript here in this folder as txt
        username_id = get_youtube_channel_id(handle)

        recent_video_ids = []
        latest_new_published_at = None

        file_list = [
            f for f in os.listdir(raw_dir)
            if os.path.isfile(os.path.join(raw_dir, f))
                and f.endswith(".json")
                and f != ".DS_Store"
        ]


        # initial_videos = 0
        # existing_file = {os.path.splitext(f)[0].split("_")[-1] for f in file_list} # contains video id for files stored
        existing_file = existing_file = {os.path.splitext(f)[0][-11:] for f in file_list}
        initial_videos = len(existing_file) # helps in containing the number of new videos added on each run



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
                
                # if the first video the api returns exist among the existing videos, break because that means they are no new video uploaded yet
                if video_id in existing_file:
                    print("Hit existing item, stopping:", handle, date_part, video_id)
                    stop = True
                    break


                time.sleep(2)
                url = video_starter_link+items["contentDetails"]["videoId"]

                raw_text = supadata_error_handler(url,items["contentDetails"]["videoId"])
                
                if raw_text is None:
                    failed_transcript+=1
                


                # save the raw transcript text to txt file in data/{channel}/raw/raw_text
                txt_filename = f"{date_part}_{safe_channel}_{video_id}.txt" # 2025-10-30_Marques_Brownlee_rU9aqBv0YdY
                json_filename = f"{date_part}_{safe_channel}_{video_id}.json" # 2025-10-30_Marques_Brownlee_rU9aqBv0YdY
                

                now_utc = datetime.now(timezone.utc) # time transcript was fetched 

                
                if raw_text: # if raw_text(transcript) is all good
                    # write transcript file
                    txt_path_abs = os.path.join(raw_text_dir, txt_filename) # txt file for the transcript to be saved
                    with open(txt_path_abs, "w", encoding="utf-8") as fh:
                        fh.write(raw_text)

                    status = "fetched"
                    transcript_source = "supadata"
                    transcript_fetched_at = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
                    file_path_raw = os.path.join("raw", "raw_text", txt_filename)  # store RELATIVE path in JSON

                else: #if raw_text fails for any reason
                    status = "unavailable"      # or "unavailable" if you won't retry
                    transcript_source = "unavailable"
                    transcript_fetched_at = None
                    file_path_raw = None

                video_info = {
                    "channel_title": channel_title,
                    "channel_id": username_id,
                    "language": safe_detect_language(raw_text) if raw_text else None,
                    "video_title": items["snippet"]["title"],
                    "description": items["snippet"]["description"],
                    "published_at": published_at,
                    "video_id": video_id,
                    "transcript_source": transcript_source,
                    "transcript_fetched_at": transcript_fetched_at,
                    "status": status,
                    "run_id": run_id,
                    "video_link": video_starter_link + items["contentDetails"]["videoId"],
                    "file_path_raw": file_path_raw,   # prefer this key over embedding text
                }
                # print("\n")
                # print(video_info["raw_transcript"]) 
                

                json_path_abs = os.path.join(channel_folder, "raw", json_filename) # json files for the json(video_info) to be saved
                with open(json_path_abs, "w", encoding="utf-8") as f:
                    json.dump(video_info, f, ensure_ascii=False, indent=2)
                
                # initial_videos = len(existing_file)
                # after successfully writing JSON:
                existing_file.add(video_id)
                
                recent_video_ids.append(f"{date_part}_{safe_channel}_{video_id}") # for state.json
                if latest_new_published_at is None:
                    latest_new_published_at = published_at
                
            if stop: # if the video already exist in storage
                break    
            
            nextpagevalue = response.get("nextPageToken")
            if not nextpagevalue:
                print("No more pages available.",handle)
                break
        


        new_videos_pulled = len(existing_file) - initial_videos # how many new videos were pulled for a particular channel
        print(
            f"Channel {handle}: {new_videos_pulled} new videos downloaded, "
             f"{failed_transcript} transcripts unavailable."
        )

        # state.json logic (insight for every new run for each channel)
        state_now_utc = datetime.now(timezone.utc)

        if recent_video_ids == []:
            latest_new_published_at = None
        else:
            latest_new_published_at = latest_new_published_at
            # latest_date_part = latest_new_published_at[:10]

        # this work with the state_json["indexed_video_ids"] so it doesnt overwrite the state.json file and we can keep a tracked record of all the "indexed videos while also update recently added videos
        state_path = os.path.join(channel_folder, "state.json")

        # 1. Load existing state (MERGE behavior)
        if os.path.exists(state_path):
            with open(state_path, "r", encoding="utf-8") as f:
                state_json = json.load(f)
        else:
            state_json = {}

        # 2. Update only the keys store_data controls
        state_json["last_checked_at"] = state_now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        state_json["latest_video_published_at"] = latest_new_published_at
        state_json["recent_video_ids"] = recent_video_ids

        # 3. Write back to state.json
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(state_json, f, ensure_ascii=False, indent=2)




store_data()
        
# i will like to use the txt folder to check for new files cause if a transcript fails it does have a txt file
# cause if it fails even with the backoff handling i will still want to try it again , i might have to include failed transcript in state.json so it can be tried again

# if it failed to download transcript dont put in recent_video_ids

# also need to fix how you are doing indexing, add processed id to state.json



