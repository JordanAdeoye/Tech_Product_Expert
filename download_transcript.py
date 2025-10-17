from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import os

load_dotenv()
video_starter_link = "https://www.youtube.com/watch?v="

API_KEY = os.getenv('API_KEY')

def get_youtube_channel_id(username):

    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(
        api_service_name, api_version, developerKey=API_KEY)
    
    request = youtube.channels().list(
        part="contentDetails,contentOwnerDetails,id,snippet,contentDetails,statistics",
        forUsername=username
    )
    response = request.execute()

    return response

result = get_youtube_channel_id("marquesbrownlee")

# print(result)
print("\n")
uploads = result['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
print("\n")
# print(result['items'][0]['id'])

username_id = result['items'][0]['id']





def get_youtube_channel_playlist(id):
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(
        api_service_name, api_version, developerKey=API_KEY)
    
    request = youtube.playlistItems().list(
        part="contentDetails,id,snippet,status",
        maxResults=50,
        playlistId="UULF"+id[2:]
    )
    response = request.execute()

    for i in range(len(response["items"])):
        items = response["items"][i]

    # print("length of list ",len(response["items"]))
        video_info = {"title": items["snippet"]["title"],
                    "description": items["snippet"]["description"],
                    "publishedAt": items["snippet"]["publishedAt"],
                    "videoId": items["contentDetails"]["videoId"],
                    "videoLink":video_starter_link+items["contentDetails"]["videoId"]}
        print("\n")
        print(video_info)

    # return video_info

get_youtube_channel_playlist(uploads)

# def get_youtube_channel_videos(id):

#     api_service_name = "youtube"
#     api_version = "v3"
    
#     youtube = build(
#         api_service_name, api_version, developerKey=API_KEY)
    
#     # request = youtube.search().list(
#     #         part="snippet",
#     #         channelId=id,
#     #         channelType="any",
#     #         eventType="none",
#     #         maxResults=50,
#     #         order="date",
#     #         safeSearch="none",
#     #         type="video",
#     #         videoCaption="any",
#     #         videoDefinition="any",
#     #         videoDimension="any",
#     #         videoDuration="medium",
#     #         videoEmbeddable="videoEmbeddableUnspecified",
#     #         videoLicense="any",
#     #         videoPaidProductPlacement="any",
#     #         videoSyndicated="any",
#     #         videoType="any"
#     #     )
    
#     request = youtube.search().list(
#         part="snippet",
#         channelId=id,
#         eventType="none",
#         maxResults=50,
#         order="date",
#         safeSearch="none",
#         type="video",
#         videoDuration="any"
#     )
#     response = request.execute()
#     for i in range(len(response["items"])):
#         items = response["items"][i]
    
#     # print("length of list ",len(response["items"]))
#         video_info = {"title": items["snippet"]["title"],
#                     "description": items["snippet"]["description"],
#                     "publishedAt": items["snippet"]["publishedAt"],
#                     "videoId": items["id"]["videoId"],
#                     "videoLink":video_starter_link+items["id"]["videoId"]}
#         print("\n")
#         print(video_info)




# get_youtube_channel_videos(username_id)



