import pytest
from unittest.mock import MagicMock, patch
import youtube_ingestion_pipeline 

from supadata.errors import SupadataError
import requests

class TestYoutubeIngestion:

    @patch("youtube_ingestion_pipeline.build")
    def test_get_youtube_channel_id(self, mock_build):
        # Arrange: build() returns a youtube client mock
        youtube = MagicMock()
        mock_build.return_value = youtube

        # Arrange: youtube.channels().list(...).execute() returns your fake API response
        youtube.channels.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "id": "UCBJycsmduvYEL83R_U4JriQ",
                    "snippet": {"title": "Marques Brownlee"},
                }
            ]
        }

        # Act
        username_id, title = youtube_ingestion_pipeline.get_youtube_channel_id("mkbhd")

        # Assert
        assert username_id == "UCBJycsmduvYEL83R_U4JriQ"
        assert title == "Marques Brownlee"
        mock_build.assert_called_once()
        youtube.channels.assert_called_once()
        youtube.channels.return_value.list.assert_called_once_with(
            part="contentDetails,contentOwnerDetails,id,snippet,contentDetails,statistics",
            forHandle="mkbhd",
        )
        youtube.channels.return_value.list.return_value.execute.assert_called_once()

    
    @patch("youtube_ingestion_pipeline.build")
    def test_get_youtube_channel_playlist(self, mock_build):
        # Arrange: build() returns a youtube client mock
        sample_response = {
            "nextPageToken": "TOKEN123",
            "items": [
                {
                    "contentDetails": {"videoId": "sfyL4BswUeE"},
                    "snippet": {"title": "Smartphone Awards 2025!"}
                },
                {
                    "contentDetails": {"videoId": "Mb6H7trzMfI"},
                    "snippet": {"title": "Driving Xiaomi's Electric Car: Are we Cooked?"}
                },
            ],
        }

        youtube = MagicMock()
        mock_build.return_value = youtube

        # Arrange: youtube.playlistItems().list(...).execute() returns your fake API response
        youtube.playlistItems.return_value.list.return_value.execute.return_value = sample_response

        # Act
        result = youtube_ingestion_pipeline.get_youtube_channel_playlist("UCBJycsmduvYEL83R_U4JriQ")

        # Assert
        assert result == sample_response

        mock_build.assert_called_once()
        youtube.playlistItems.assert_called_once()
        youtube.playlistItems.return_value.list.assert_called_once()
        youtube.playlistItems.return_value.list.return_value.execute.assert_called_once()


    @patch("youtube_ingestion_pipeline.raw_transcript")
    def test_supadata_success(self,mock_raw):
        mock_raw.return_value = "TRANSCRIPT TEXT"

        result = youtube_ingestion_pipeline.supadata_error_handler(
            url="fake-url",
            videoid="video123"
        )

        assert result == "TRANSCRIPT TEXT"
        mock_raw.assert_called_once()

    


    @patch("youtube_ingestion_pipeline.time.sleep")
    @patch("youtube_ingestion_pipeline.raw_transcript")
    def test_rate_limit_then_success(self,mock_raw, mock_sleep):
        mock_raw.side_effect = [
            SupadataError("limit-exceeded"),
            "TRANSCRIPT TEXT"
        ]

        result = youtube_ingestion_pipeline.supadata_error_handler(
            url="fake-url",
            videoid="video123"
        )

        assert result == "TRANSCRIPT TEXT"
        assert mock_raw.call_count == 2
        mock_sleep.assert_called_once()

    
    
    @patch("youtube_ingestion_pipeline.raw_transcript")
    def test_rate_limit_then_success(self,mock_raw):
        mock_raw.side_effect = SupadataError(400,"transcript-unavailable","{}")
        result = youtube_ingestion_pipeline.supadata_error_handler(
            url="fake-url",
            videoid="video123"
        )

        assert result is None
        mock_raw.assert_called_once()


    


    @patch("youtube_ingestion_pipeline.time.sleep")
    @patch("youtube_ingestion_pipeline.raw_transcript")
    def test_http_502_retries_then_fails(self,mock_raw, mock_sleep):
        response = MagicMock()
        response.status_code = 502
        error = requests.exceptions.HTTPError(response=response)

        mock_raw.side_effect = error

        result = youtube_ingestion_pipeline.supadata_error_handler(
            url="fake-url",
            videoid="video123"
        )

        assert result is None
        assert mock_raw.call_count == 5
        assert mock_sleep.call_count == 5

    @patch.dict(
        "os.environ", {"SUPABASE_URL": "https://test.supabase.co", "SUPABASE_KEY": "test-key"}
    )
    @patch("youtube_ingestion_pipeline.create_client")
    def test_supabase_credentials(self,mock_create_client):
        client = MagicMock()
        mock_create_client.return_value = client
        result = youtube_ingestion_pipeline.supabase()

        assert result is client
        mock_create_client.assert_called_once_with("https://test.supabase.co", "test-key")
        


    

    @patch.dict("os.environ", {}, clear=True)
    def test_get_supabase_client_without_credentials(self) -> None:
        """Test get_supabase_client returns None when both credentials are missing."""
        result = youtube_ingestion_pipeline.supabase()
        assert result is None



   
    @patch("youtube_ingestion_pipeline.requests.post")
    def test_upload_transcript(self,mock_post):
        response = MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        mock_post.return_value = response
        transcript_path = "channel/raw/video.txt"
        raw_text = "hello world"


        youtube_ingestion_pipeline.SUPABASE_URL = "https://test.supabase.co"
        youtube_ingestion_pipeline.SUPABASE_KEY = "test-key"

        # Act
        result = youtube_ingestion_pipeline.upload_transcript_bytes(
            transcript_path,
            raw_text,
        )

        # Assert: request was made correctly
        mock_post.assert_called_once_with(
            "https://test.supabase.co/storage/v1/object/transcripts/channel/raw/video.txt",
            headers={
                "Authorization": "Bearer test-key",
                "apikey": "test-key",
                "Content-Type": "text/plain; charset=utf-8",
                "x-upsert": "false",
            },
            data=b"hello world",
            timeout=30,
        )

        # Assert: Supabase accepted the upload
        response.raise_for_status.assert_called_once()
        assert result == "uploaded"

    
    @patch("youtube_ingestion_pipeline.requests.post")
    def test_upload_transcript_already_exists(self, mock_post):
        response = MagicMock()
        response.status_code = 409
        mock_post.return_value = response

        youtube_ingestion_pipeline.SUPABASE_URL = "https://test.supabase.co"
        youtube_ingestion_pipeline.SUPABASE_KEY = "test-key"

        result = youtube_ingestion_pipeline.upload_transcript_bytes("channel/raw/video.txt", "hello")

        assert result == "exists"


    @patch("youtube_ingestion_pipeline.requests.post")
    def test_upload_transcript_already_exists_400_duplicate(self, mock_post):
        response = MagicMock()
        response.status_code = 400
        response.text = '{"statuscode":"409","error":"duplicate","message":"the resource already exists"}'
        mock_post.return_value = response

        youtube_ingestion_pipeline.SUPABASE_URL = "https://test.supabase.co"
        youtube_ingestion_pipeline.SUPABASE_KEY = "test-key"

        result = youtube_ingestion_pipeline.upload_transcript_bytes("channel/raw/video.txt", "hello")

        assert result == "exists"

    

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import youtube_ingestion_pipeline


def _sb_with_tables(channel_row, existing_video_ids):
    sb = MagicMock()

    channels_tbl = MagicMock()
    videos_tbl = MagicMock()
    logs_tbl = MagicMock()

    def table(name):
        if name == "Channels":
            return channels_tbl
        if name == "Videos":
            return videos_tbl
        if name == "Logs":
            return logs_tbl
        raise ValueError(name)

    sb.table.side_effect = table

    # Channels upsert -> execute -> .data
    channels_tbl.upsert.return_value.execute.return_value = SimpleNamespace(data=[channel_row])

    # Videos select -> eq -> execute -> .data
    videos_tbl.select.return_value.eq.return_value.execute.return_value = SimpleNamespace(
        data=[{"video_id": vid} for vid in existing_video_ids]
    )

    # Videos bulk upsert
    videos_tbl.upsert.return_value.execute.return_value = SimpleNamespace(data=[])

    # Logs upsert
    logs_tbl.upsert.return_value.execute.return_value = SimpleNamespace(data=[])

    return sb, channels_tbl, videos_tbl, logs_tbl


@patch("youtube_ingestion_pipeline.time.sleep")
@patch("youtube_ingestion_pipeline.upload_transcript_bytes")
@patch("youtube_ingestion_pipeline.supadata_error_handler")
@patch("youtube_ingestion_pipeline.get_youtube_channel_playlist")
@patch("youtube_ingestion_pipeline.get_youtube_channel_id")
@patch("youtube_ingestion_pipeline.supabase")
def test_store_data_processes_one_new_video_and_writes_to_supabase(
    mock_supabase_factory,
    mock_get_channel_id,
    mock_get_playlist,
    mock_supadata,
    mock_upload,
    mock_sleep,
):
    # Arrange: globals used by store_data
    youtube_ingestion_pipeline.youtube_channels = ["@unboxtherapy"]
    youtube_ingestion_pipeline.video_starter_link = "https://www.youtube.com/watch?v="

    channel_row = {
        "id": "channel-uuid-1",
        "latest_video_published_at": "2025-11-01T00:00:00Z",  # older than playlist item
    }
    sb, channels_tbl, videos_tbl, logs_tbl = _sb_with_tables(
        channel_row=channel_row,
        existing_video_ids=set(),  # no videos exist yet
    )
    mock_supabase_factory.return_value = sb

    mock_get_channel_id.return_value = ("CHANNEL_YT_ID", "Unbox Therapy")

    mock_get_playlist.return_value = {
        "items": [
            {
                "snippet": {
                    "publishedAt": "2025-12-14T17:29:17Z",
                    "channelTitle": "Unbox Therapy",
                    "title": "The World’s Thinnest Paper Tablet",
                    "description": "desc",
                },
                "contentDetails": {"videoId": "L2fBJCDEEJk"},
            }
        ],
        # no nextPageToken => stop after 1 page
    }

    mock_supadata.return_value = "TRANSCRIPT TEXT"
    mock_upload.return_value = "uploaded"

    # Act
    youtube_ingestion_pipeline.store_data()

    # Assert: transcript fetched + upload called
    mock_supadata.assert_called_once_with(
        "https://www.youtube.com/watch?v=L2fBJCDEEJk",
        "L2fBJCDEEJk",
    )
    mock_upload.assert_called_once()
    upload_path = mock_upload.call_args[0][0]
    assert upload_path.startswith("unboxtherapy/")  # '@' stripped
    assert upload_path.endswith("_L2fBJCDEEJk.txt")

    # Assert: Videos bulk upsert called with one row
    assert videos_tbl.upsert.called
    video_rows = videos_tbl.upsert.call_args[0][0]
    assert len(video_rows) == 1
    row = video_rows[0]
    assert row["video_id"] == "L2fBJCDEEJk"
    assert row["transcript_status"] == "fetched"
    assert row["transcript_path"] is not None
    assert row["channel_id"] == "channel-uuid-1"

    # Assert: Logs upsert called with one row and conflict on video_id
    assert logs_tbl.upsert.called
    logs_rows = logs_tbl.upsert.call_args[0][0]
    logs_kwargs = logs_tbl.upsert.call_args.kwargs
    assert len(logs_rows) == 1
    assert logs_rows[0]["video_id"] == "L2fBJCDEEJk"
    assert "run_id" in logs_rows[0] and logs_rows[0]["run_id"].startswith("run_")
    assert logs_kwargs["on_conflict"] == "video_id"

    # Assert: Channels upsert called twice (initial + final update)
    assert channels_tbl.upsert.call_count == 2



    
