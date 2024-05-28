import os
import pandas as pd
import numpy as np
import requests
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, CELEBRITIES


def get_video_id(
    publish_after: str, publish_before: str, iterations: int, search_term: str
):
    """
    Get the video IDs of the videos
    """

    video_id_list = []
    url = "https://yt.lemnoslife.com/noKey/search"

    print(f"Getting video IDs for {iterations} iterations...")

    for i in range(1, iterations + 1):
        parameters = {
            "part": "id,snippet",
            "maxResults": "50",
            "q": search_term,
            "type": "video",
            "publishedBefore": publish_before,
            "publishedAfter": publish_after,
            "order": "date",
        }
        if i > 1:
            parameters["nextPageToken"] = nextpagetoken

        res = requests.get(url, params=parameters)
        search_list = res.json()
        df = pd.json_normalize(search_list["items"])
        video_id_list.append(df["id.videoId"])

        print(f"Iteration {i}: Found {len(df['id.videoId'])} video IDs")

        if "nextPageToken" in search_list:
            nextpagetoken = search_list["nextPageToken"]

    video_ids = pd.concat(video_id_list, axis=0, ignore_index=True)
    print(f"Total video IDs found: {len(video_ids)}")

    return video_ids


def get_comments(video_ids: list):
    """
    Get comments from the videos
    """

    list_text = []
    list_date = []
    list_video_id = []  # Added list for video IDs
    url = "https://yt.lemnoslife.com/noKey/commentThreads"

    print(f"Getting comments for {len(video_ids)} videos...")

    for video_id in video_ids:
        parameters = {"part": "id,snippet", "maxResults": "50", "videoId": video_id}
        res = requests.get(url, params=parameters)
        comment_list = res.json()

        try:
            comment_df = pd.json_normalize(comment_list["items"])
            comment_df["snippet.topLevelComment.snippet.updatedAt"] = comment_df[
                "snippet.topLevelComment.snippet.updatedAt"
            ].apply(lambda x: x[:10])
            comment_df["updateDate"] = pd.to_datetime(
                comment_df["snippet.topLevelComment.snippet.updatedAt"]
            )
            comment_df["video_id"] = video_id  # Added video ID to the DataFrame
            list_text.append(comment_df["snippet.topLevelComment.snippet.textOriginal"])
            list_date.append(comment_df["updateDate"])
            list_video_id.append(comment_df["video_id"])  # Added video ID to the list

            print(f"Video ID {video_id}: Found {len(comment_df)} comments")
        except KeyError:
            pass

    df_text = pd.concat(list_text, axis=0, ignore_index=True)
    df_date = pd.concat(list_date, axis=0, ignore_index=True)
    df_video_id = pd.concat(
        list_video_id, axis=0, ignore_index=True
    )  # Concatenated video ID list
    df = pd.concat(
        [df_text, df_date, df_video_id], axis=1
    )  # Added video ID to the final DataFrame
    df.columns = ["text", "updateDt", "video_id"]  # Added video_id to the column names

    print(f"Total comments found: {len(df)}")

    return df


def get_video_stats(video_ids: list):
    """
    Get the video stats of the videos
    """

    stats = []
    url = "https://yt.lemnoslife.com/noKey/videos"

    print(f"Getting video stats for {len(video_ids)} videos...")

    for i, video_id in enumerate(video_ids):
        parameters = {"part": "id,snippet,statistics", "id": video_id}
        res = requests.get(url, params=parameters)
        video_data = res.json()

        try:
            view_count = int(video_data["items"][0]["statistics"]["viewCount"])
            like_count = int(video_data["items"][0]["statistics"]["likeCount"])
            stats.append([video_id, view_count, like_count])
        except (KeyError, IndexError):
            print(f"Case {i} pass")
            stats.append([video_id, np.nan, np.nan])

        print(f"Status: {i + 1} / {len(video_ids)}")

    stats_df = pd.DataFrame(stats, columns=["video_id", "view_count", "like_count"])
    print(f"Total video stats found: {len(stats_df)}")

    return stats_df


def process_celebrities(celebrities: list):
    """
    Process the celebrities
    """

    for celebrity in celebrities:
        name = celebrity["name"]
        search_term = celebrity["search_term"]
        start_date = celebrity["start_date"]
        end_date = celebrity["end_date"]
        comments_file_path = os.path.join(RAW_DATA_PATH, f"{name}_youtube_comments.csv")
        stats_file_path = os.path.join(RAW_DATA_PATH, f"{name}_youtube_stats.csv")

        print(f"Processing {name}...")
        print(f"Search Term: {search_term}")
        print(f"Start Date: {start_date}")
        print(f"End Date: {end_date}")

        video_ids = get_video_id(start_date, end_date, 20, search_term)  # 1000 videos
        stats_df = get_video_stats(video_ids)
        comments_df = get_comments(video_ids)

        # Saving raw comments and stats to file
        comments_df.to_csv(comments_file_path, index=False)
        stats_df.to_csv(stats_file_path, index=False)

    return comments_df, stats_df


if __name__ == "__main__":
    process_celebrities(CELEBRITIES)
