import os
import pandas as pd
import numpy as np
import requests
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from config import RAW_DATA_PATH, CELEBRITIES

MAX_VIDEO_COUNT = 500
MAX_COMMENTS_PER_VIDEO = 5000


def get_video_id(
    publish_after: str,
    publish_before: str,
    search_term: str,
    max_video_count: int = MAX_VIDEO_COUNT,
):
    """
    Get the video IDs of the videos up to a maximum count
    """
    video_id_list = []
    url = "https://yt.lemnoslife.com/noKey/search"

    print(f"Getting video IDs up to a maximum of {max_video_count} videos...")

    params = {
        "part": "id,snippet",
        "maxResults": "50",
        "q": search_term,
        "type": "video",
        "publishedBefore": publish_before,
        "publishedAfter": publish_after,
        "order": "relevance",
    }

    while len(video_id_list) < max_video_count:
        res = requests.get(url, params=params)
        data = res.json()

        if "items" in data:
            df = pd.json_normalize(data["items"])
            if "id.videoId" in df.columns:
                video_id_list.extend(df["id.videoId"].tolist())
                print(
                    f"Found {len(df['id.videoId'])} video IDs, total: {len(video_id_list)}"
                )
            else:
                print("'id.videoId' not found in response")

            if "nextPageToken" in data:
                params["pageToken"] = data["nextPageToken"]
            else:
                break
        else:
            print("No items found in response")
            break

    # Ensure we do not exceed the maximum video count
    video_ids = pd.Series(video_id_list[:max_video_count]).drop_duplicates()
    print(f"Total unique video IDs found: {len(video_ids)}")

    return video_ids


def fetch_comments(video_id: str, max_comments_per_video: int = MAX_COMMENTS_PER_VIDEO):
    list_text = []
    list_date = []
    list_video_id = []
    url = "https://yt.lemnoslife.com/noKey/commentThreads"

    parameters = {
        "part": "id,snippet",
        "maxResults": "50",
        "videoId": video_id,
        "order": "relevance",
    }
    total_comments = 0

    while total_comments < max_comments_per_video:
        res = requests.get(url, params=parameters)
        comment_list = res.json()

        if "items" in comment_list:
            comment_df = pd.json_normalize(comment_list["items"])

            if not comment_df.empty:
                comment_df["updateDate"] = pd.to_datetime(
                    comment_df["snippet.topLevelComment.snippet.updatedAt"]
                ).dt.date
                comment_df["video_id"] = video_id

                list_text.extend(
                    comment_df["snippet.topLevelComment.snippet.textOriginal"]
                )
                list_date.extend(comment_df["updateDate"])
                list_video_id.extend(comment_df["video_id"])

                total_comments += len(comment_df)

                print(
                    f"Video ID {video_id}: Found {len(comment_df)} comments, total: {total_comments}"
                )

                if (
                    "nextPageToken" in comment_list
                    and total_comments < max_comments_per_video
                ):
                    parameters["pageToken"] = comment_list["nextPageToken"]
                else:
                    break
            else:
                print(f"Video ID {video_id}: No comments found in this response.")
                break
        else:
            print(f"Video ID {video_id}: No items found in response.")
            break

    return (
        list_text[:max_comments_per_video],
        list_date[:max_comments_per_video],
        list_video_id[:max_comments_per_video],
    )


def get_comments(video_ids: list):
    """
    Get comments from the videos
    """
    results = []
    print("Getting comments from videos...")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {
            executor.submit(fetch_comments, video_id): video_id
            for video_id in video_ids
        }
        for future in as_completed(futures):
            results.append(future.result())

    list_text, list_date, list_video_id = zip(*results)
    df_text = pd.concat([pd.Series(x) for x in list_text], axis=0, ignore_index=True)
    df_date = pd.concat([pd.Series(x) for x in list_date], axis=0, ignore_index=True)
    df_video_id = pd.concat(
        [pd.Series(x) for x in list_video_id], axis=0, ignore_index=True
    )

    df = pd.concat([df_text, df_date, df_video_id], axis=1)
    df.columns = ["text", "updateDt", "video_id"]

    print(f"Total comments found: {len(df)} in {len(df['video_id'].unique())} videos")

    return df


def fetch_video_stats(video_id):
    url = "https://yt.lemnoslife.com/noKey/videos"
    parameters = {"part": "id,snippet,statistics", "id": video_id}
    res = requests.get(url, params=parameters)
    video_data = res.json()

    try:
        view_count = int(video_data["items"][0]["statistics"]["viewCount"])
        like_count = int(video_data["items"][0]["statistics"]["likeCount"])
        return [video_id, view_count, like_count]
    except (KeyError, IndexError):
        return [video_id, np.nan, np.nan]


def get_video_stats(video_ids: list):
    """
    Get the video stats of the videos
    """
    stats = []
    print("Getting video stats...")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {
            executor.submit(fetch_video_stats, video_id): video_id
            for video_id in video_ids
        }
        for future in as_completed(futures):
            stats.append(future.result())

    stats_df = pd.DataFrame(stats, columns=["video_id", "view_count", "like_count"])
    print(f"Total video stats found: {len(stats_df)}")

    return stats_df


def process_single_celebrity(celebrity):
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

    video_ids = get_video_id(start_date, end_date, search_term)
    stats_df = get_video_stats(video_ids)
    comments_df = get_comments(video_ids)

    comments_df.to_csv(comments_file_path, index=False)
    stats_df.to_csv(stats_file_path, index=False)

    return comments_df, stats_df


def process_celebrities(celebrities: list):
    """
    Process the celebrities
    """
    results = []
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {
            executor.submit(process_single_celebrity, celeb): celeb
            for celeb in celebrities
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"An error occurred: {e}")

    if results:
        comments_dfs, stats_dfs = zip(*results)
        return pd.concat(comments_dfs, ignore_index=True), pd.concat(
            stats_dfs, ignore_index=True
        )
    else:
        return pd.DataFrame(), pd.DataFrame()


if __name__ == "__main__":
    process_celebrities(CELEBRITIES)
