import requests
import json
from datetime import datetime, timedelta, timezone
from core.config import INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_BUSINESS_ID
from core.logger import logger


def fetch_instagram_posts(config):

    # Load keywords safely
    keywords = config["keywords"] if isinstance(config["keywords"], list) else json.loads(config["keywords"])

    # Convert keywords → hashtags
    hashtags = [
        k.replace(" ", "").lower()
        for k in keywords
        if isinstance(k, str) and len(k.strip()) > 2
    ]

    posts = []

    # Config-based time filtering with buffer
    frequency = int(config.get("frequency", 15))
    buffer = max(30, frequency)

    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=frequency + buffer)

    for hashtag in hashtags[:5]:  # limit to avoid rate issues

        try:
            # Step 1: Get hashtag ID
            search = requests.get(
                "https://graph.facebook.com/v19.0/ig_hashtag_search",
                params={
                    "user_id": INSTAGRAM_BUSINESS_ID,
                    "q": hashtag,
                    "access_token": INSTAGRAM_ACCESS_TOKEN
                },
                timeout=(5, 20)
            ).json()

            if "error" in search:
                logger.warning(f"Instagram hashtag search error for '{hashtag}': {search['error']}")
                continue

            if "data" not in search or len(search["data"]) == 0:
                continue

            hashtag_id = search["data"][0]["id"]

            # Step 2: Get recent media
            media = requests.get(
                f"https://graph.facebook.com/v19.0/{hashtag_id}/recent_media",
                params={
                    "user_id": INSTAGRAM_BUSINESS_ID,
                    "fields": "id,caption,media_type,permalink,timestamp",
                    "limit": 20,
                    "access_token": INSTAGRAM_ACCESS_TOKEN
                },
                timeout=(5, 20)
            ).json()

            if "error" in media:
                logger.warning(f"Instagram media fetch error for '{hashtag}': {media['error']}")
                continue

            if "data" not in media or len(media["data"]) == 0:
                continue

            for item in media["data"]:

                text = item.get("caption", "") or ""

                posted_at = datetime.fromisoformat(
                    item["timestamp"].replace("Z", "+00:00")
                )

                # Manual time filtering
                if posted_at < cutoff_time:
                    continue

                posts.append({
                    "id": item["id"],
                    "text": text,
                    "url": item.get("permalink", ""),
                    "username": "instagram_user",
                    "published_at": item["timestamp"]
                })

        except Exception as e:
            logger.warning(f"Instagram fetch exception for '{hashtag}': {e}")
            continue

    logger.info(f"Instagram collection completed for config {config.get('id')} with {len(posts)} posts")

    return posts