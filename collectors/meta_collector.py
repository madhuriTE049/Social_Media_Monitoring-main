import requests
import logging
import json
from core.config import META_ACCESS_TOKEN, FB_PAGE_IDS, INSTAGRAM_BUSINESS_ACCOUNT_ID

log = logging.getLogger(__name__)

BASE_URL = "https://graph.facebook.com/v19.0"

def fetch_facebook_posts(config, published_after=None):
    """
    Fetches posts from configured Facebook Page IDs and filters by keywords locally.
    """
    if not META_ACCESS_TOKEN:
        log.warning("META_ACCESS_TOKEN not set. Skipping Facebook collection.")
        return []

    if not FB_PAGE_IDS:
        log.warning("FB_PAGE_IDS not set. Skipping Facebook collection.")
        return []

    keywords = config["keywords"] if isinstance(config["keywords"], list) else json.loads(config["keywords"])
    all_posts = []

    for page_id in FB_PAGE_IDS:
        url = f"{BASE_URL}/{page_id.strip()}/posts"
        params = {
            "access_token": META_ACCESS_TOKEN,
            "fields": "id,message,created_time,permalink_url,from",
            "limit": 50
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            if "error" in data:
                log.error(f"Error fetching FB posts for {page_id}: {data['error'].get('message')}")
                continue

            for item in data.get("data", []):
                message = item.get("message", "")
                created_at = item.get("created_time")

                # Filter by timeframe if provided
                if published_after and created_at < published_after:
                    continue

                # The keyword filtering happen in fetch_job.py, but we only return potential matches
                # to optimize or just return all and let fetch_job handle it like YouTube.
                # Actually YouTube collector doesn't filter, fetch_job does.
                
                post = {
                    "id": item["id"],
                    "text": message,
                    "author": item.get("from", {}).get("name", "Unknown"),
                    "url": item.get("permalink_url"),
                    "published_at": created_at,
                    "platform": "FACEBOOK"
                }
                all_posts.append(post)

        except Exception as e:
            log.error(f"Request failed for FB Page {page_id}: {e}")

    return all_posts

def get_hashtag_id(hashtag_name):
    """
    Resolves a hashtag name to a Meta Hashtag ID.
    """
    url = f"{BASE_URL}/ig_hashtag_search"
    params = {
        "user_id": INSTAGRAM_BUSINESS_ACCOUNT_ID,
        "q": hashtag_name,
        "access_token": META_ACCESS_TOKEN
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        if "error" in data:
            print(f"DEBUG: Meta Hashtag Search Error: {json.dumps(data['error'], indent=2)}")
        if "data" in data and len(data["data"]) > 0:
            return data["data"][0]["id"]
    except Exception as e:
        log.error(f"Failed to find hashtag ID for {hashtag_name}: {e}")
    return None

def fetch_instagram_posts(config, published_after=None):
    """
    Fetches Instagram posts using Hashtag Search.
    Note: Requires an Instagram Business Account ID and specific permissions.
    """
    if not META_ACCESS_TOKEN or not INSTAGRAM_BUSINESS_ACCOUNT_ID:
        log.warning("META_ACCESS_TOKEN or INSTAGRAM_BUSINESS_ACCOUNT_ID not set. Skipping Instagram collection.")
        return []

    keywords = config["keywords"] if isinstance(config["keywords"], list) else json.loads(config["keywords"])
    all_posts = []

    for keyword in keywords:
        # Instagram hashtag search only works for single words, no spaces
        # We'll treat each keyword as a hashtag (remove spaces)
        tag = keyword.replace(" ", "").replace("#", "")
        hashtag_id = get_hashtag_id(tag)
        
        if not hashtag_id:
            continue

        url = f"{BASE_URL}/{hashtag_id}/recent_media"
        params = {
            "user_id": INSTAGRAM_BUSINESS_ACCOUNT_ID,
            "fields": "id,caption,timestamp,permalink",
            "access_token": META_ACCESS_TOKEN,
            "limit": 10  # keep low to avoid API "reduce data" errors
        }

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "error" in data:
                print(f"DEBUG: Meta API Error for {tag}: {json.dumps(data['error'], indent=2)}")
                log.error(f"Error fetching IG media for hashtag {tag}: {data['error'].get('message')}")
                continue

            for item in data.get("data", []):
                caption = item.get("caption", "")
                created_at_str = item.get("timestamp")

                # Compare datetimes instead of strings
                if published_after and created_at_str:
                    try:
                        from datetime import datetime
                        item_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).replace(tzinfo=None)
                        pub_after_dt = datetime.fromisoformat(published_after.replace('Z', '+00:00')).replace(tzinfo=None)
                        if item_dt < pub_after_dt:
                            continue
                    except Exception as e:
                        print(f"DEBUG: Date parse error: {e}")

                # Parse published_at to a naive datetime object
                parsed_pub_dt = None
                if created_at_str:
                    try:
                        from datetime import datetime
                        parsed_pub_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00')).replace(tzinfo=None)
                    except Exception as e:
                        print(f"DEBUG: Failed to parse published_at: {e} | Raw: {created_at_str}")
                else:
                    print(f"DEBUG: created_at_str is empty for post {item['id']}")

                post = {
                    "id": item["id"],
                    "text": caption,
                    "author": "Instagram User",
                    "url": item.get("permalink"),
                    "published_at": parsed_pub_dt,
                    "platform": "INSTAGRAM"
                }
                all_posts.append(post)

        except Exception as e:
            log.error(f"Request failed for IG hashtag {tag}: {e}")

    return all_posts
