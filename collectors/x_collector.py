import json
import tweepy
from datetime import datetime, timedelta, timezone
from core.config import *
from core.query_builder import build_query
from core.logger import logger


def get_x_client():

    return tweepy.Client(
        bearer_token=X_BEARER_TOKEN,
        consumer_key=X_API_KEY,
        consumer_secret=X_API_SECRET,
        access_token=X_ACCESS_TOKEN,
        access_token_secret=X_ACCESS_SECRET,
        wait_on_rate_limit=True
    )


def fetch_tweets(config):

    client = get_x_client()

    config["keywords"] = json.loads(config["keywords"]) if isinstance(config["keywords"],str) else config["keywords"]
    config["locations"] = json.loads(config["locations"]) if config["locations"] else []
    fetch_interval = config["frequency"] if config["frequency"] else FETCH_INTERVAL_MINUTES
    query = build_query(config)

    # calculate start time (last FETCH_INTERVAL_MINUTES minutes)
    start_time = (datetime.now(timezone.utc) - timedelta(minutes=fetch_interval)).isoformat()

    logger.info(f"Fetching tweets for config {config['id']} with query: {query}")
    logger.info(f"Start time for fetching: {start_time}")
    logger.info(f"Fetch interval (minutes): {fetch_interval}")
    logger.info(f"Current time: {datetime.now(timezone.utc).isoformat()}")

    response = client.search_recent_tweets(
        query=query,
        max_results=MAX_RESULTS_PER_QUERY,
        start_time=start_time,
        tweet_fields=["created_at","public_metrics","lang","author_id"],
        user_fields=["username","name","public_metrics","description"],
        expansions=["author_id"]
    )

    if not response.data:
        return [], {}

    users_map = {}

    if response.includes and "users" in response.includes:
        for user in response.includes["users"]:
            users_map[user.id] = user

    return response.data, users_map