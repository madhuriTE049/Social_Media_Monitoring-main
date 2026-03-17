import json
from datetime import datetime

def save_post(cursor, tweet, author, config_id, matched_keywords):
    post_url = f"https://x.com/{author.username}/status/{tweet.id}"
    metrics = tweet.public_metrics or {}

    cursor.execute("""
        INSERT IGNORE INTO posts
        (platform_post_id, platform, keyword_config_id, matched_keywords,
         post_text, post_url, author_username, author_display_name,
         author_followers, like_count, retweet_count, reply_count,
         impression_count, language, posted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        str(tweet.id),
        "X",
        config_id,
        json.dumps(matched_keywords),
        tweet.text,
        post_url,
        author.username,
        author.name,
        author.public_metrics.get("followers_count",0) if author.public_metrics else 0,
        metrics.get("like_count",0),
        metrics.get("retweet_count",0),
        metrics.get("reply_count",0),
        metrics.get("impression_count",0),
        tweet.lang or "en",
        tweet.created_at.replace(tzinfo=None) if tweet.created_at else datetime.utcnow()
    ))

    return cursor.lastrowid


def get_recent_posts(cursor, config_id, platform, minutes):

    cursor.execute("""
        SELECT posted_at, post_text, post_url
        FROM posts
        WHERE keyword_config_id = %s
        AND platform = %s
        AND fetched_at >= NOW() - INTERVAL %s MINUTE
        ORDER BY posted_at DESC
    """,(config_id, platform, minutes))

    return cursor.fetchall()

def get_recent_posts_all_platforms(cursor, config_id, minutes):

    cursor.execute("""
        SELECT
            platform,
            posted_at,
            post_text,
            post_url
        FROM posts
        WHERE keyword_config_id = %s
        AND fetched_at >= NOW() - INTERVAL %s MINUTE
        ORDER BY fetched_at DESC
    """,(config_id, minutes))

    return cursor.fetchall()