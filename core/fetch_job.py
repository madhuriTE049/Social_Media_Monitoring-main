# import logging
# import json

# from database.connection import get_db_connection
# from database.repository import save_post
# from collectors.x_collector import fetch_tweets
# from parsers.sentiment import analyze_sentiment
# from parsers.demographics import estimate_demographics
# from collectors.youtube_collector import fetch_youtube_posts

# from notifications.email_sender import send_email
# from notifications.report_builder import build_email_table
# from notifications.report_builder import build_combined_email_table

# from database.repository import get_recent_posts
# from database.repository import get_recent_posts_all_platforms

# from core.logger import logger

# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# EMAIL_MODE = "combined"
# # options: "separate" or "combined"


# def find_matching_keywords(text, keywords):

#     text = text.lower()
#     matched = []

#     for k in keywords:

#         k_clean = k.lower()

#         if k_clean in text:
#             matched.append(k)

#         elif k_clean.replace(" ", "") in text.replace(" ", ""):
#             matched.append(k)

#     return matched


# def run_fetch_job():
#     """Run fetch for ALL active configs (kept for manual/testing use)."""
#     db = get_db_connection()
#     cur = db.cursor(dictionary=True)
#     cur.execute("SELECT * FROM keyword_configs WHERE is_active=1")
#     configs = cur.fetchall()
#     cur.close()
#     db.close()

#     for config in configs:
#         run_fetch_job_for_config(config)


# def run_fetch_job_for_config(config):

#     config_id = config["id"]
#     frequency = int(config.get("frequency") or 60)
    
#     logger.info(f"Starting fetch job for config id={config_id} (frequency={frequency}m)")

#     db = get_db_connection()
#     cur = db.cursor(dictionary=True)

#     total = 0

#     # store posts grouped by email list
#     email_groups = {}

#     if True:  # single-config scope

#         tweets, users = fetch_tweets(config)
#         logger.info(f"X returned {len(tweets)} tweets")

#         keywords = config["keywords"] if isinstance(config["keywords"], list) else json.loads(config["keywords"])

#         for tweet in tweets:

#             author = users.get(tweet.author_id)

#             if not author:
#                 continue

#             matched = find_matching_keywords(tweet.text, keywords)

#             try:

#                 post_id = save_post(cur, tweet, author, config["id"], matched)

#                 if post_id:

#                     sentiment, score = analyze_sentiment(tweet.text)

#                     cur.execute("""
#                         INSERT IGNORE INTO post_sentiment
#                         (post_id,sentiment,sentiment_score)
#                         VALUES (%s,%s,%s)
#                     """,(post_id,sentiment,score))

#                     demo = estimate_demographics(
#                         author.username,
#                         author.name,
#                         author.description
#                     )

#                     cur.execute("""
#                         INSERT IGNORE INTO author_demographics
#                         (post_id,estimated_age_group,estimated_gender)
#                         VALUES (%s,%s,%s)
#                     """,(post_id,demo["estimated_age_group"],demo["estimated_gender"]))

#                     db.commit()
#                     total += 1

#             except Exception as e:
#                 db.rollback()
#                 log.warning(e)

#         # -----------------------------
#         # YOUTUBE COLLECTION
#         # -----------------------------

#         youtube_videos = fetch_youtube_posts(config)
        
#         logger.info(f"YouTube returned {len(youtube_videos)} videos")

#         for video in youtube_videos:

#             text = video.get("text", "").strip()

#             if not text:
#                 continue

#             matched = find_matching_keywords(text, keywords)

#             try:

#                 cur.execute("""
#                 INSERT IGNORE INTO posts
#                 (platform_post_id, platform, keyword_config_id, matched_keywords,
#                 post_text, post_url, author_username, author_display_name,
#                 like_count, retweet_count, reply_count, impression_count,
#                 language, posted_at)
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 """,(
#                     video["id"],
#                     "YOUTUBE",
#                     config["id"],
#                     json.dumps(matched),
#                     text,
#                     video["url"],
#                     video["channel"],
#                     video["channel"],
#                     0,
#                     0,
#                     0,
#                     0,
#                     "en",
#                     video["published_at"]
#                 ))

#                 post_id = cur.lastrowid

#                 if post_id:

#                     sentiment, score = analyze_sentiment(text)

#                     cur.execute("""
#                         INSERT IGNORE INTO post_sentiment
#                         (post_id, sentiment, sentiment_score)
#                         VALUES (%s,%s,%s)
#                     """,(post_id,sentiment,score))

#                     demo = estimate_demographics(
#                         video["channel"],
#                         video["channel"],
#                         ""
#                     )

#                     cur.execute("""
#                         INSERT IGNORE INTO author_demographics
#                         (post_id, estimated_age_group, estimated_gender)
#                         VALUES (%s,%s,%s)
#                     """,(post_id,demo["estimated_age_group"],demo["estimated_gender"]))

#                     db.commit()
#                     total += 1

#             except Exception as e:
#                 db.rollback()
#                 log.warning(e)

#         # -----------------------------
#         # EMAIL COLLECTION LOGIC
#         # -----------------------------

#         emails = config.get("emails")

#         logger.info(f"Checking email trigger for config {config['id']}")

#         if emails:

#             emails = emails if isinstance(emails, list) else json.loads(emails)

#             log.info(f"Emails configured: {emails}")

#             email_key = tuple(sorted(set(emails)))

#             if EMAIL_MODE == "separate":

#                 platforms = ["X", "YOUTUBE"]

#                 for platform in platforms:

#                     posts = get_recent_posts(cur, config["id"], platform, frequency)

#                     if email_key not in email_groups:
#                         email_groups[email_key] = []

#                     email_groups[email_key].extend(posts)

#             elif EMAIL_MODE == "combined":

#                 posts = get_recent_posts_all_platforms(
#                     cur,
#                     config["id"],
#                     frequency
#                 )

#                 if email_key not in email_groups:
#                     email_groups[email_key] = []

#                 email_groups[email_key].extend(posts)

#     # -----------------------------
#     # SEND EMAILS AFTER COLLECTING
#     # -----------------------------

#     for email_key, posts in email_groups.items():

#         if not posts:
#             continue

#         emails = list(email_key)

#         logger.info(f"Sending report to {emails} with {len(posts)} posts")

#         if EMAIL_MODE == "separate":

#             html = build_email_table(
#                 posts,
#                 "Social Listening Report"
#             )

#         else:

#             html = build_combined_email_table(
#                 posts,
#                 "Social Listening (All Platforms)"
#             )

#         send_email(
#             emails,
#             "Social Media Monitoring Report",
#             html
#         )

#     cur.close()
#     db.close()

#     logger.info(f"Fetched {total} posts")

import logging
import json
from datetime import datetime

from database.connection import get_db_connection
from database.repository import save_post
from database.repository import get_recent_posts, get_recent_posts_all_platforms

from collectors.x_collector import fetch_tweets
from collectors.youtube_collector import fetch_youtube_posts
from collectors.instagram_collector import fetch_instagram_posts

from parsers.sentiment import analyze_sentiment
from parsers.demographics import estimate_demographics

from notifications.email_sender import send_email
from notifications.report_builder import build_email_table, build_combined_email_table

from core.logger import logger

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

EMAIL_MODE = "combined"
#EMAIL_MODE = "separate"


def find_matching_keywords(text, keywords):

    text = text.lower()
    matched = []

    for k in keywords:

        k_clean = k.lower()

        if k_clean in text:
            matched.append(k)

        elif k_clean.replace(" ", "") in text.replace(" ", ""):
            matched.append(k)

    return matched


def run_fetch_job_for_config(config, cur, db):

    config_id = config["id"]
    frequency = int(config.get("frequency") or 60)

    logger.info(f"Starting fetch job for config id={config_id} (frequency={frequency}m)")

    total = 0

    keywords = config["keywords"] if isinstance(config["keywords"], list) else json.loads(config["keywords"])

    # X COLLECTION

    tweets, users = fetch_tweets(config)
    logger.info(f"X returned {len(tweets)} tweets")

    for tweet in tweets:
        author = users.get(tweet.author_id)
        if not author:
            continue

        matched = find_matching_keywords(tweet.text, keywords)

        try:
            post_id = save_post(cur, tweet, author, config_id, matched)

            if post_id:
                sentiment, score = analyze_sentiment(tweet.text)

                cur.execute("""
                    INSERT IGNORE INTO post_sentiment
                    (post_id,sentiment,sentiment_score)
                    VALUES (%s,%s,%s)
                """,(post_id,sentiment,score))

                demo = estimate_demographics(
                    author.username,
                    author.name,
                    author.description
                )

                cur.execute("""
                    INSERT IGNORE INTO author_demographics
                    (post_id,estimated_age_group,estimated_gender)
                    VALUES (%s,%s,%s)
                """,(post_id,demo["estimated_age_group"],demo["estimated_gender"]))

                total += 1

        except Exception as e:
            db.rollback()
            log.warning(e)

    # YOUTUBE COLLECTION

    youtube_videos = fetch_youtube_posts(config)
    logger.info(f"YouTube returned {len(youtube_videos)} videos")

    for video in youtube_videos:
        text = video.get("text", "").strip()

        if not text:
            continue

        matched = find_matching_keywords(text, keywords)

        try:
            cur.execute("""
            INSERT IGNORE INTO posts
            (platform_post_id, platform, keyword_config_id, matched_keywords,
            post_text, post_url, author_username, author_display_name,
            like_count, retweet_count, reply_count, impression_count,
            language, posted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,(
                video["id"],
                "YOUTUBE",
                config_id,
                json.dumps(matched),
                text,
                video["url"],
                video["channel"],
                video["channel"],
                0,0,0,0,
                "en",
                video["published_at"]
            ))

            post_id = cur.lastrowid

            if post_id:
                sentiment, score = analyze_sentiment(text)

                cur.execute("""
                    INSERT IGNORE INTO post_sentiment
                    (post_id, sentiment, sentiment_score)
                    VALUES (%s,%s,%s)
                """,(post_id,sentiment,score))

                demo = estimate_demographics(video["channel"], video["channel"], "")

                cur.execute("""
                    INSERT IGNORE INTO author_demographics
                    (post_id, estimated_age_group, estimated_gender)
                    VALUES (%s,%s,%s)
                """,(post_id,demo["estimated_age_group"],demo["estimated_gender"]))

                total += 1

        except Exception as e:
            db.rollback()
            log.warning(e)

    # INSTAGRAM COLLECTION

    try:
        instagram_posts = fetch_instagram_posts(config)
    except Exception as e:
        logger.error(f"Instagram fetch failed: {e}")
        instagram_posts = []

    logger.info(f"Instagram returned {len(instagram_posts)} posts")

    for post in instagram_posts:

        posted_at = datetime.fromisoformat(post["published_at"].replace("Z","+00:00"))

        text = post.get("text", "").strip()

        if not text:
            continue

        matched = find_matching_keywords(text, keywords)

        if not matched:
            continue

        try:

            cur.execute("""
            INSERT IGNORE INTO posts
            (platform_post_id, platform, keyword_config_id, matched_keywords,
            post_text, post_url, author_username, author_display_name,
            like_count, retweet_count, reply_count, impression_count,
            language, posted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,(
                post["id"],
                "INSTAGRAM",
                config_id,
                json.dumps(matched),
                text,
                post["url"],
                post.get("username",""),
                post.get("username",""),
                0,0,0,0,
                "en",
                posted_at
            ))

            post_id = cur.lastrowid

            if post_id:

                sentiment, score = analyze_sentiment(text)

                cur.execute("""
                    INSERT IGNORE INTO post_sentiment
                    (post_id, sentiment, sentiment_score)
                    VALUES (%s,%s,%s)
                """,(post_id, sentiment, score))

                demo = estimate_demographics(
                    post.get("username",""),
                    post.get("username",""),
                    ""
                )

                cur.execute("""
                    INSERT IGNORE INTO author_demographics
                    (post_id, estimated_age_group, estimated_gender)
                    VALUES (%s,%s,%s)
                """,(post_id, demo["estimated_age_group"], demo["estimated_gender"]))

                total += 1

        except Exception as e:
            db.rollback()
            log.warning(e)

    db.commit()

    return total


def run_single_config_job(config):

    db = get_db_connection()
    cur = db.cursor(dictionary=True)

    total = run_fetch_job_for_config(config, cur, db)

    cur.close()
    db.close()

    logger.info(f"Fetched {total} posts")


def send_grouped_reports_by_frequency():

    db = get_db_connection()
    cur = db.cursor(dictionary=True)

    cur.execute("SELECT * FROM keyword_configs WHERE is_active=1")
    configs = cur.fetchall()

    # GROUP CONFIGS BY FREQUENCY
    frequency_groups = {}

    for config in configs:

        frequency = int(config.get("frequency") or 60)

        if frequency not in frequency_groups:
            frequency_groups[frequency] = []

        frequency_groups[frequency].append(config)

    # PROCESS EACH FREQUENCY GROUP
    for frequency, configs_group in frequency_groups.items():

        # ============================
        # COMBINED MODE
        # ============================
        if EMAIL_MODE == "combined":

            email_groups = {}

            for config in configs_group:

                config_id = config["id"]

                emails = config.get("emails")
                if not emails:
                    continue

                emails = emails if isinstance(emails, list) else json.loads(emails)
                email_key = tuple(sorted(set(emails)))

                posts = get_recent_posts_all_platforms(
                    cur,
                    config_id,
                    frequency
                )

                if email_key not in email_groups:
                    email_groups[email_key] = []

                email_groups[email_key].extend(posts)

            for email_key, posts in email_groups.items():

                if not posts:
                    continue

                emails = list(email_key)

                logger.info(f"Sending grouped report (frequency={frequency}) to {emails} with {len(posts)} posts")

                html = build_combined_email_table(
                    posts,
                    f"Social Listening Report ({frequency} min)"
                )

                send_email(
                    emails,
                    f"Social Media Monitoring Report ({frequency} min)",
                    html
                )

        # ============================
        # SEPARATE MODE
        # ============================
        elif EMAIL_MODE == "separate":

            platforms = ["X", "YOUTUBE", "INSTAGRAM"]

            for config in configs_group:

                config_id = config["id"]

                emails = config.get("emails")
                if not emails:
                    continue

                emails = emails if isinstance(emails, list) else json.loads(emails)

                for platform in platforms:

                    posts = get_recent_posts(
                        cur,
                        config_id,
                        platform,
                        frequency
                    )

                    if not posts:
                        continue

                    logger.info(f"Sending {platform} report for config {config_id} to {emails} with {len(posts)} posts")

                    html = build_email_table(
                        posts,
                        f"Social Listening ({platform})"
                    )

                    send_email(
                        emails,
                        f"{platform} Monitoring Report",
                        html
                    )

    cur.close()
    db.close()