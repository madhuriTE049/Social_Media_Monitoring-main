from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


def convert_to_ist(ts):

    if not ts:
        return ""

    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

    ts = ts.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Asia/Kolkata"))

    return ts.strftime("%d %b %Y %I:%M %p IST")


def build_email_table(posts, title):

    rows = ""

    for p in posts:

        time_ist = convert_to_ist(p['posted_at'])

        rows += f"""
        <tr>
            <td>{time_ist}</td>
            <td>{p['post_text']}</td>
            <td><a href="{p['post_url']}">View Post</a></td>
        </tr>
        """

    html = f"""
    <html>
    <body>

    <h2>{title}</h2>

    <table border="1" cellspacing="0" cellpadding="8">

        <tr>
            <th>Timestamp</th>
            <th>Post Content</th>
            <th>Link</th>
        </tr>

        {rows}

    </table>

    </body>
    </html>
    """

    return html



def build_combined_email_table(posts, title):

    html = f"""
    <h2>{title}</h2>

    <table border="1" cellpadding="6" cellspacing="0"
    style="border-collapse:collapse;font-family:Arial;width:100%;">

        <tr style="background:#f2f2f2">
            <th>Time</th>
            <th>Post</th>
            <th>Link</th>
            <th>Platform</th>
        </tr>
    """

    for post in posts:

        time_ist = convert_to_ist(post['posted_at'])

        html += f"""
        <tr>
            <td>{time_ist}</td>
            <td>{post['post_text'][:200]}</td>
            <td>
                <a href="{post['post_url']}">View</a>
            </td>
            <td>{post['platform']}</td>
        </tr>
        """

    html += "</table>"

    return html