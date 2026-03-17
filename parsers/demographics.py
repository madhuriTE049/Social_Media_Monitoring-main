import re

FEMALE_SIGNALS = ["she","her","girl","woman","female","lady","mrs","miss","ms"]
MALE_SIGNALS = ["he","him","boy","man","male","mr","sir","guy"]

def estimate_demographics(username, display_name, bio=""):

    text = f"{username} {display_name} {bio}".lower()
    tokens = re.findall(r'\b\w+\b', text)

    gender = "unknown"

    if any(t in FEMALE_SIGNALS for t in tokens):
        gender = "female"
    elif any(t in MALE_SIGNALS for t in tokens):
        gender = "male"

    return {
        "estimated_gender": gender,
        "estimated_age_group": "unknown"
    }