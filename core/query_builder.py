def build_query(config):

    boolean_query = config.get("boolean_query")
    keywords = config.get("keywords",[])
    locations = config.get("locations",[])

    if boolean_query:
        query = boolean_query

    elif keywords:
        keyword_parts = " OR ".join(
            [f'"{k}"' if " " in k else k for k in keywords]
        )
        query = f"({keyword_parts})"

    else:
        raise ValueError("No keywords defined")

    if locations:
        location_parts = " OR ".join([f'place:"{loc}"' for loc in locations])
        query += f" ({location_parts})"

    # query += " -is:retweet -is:reply lang:en"

    return query