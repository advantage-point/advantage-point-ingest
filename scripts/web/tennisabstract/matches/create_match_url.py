def create_match_url(
    match_url_suffix: str
) -> str:
    """
    Arguments:
    - match_url_suffix: Match URL suffix (contains .htm or .html)

    Create match url.
    """

    match_url_stem = 'https://www.tennisabstract.com/charting'
    match_url = f"{match_url_stem}/{match_url_suffix}"

    return match_url