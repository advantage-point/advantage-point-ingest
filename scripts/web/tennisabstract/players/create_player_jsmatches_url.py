def create_player_jsmatches_url(
    player_name: str
) -> str:
    """
    Arguments:
    - player_name: Player name

    Returns player 'jsmatches' url
    """

    # format parts of url string
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/jsmatches/{url_player_name}.js"

    return player_url