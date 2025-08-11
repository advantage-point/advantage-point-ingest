def create_player_url(
    player_name: str,
    player_gender: str
) -> str:
    """
    Arguments:
    - player_name: Player name
    - player_gender: Player gender

    Returns player url
    """

    # format parts of url string
    url_player_str = 'player' if player_gender == 'M' else 'wplayer'
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/cgi-bin/{url_player_str}.cgi?p={url_player_name}"

    return player_url

def create_player_classic_url(
    player_name: str,
    player_gender: str
) -> str:
    """
    Arguments:
    - player_name: Player name
    - player_gender: Player gender

    Returns player url
    """

    # format parts of url string
    url_player_str = 'player' if player_gender == 'M' else 'wplayer'
    url_player_name = player_name.replace(' ', '')

    player_url = f"https://www.tennisabstract.com/cgi-bin/{url_player_str}-classic.cgi?p={url_player_name}"

    return player_url