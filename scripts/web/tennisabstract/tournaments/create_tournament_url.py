def create_tournament_url(
    tournament_year,
    tournament_gender: str,
    tournament_name: str,
) -> str:
    """
    Arguments:
    - tournament_year: Tournament year
    - tournament_gender: Tournament gender
    - tournament_name: Tournament name

    Returns tournament url
    """

    # make sure year is string for concatenation
    tournament_year = str(tournament_year)

    # format parts of url string
    url_tournament_year = tournament_year if tournament_gender == 'M' else f"W_{tournament_year}"
    url_tournament_str = 'tourney' if tournament_gender == 'M' else 'wtourney'
    url_tournament_name = tournament_name.replace(' ', '_')

    tournament_url = f"https://www.tennisabstract.com/cgi-bin/{url_tournament_str}.cgi?t={url_tournament_year}{url_tournament_name}"

    return tournament_url