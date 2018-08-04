import pytest

from nhlscrapy.nhlscrapy import NHLScraper

def test_pull_player_list_throws_error_when_year_before_1917():
	nhl = NHLScraper()
	with pytest.raises(ValueError):
		nhl._pull_player_list("19161917")

def test_pull_player_list_throws_error_for_season_that_has_not_yet_happened():
	nhl = NHLScraper()
	with pytest.raises(ValueError):
		nhl._pull_player_list("30013002")
