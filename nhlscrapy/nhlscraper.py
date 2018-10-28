from datetime import datetime
from multiprocessing.pool import ThreadPool
from time import sleep, time as timer
import json
import os
import re

import requests

from nhlscrapy import _generate_years, _flatten_json, _validate_years, _get_start_end_date, _write_to_disk, _write_to_s3


class NHLScraper():

    def __init__(self, location, bucket=None):
        START_YEAR = 1917
        self.BASE_URL = "https://statsapi.web.nhl.com"
        self.ROSTER_URL = self.BASE_URL + "/api/v1/teams?expand=team.roster&season="
        self.player_dict = {}
        self.game_dict = {}
        self.division_dict = {}
        self.draft_dict = {}
        self.stat_types = self._pull_player_stat_type()
        self.standing_types = self._pull_standing_type()
        self.location = location
        self.bucket = bucket
        end_year = datetime.now().year
        if datetime.now().month >= 10:
            end_year += 1

        ThreadPool(40).map(self._pull_player_list, iterable=(_generate_years(START_YEAR, end_year)))
        ThreadPool(40).map(self._pull_game_list, iterable=(_generate_years(START_YEAR, end_year)))

    def get_player_data(self, player_list=None, stat_type="gameLog"):
        self.stat_type = stat_type
        if player_list:
            subset = {}
            for player in player_list:
                subset[player] = self.player_dict[player]
            ThreadPool(40).map(self._pull_player_data, subset)
        else:
            ThreadPool(40).map(self._pull_player_data, self.player_dict)

    def get_game_data(self, start_date="2017-09-01", end_date="2018-07-01", team_list=None):
        self.start_game_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_game_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.team = team
        if team_list:
            subset = {}
            for team in team_list:
                subset[team] = self.game_dict[team]
            ThreadPool(40).map(self._pull_game_data, subset)
        else:
            ThreadPool(40).map(self._pull_game_data, self.game_dict)

    def get_awards_data(self):
        r = requests.get(self.BASE_URL + "/api/v1/awards")
        awards = json.loads(r.text)

        directory = "./awards/"
        filename = "awards.json.gz"
        if self.location == "disk":
            _write_to_disk(directory, filename, awards)
        elif self.location == "s3":
            _write_to_s3(self.bucket, directory, filename, awards)

    def get_draft_data(self, year=None):
        if year:
            self._pull_draft_data(year)
        else:
            ThreadPool(40).map(self._pull_draft_data, iterable=(_generate_years(1995, 2018)))

    def _pull_draft_data(self, year):
        draft_year = year[:4]
        DRAFT_URL = self.BASE_URL + "/api/v1/draft/" + draft_year

        r = requests.get(DRAFT_URL)
        draft_data = json.loads(r.text)

        directory = "./draft_data/"
        filename = draft_year + "_draft.json.gz"
        if self.location == "disk":
            _write_to_disk(directory, filename, draft_data)
        elif self.location == "s3":
            _write_to_s3(self.bucket, directory, filename, draft_data)

    def _pull_player_list(self, year):
        start_year = int(year[:4])
        end_year = int(year[4:])

        _validate_years(start_year, end_year)
        
        r = requests.get(self.ROSTER_URL + year)
        data = json.loads(r.text)

        for team in data["teams"]:
            try:
                for player in team["roster"]["roster"]:
                    flattened_data = _flatten_json(player)
                    if flattened_data.get("person.fullName") not in self.player_dict:
                        self.player_dict[flattened_data.get("person.fullName")] = {"api.link": flattened_data.get("person.link"), "year": [year]}
                    else:
                        self.player_dict[flattened_data.get("person.fullName")]["year"].append(year)
                        self.player_dict[flattened_data.get("person.fullName")]["year"].sort()
            except:
                continue

    def _pull_game_list(self, year):
        start_year= year[:4]
        end_year = year[4:]
        GAME_SCHEDULE = self.BASE_URL + "/api/v1/schedule?&startDate=" + start_year + "-09-01&endDate=" + end_year + "-07-01"
        games = requests.get(GAME_SCHEDULE)
        games_json = json.loads(games.text)

        for date in games_json["dates"]:
            for game in date["games"]:
                flattened_data = _flatten_json(game)
                if date["date"] not in self.game_dict:
                    self.game_dict[date["date"]] = {"api.link": [flattened_data.get("link")]}
                else:
                    self.game_dict[date["date"]]["api.link"].append(flattened_data.get("link"))

    def _pull_player_data(self, player):

        link = self.player_dict[player]["api.link"]
        years = self.player_dict[player]["year"]

        PLAYER_URL = self.BASE_URL + link
        for year in years:
            STATS_URL = PLAYER_URL + "/stats?stats=" + self.stat_type + "&season=" + year
            r = requests.get(PLAYER_URL)
            player_info = json.loads(r.text)
            position = player_info["people"][0]["primaryPosition"]["type"]
            name = player_info["people"][0]["fullName"]

            r_gamelog = requests.get(STATS_URL)
            data = json.loads(r_gamelog.text) 
            player_info.update(data["stats"][0])

            directory = "./player_gamelog_" + self.stat_type + "/" + position + "/" + name + "/"
            filename = year + ".json.gz"

            if self.location == "disk":
                _write_to_disk(directory, filename, player_info)
            elif self.location == "s3":
                _write_to_s3(self.bucket, directory, filename, player_info)

    def _pull_game_data(self, date):

        current_date = datetime.datetime.strptime(date, "%Y-%m-%d")

        if self.start_game_date <= current_date <= self.end_game_date:
            for link in self.game_dict[date]["api.link"]:
                GAME_URL = self.BASE_URL + link
                game_id = re.findall("\d+", link)[1]

                game_data = requests.get(GAME_URL)
                game = json.loads(game_data.text)

                away = game["gameData"]["teams"]["away"]["abbreviation"]
                home = game["gameData"]["teams"]["home"]["abbreviation"]
                directory = "./game_data/" + date + "/"
                filename = away + "vs" + home + ".json"

                if self.team:
                    if self.team == away or self.team == home:
                        if self.location == "disk":
                            _write_to_disk(directory, filename, game)
                        elif self.location == "s3":
                            _write_to_s3(self.bucket, directory, filename, game)
                else:
                    if self.location == "disk":
                        _write_to_disk(directory, filename, game)
                    elif self.location == "s3":
                        _write_to_s3(self.bucket, directory, filename, game)

    def _pull_player_stat_type(self):
        r = requests.get(self.BASE_URL + "/api/v1/statTypes")
        stat_types = json.loads(r.text)

        types = []
        for dic in stat_types:
            types.append(dic["displayName"])
        return types

    def _pull_standing_type(self):
        r = requests.get(self.BASE_URL + "/api/v1/standingsTypes")
        stat_types = json.loads(r.text)

        types = []
        for dic in stat_types:
            types.append(dic["name"])
        return types