from multiprocessing.pool import ThreadPool
from time import sleep, time as timer
import csv 
import datetime
import gzip
import json
import os
import re

import requests


class NHLScraper():

    def __init__(self):
        self.BASE_URL = "https://statsapi.web.nhl.com"
        self.ROSTER_URL = self.BASE_URL + "/api/v1/teams?expand=team.roster&season="
        self.player_dict = {}
        self.game_dict = {}
        self.division_dict = {}
        self.draft_dict = {}
        self.stat_types = self._pull_player_stat_type()
        self.standing_types = self._pull_standing_type()
        ThreadPool(40).map(self._pull_player_list, iterable=(NHLScraper._generate_years(1917, 2018)))
        ThreadPool(40).map(self._pull_game_list, iterable=(NHLScraper._generate_years(1917, 2018)))

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
        self.start_game_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        self.end_game_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
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
        NHLScraper._write_to_disk(directory, filename, awards)

    def get_draft_data(self, year=None):
        if year:
            self._pull_draft_data(year)
        else:
            ThreadPool(40).map(self._pull_draft_data, iterable=(NHLScraper._generate_years(1995, 2018)))

    def _pull_draft_data(self, year):
        draft_year = year[:4]
        DRAFT_URL = self.BASE_URL + "/api/v1/draft/" + draft_year

        r = requests.get(DRAFT_URL)
        draft_data = json.loads(r.text)

        directory = "./draft_data/"
        filename = draft_year + "_draft.json.gz"
        NHLScraper._write_to_disk(directory, filename, draft_data)

    def _pull_player_list(self, year):
        start_year = int(year[:4])
        end_year = int(year[4:])

        NHLScraper._validate_years(start_year, end_year)
        
        r = requests.get(self.ROSTER_URL + year)
        data = json.loads(r.text)

        for team in data["teams"]:
            try:
                for player in team["roster"]["roster"]:
                    flattened_data = self._flatten_json(player)
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
                flattened_data = self._flatten_json(game)
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

            NHLScraper._write_to_disk(directory, filename, player_info)

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
                filename = away + "vs" + home + ".json.gz"

                if self.team:
                    if self.team == away or self.team == home:
                        NHLScraper._write_to_disk(directory, filename, game)
                else:
                    NHLScraper._write_to_disk(directory, filename, game)
               
    @staticmethod
    def _write_to_disk(directory, filename, data):
        if not os.path.exists(directory):
            os.makedirs(directory)
        with gzip.GzipFile(directory + filename, "w") as gzfile:
            gzfile.write(json.dumps(data).encode("utf-8"))

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

    @staticmethod
    def _generate_years(start_year, end_year):
        year1 = start_year
        year2 = start_year + 1

        while year2 <= end_year:
            season = str(year1) + str(year2)
            yield season
            year1 += 1
            year2 += 1

    @staticmethod
    def _flatten_json(blob, delim="."):
        flattened = {}

        for i in blob.keys():
            if isinstance(blob[i], dict):
                get = NHLScraper._flatten_json(blob[i])
                for j in get.keys():
                    flattened[ i + delim + j ] = get[j]
            else:
                flattened[i] = blob[i]

        return flattened

    @staticmethod
    def _validate_years(start_year, end_year):
        if start_year < 1917 or end_year < 1918:
            raise ValueError("Date is before the NHL started recording data.")

        if end_year > datetime.datetime.now().year or start_year >= datetime.datetime.now().year:
            raise ValueError("NHL data not yet recorded.")


if __name__ == "__main__":
    nhl = NHLScraper()
    nhl.get_draft_data("1995")
    #print(nhl.standing_types)
