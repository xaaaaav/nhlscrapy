from multiprocessing.pool import ThreadPool
from io import StringIO
from time import time as timer
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
        self.player_list = StringIO()
        self.game_list = StringIO()

    def get_player_gamelog(self, start_year=1917, end_year=datetime.datetime.now().year):
        start = timer()
        print("starting data pull")
        ThreadPool(20).map(self._pull_player_list, iterable=(self._generate_years(start_year, end_year)))
        ThreadPool(20).map(self._pull_player_data, list(set(self.player_list.getvalue().split(",")[:-1])))
        print("done")
        print("elapsed time: {}s".format(timer() - start))

    def get_game_data(self, start_year=1917, end_year=datetime.datetime.now().year):
        start = timer()
        print("starting data pull")
        ThreadPool(20).map(self._pull_game_list, iterable=(self._generate_years(start_year, end_year)))
        ThreadPool(20).map(self._pull_game_data, self.game_list.getvalue().split(",")[:-1])
        print("done")
        print("elapsed time: {}s".format(timer() - start))

    def _pull_player_list(self, year):
        start_year = int(year[:4])
        end_year = int(year[4:])

        if start_year < 1917 or end_year < 1918:
            raise BeforeNHLRecordedData

        if end_year > datetime.datetime.now().year or start_year >= datetime.datetime.now().year:
            raise NHLDataNonExistent
        
        r = requests.get(self.ROSTER_URL + year)
        data = json.loads(r.text)

        for team in data["teams"]:
            try:
                for player in team["roster"]["roster"]:
                    flattened_data = self._flatten_json(player)
                    self.player_list.write(flattened_data.get("person.link") + "::" + year + ",")
            except:
                continue

    def _pull_game_list(self, year):
        headers = True
        year1 = year[:4]
        year2 = year[4:]
        GAME_SCHEDULE = self.BASE_URL + "/api/v1/schedule?&startDate=" + year1 + "-09-01&endDate=" + year2 + "-07-01"
        games = requests.get(GAME_SCHEDULE)
        games_json = json.loads(games.text)

        for date in games_json["dates"]:
            for game in date["games"]:
                flattened_data = self._flatten_json(game)
                self.game_list.write(flattened_data.get("link") + ",")

    def _pull_player_data(self, player):

        link = player.split("::")[0]
        year = player.split("::")[1]

        PLAYER_URL = self.BASE_URL + link
        STATS_URL = PLAYER_URL + "/stats?stats=gameLog&season=" + year
        r = requests.get(PLAYER_URL)
        player_info = json.loads(r.text)
        del player_info["copyright"]
        position = player_info["people"][0]["primaryPosition"]["type"]
        name = player_info["people"][0]["fullName"]

        r_gamelog = requests.get(STATS_URL)
        data = json.loads(r_gamelog.text) 
        player_info.update(data["stats"][0])

        directory = "./player_gamelog/" + position + "/" + name

        if not os.path.exists(directory):
            os.makedirs(directory)

        with gzip.GzipFile(directory + "/" + year + ".json.gz", "w") as gzfile:
            gzfile.write(json.dumps(player_info).encode("utf-8"))

    def _pull_game_data(self, link):
        GAME_URL = self.BASE_URL + link
        game_id = re.findall("\d+", link)[1]

        game_data = requests.get(GAME_URL)
        game = json.loads(game_data.text)

        directory = "./game_data/"

        if not os.path.exists(directory):
            os.makedirs(directory)

        with gzip.GzipFile(directory + "/" + game_id + ".json.gz", "w") as gzfile:
            gzfile.write(json.dumps(game).encode("utf-8"))


    def _generate_years(self, start_year, endyear):
        year1 = start_year
        year2 = start_year + 1

        while year2 <= endyear:
            season = str(year1) + str(year2)
            yield season
            year1 += 1
            year2 += 1

    def _flatten_json(self, blob, delim="."):
        flattened = {}

        for i in blob.keys():
            if isinstance(blob[i], dict):
                get = self._flatten_json(blob[i])
                for j in get.keys():
                    flattened[ i + delim + j ] = get[j]
            else:
                flattened[i] = blob[i]

        return flattened



class BeforeNHLRecordedData(Exception):
  pass


class NHLDataNonExistent(Exception):
  pass


if __name__ == "__main__":
    nhl = NHLScraper()
    nhl.get_game_data(start_year=2017)


#get_player_list(end_year=2018)
#cleanup_playerlist()
#get_player_data()
