import csv
import json
import urllib.request
from collections import defaultdict

import boto3


def calculate_match_points(team1, team2, score1, score2):
    if score1 == score2:
        return {team1: 1, team2: 1}
    elif score1 > score2:
        return {team1: 3, team2: 0}
    else:
        return {team1: 0, team2: 3}


def lambda_handler(event, context):
    results_url = "https://raw.githubusercontent.com/openfootball/football.json/master/2020-21/en.1.json"
    final_table = defaultdict(lambda: {"points": 0, "gd": 0})

    with urllib.request.urlopen(results_url) as url:
        results = json.loads(url.read().decode())

    print(f"Running {results['name']} table generator")
    for match in results["matches"]:
        if match.get("score"):
            team1 = match["team1"]
            team2 = match["team2"]
            score1 = match["score"]["ft"][0]
            score2 = match["score"]["ft"][1]

            match_outcome = calculate_match_points(team1, team2, score1, score2)
            final_table[team1] = {
                "points": final_table[team1]["points"] + match_outcome[team1],
                "gd": final_table[team1]["gd"] + (score1 - score2)
            }
            final_table[team2] = {
                "points": final_table[team2]["points"] + match_outcome[team2],
                "gd": final_table[team2]["gd"] + (score2 - score1)
            }

    upload(final_table)


def upload(table):
    filepath = '/tmp/en1_table.csv'

    csv_columns = ['team', 'points', 'gd']
    with open(filepath, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
        writer.writeheader()
        for team, data in table.items():
            writer.writerow({**{'team': team}, **table[team]})

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('football-table-predictor-dev')
    bucket.upload_file(filepath, 'en1/table.csv')


if __name__ == '__main__':
    lambda_handler(None, None)
