from pymongo import MongoClient
from sportsreference.ncaab.teams import Teams
from time import strftime


client = MongoClient()
db = client.clarktechsports

sos = {}
srs = {}

for team in Teams():
    # Convert the pandas DataFrame to a single-dimension index-less dictionary
    team_stats = team.dataframe.to_dict('r')[0]
    sos[team.abbreviation] = team.strength_of_schedule
    srs[team.abbreviation] = team.simple_rating_system
    # Determine if a document already exists for the given team
    if bool(db.teams.find({"abbreviation": team.abbreviation}).count()):
        # If a document exists, overwrite all stats with the latest values
        db.teams.update_one({"abbreviation": team.abbreviation},
                            {"$set": team_stats})
    else:
        # If a document doesn't exist, add a new one
        db.teams.insert_one(team_stats)

# Insert every team's strength of schedule, indexed by the date
db.sos.insert_one({"date": strftime("%x"), "sos": sos})
# Insert every team's simple rating system score, indexed by the date
db.srs.insert_one({"date": strftime("%x"), "srs": srs})
