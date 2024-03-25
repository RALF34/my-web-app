from fastapi import FastAPI
from pymongo import MongoClient

app = FastAPI()

mongoClient = MongoClient("mongodb://database:27017")
database = mongoClient["air_quality"]

@app.get("/")
async def get_response(station: str, pollutant: str)
    query_filter = {"_id.station": station, "_id.pollutant": pollutant}
    return {
        "working_days": database["working_days"].find(query_filter),
        "weekends": database["weeekends"].find(query_filter)}
