from datetime import date
from typing import Dict, List, Tuple

import pymongo
import streamlit as st

OVERSEAS_DEPARTMENTS = [
    "GUADELOUPE",
    "GUYANE",
    "MARTINIQUE",
    "LA REUNION",
    "MAYOTTE",
    "SAINT-MARTIN"]


@st.cache_resource
def init_connection():
    return pymongo.MongoClient("mongodb://database:27017")

@st.cache_data
def get_stations() -> List[str]:
    database = init_connection()["air_quality"]
    return database["distribution_pollutants"].distinct("_id")

@st.cache_data
def get_data(s: str, p: str) -> 
Tuple[
    pymongo.collection | None, 
    pymongo.collection | None]:
    '''
    Return the pymongo collections (or None when not enough data) 
    containing hourly average air concentrations of pollutant "p" 
    recorded by station "s" on both working_days and weekends.
    '''
    working_days, weekends = None, None
    # Send the request to the backend and retrieve the expected data.
    response = requests.get(
        "http://backend:8080",
        params={"s": s,"p": p},
        verify=False)
    if response.status_code == 200:
        working_days = response.json()["working_days"]
        weekends = response.json()["weekends"]
    return working_days, weekends

@st.cache_data
def get_items(
    about: str, 
    query_filter: Dict[str, str] | None) -> List[str]:
    '''
    Query the "air_quality" database to retrieve the items
    representing the available choices proposed to the user.

    Arguments:
    about -- string determining the name of the collection
             to query within the database.
    query_filter -- dictionary used as filter for the query.
    '''
    database = init_connection()["air_quality"]
    # Query the appropriate collection and store the retrieved
    # elements in a list "items".
    match about:
        case "regions":
            items = database["regions"].find().distinct("_id")
            for e in overseas_departments:
                items.remove(e)
        case "departments":
            if query_filter["_id"] == "OUTRE-MER":
                items = overseas_departments
            else:
                items = list(set(database["regions"].find_one(
                    query_filter)["departments"]))
        case "cities":
            items = list(set(database["departments"].find_one(
                query_filter)["cities"]))
        case "stations":
            list_of_stations = database["cities"].find_one(
                query_filter)["stations"]
            items = list(set([
                e["name"]+"#"+e["code"]
                for e in list_of_stations]))
        case "pollutants":
            items = list(set(database["distribution_pollutants"].find_one(
                query_filter)["monitored_pollutants"]))
    # Build the "listed_items" list giving the ordered set of the retrieved
    # items along with their corresponding position.
    listed_items = list(zip(sorted(items), range(1,len(items)+1)))
    if about == "regions":
        listed_items.append(("OUTRE-MER",len(listed_items)+1))
    return listed_items

def get_dates() -> Tuple[date, date]:
    '''
    Return the starting and the ending dates of the period over which
    the pollution data are collected.
    '''
    last_update = database["last_update"].find_one()
    ending_date = date(
        last_update.year,
        last_update.month,
        last_update.day)
    starting_date = ending_date - timedelta(days=180)
    return starting_date, ending_date
