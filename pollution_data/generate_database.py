from datetime import date, datetime, timedelta

from pandas import DataFrame, read_csv, read_excel
from pymongo import MongoClient

from .constants import FRENCH_DEPARTMENTS

mongoClient = MongoClient()
database = mongoClient["air_quality"]

string_to_datetime = lambda x: (
    datetime.strptime(x,"%Y/%m/%d %H:%M:%S"))

def store_locations() -> None:
    '''
    Create mongoDB collection "LCSQA_stations storing informations
    regarding location in France of all the stations owned by the
    Central Laboratory of Air Quality Monitoring (LCSQA).
    '''
    url = "https://www.lcsqa.org/system/files/media/documents/"+\
    "Liste points de mesures 2021 pour site LCSQA_27072022.xlsx"
    # Import file from previous url giving location of LCSQA stations.
    data = read_excel(url.replace(" ", "%20"), sheet_name=1)
    # Rearrange and clean the data.
    c = data.columns.tolist()
    columns_to_remove = c[3:7]+c[10:]
    labels = data.iloc[1].tolist()
    labels_to_keep = labels[:3]+labels[7:10]
    data = data.drop(
        columns=columns_to_remove
    ).drop(
        [0,1]
    ).set_axis(
        labels_to_keep,
        axis="columns")
    # Define a function "get_department" to retrieve names of French
    # departments using postal codes of french cities.
    get_department = lambda x: (
        FRENCH_DEPARTMENTS[x[:2]] if not(x[1].isdigit()) or int(x[:2]) < 97
        else FRENCH_DEPARTMENTS[x[:3]]
    )
    # Add a new column "Département" using "get_department".
    data["Département"] = data["Code commune"].apply(
        get_department) # Keep only columns with useful informations.
    data = data[
        ["Région",
        "Département",
        "Commune",
        "Nom station",
        "Code station"]]
    # Turn the "data" dataframe into the "LCSQA_stations" collection.
    database["LCSQA_stations"].insert_many(data.to_dict("records"))
    
    
def store_pollution_data() -> None:
    '''
    Create two collections storing hourly average concentrations of 
    air pollutants recorded on working days and weekends.

    Arguments:
    n_days -- number of last pollution days whose data are collected.
    update -- boolean used to determine the names of the collections
              storing the pollution data.
    '''

    d = date.today() - timedelta(days=180)
    # Iterate over each day until the current day.
    while d < date.today():
        url = "https://files.data.gouv.fr/lcsqa/concentrations-de"+\
        "-polluants-atmospheriques-reglementes/temps-reel/"+\
        str(d.year)+"/FR_E2_"+d.isoformat()+".csv"
        data = read_csv(url, sep=";")
        # Test whether "csv" file provide some pollution data
        # (Server errors may occur, making data unavailable).
        if "validité" in data.columns:
            # Extract rows with validated data.
            data = data[data["validité"]==1]
            # Extract rows with consistent concentration value
            # (bugs during the recording process may generate 
            # negative values.)
            data = data[data["valeur brute"]>0]
                 # Extract rows with pollutants of interest.
            data["pollutant_to_ignore"] = data["Polluant"].apply(
                lambda x: x in ["NO","NOX as NO2","C6H6","O3"])
            data = data[data["pollutant_to_ignore"]==False]
            data["dateTime"] = data["Date de début"].apply(
                lambda x: stringToDatetime(x))
            data["hour"] = data["dateTime"].apply(
                lambda x: x.hour)
            data["working_day"] = data["dateTime"].apply(
                lambda x: x.weekday()<=4)
            data = data[
                ["code site",
                "Polluant",
                "hour",
                "valeur brute",
                "dateTime",
                "working_day"]]
            # Put the extracted data into the "LCSQA_data"
            # collection.
            database["LCSQA_data"].insert_many(
                data.to_dict("records"))
        # Move on to the following day.
        d += timedelta(days=1)
    # Split the "LCSQA_data" collection into two collections
    # (named differently depending on whether being on an update
    # or not) to separate the data recorded on working days from those
    # recorded on weekends.
    names = ("working_days","weekends") if not(update) \
    else ("new_working_days", "new_weekends")
    for document in database["LCSQA_data"].find():
        if document["working_day"]:
            database[names[0]].insert_one(document)
        else:
            database[names[1]].insert_one(document)
    
    database.drop_collection("LCSQA_data")


def generate_database() -> None:
    '''
    Create the "air quality" MongoDB database comprised of
    the following collections:
        - "cities", grouping air quality monitoring stations by cities.
        - "departments", grouping cities by French department.
        - "regions", grouping French departments by French region.
        - "LCSQA_data", containing air pollution data collected over 
           the last 180 days.
    '''
    if "air_quality" in mongoClient.list_database_names():
        mongoClient.drop_database("air_quality")
    database = mongoClient["air_quality"]
    # Create the "LCSQA_stations" collection.
    store_locations()
    # Create the "cities" collection using "LCSQA_stations".
    database["LCSQA_stations"].aggregate([
        {"$set":
            {"station": {"name": "$Nom station",
                         "code": "$Code station"}}},
        {"$group":
            {"_id": "$Commune",
             "stations": {"$push": "$station"}}},
        {"$out": "cities"}])
    # Create the "departments" collection using "LCSQA_stations"
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$Département",
             "cities": {"$push": "$Commune"}}},
        {"$out": "departments"}])
    # Create the "regions" collection using "LCSQA_stations".
    database["LCSQA_stations"].aggregate([
        {"$group":
            {"_id": "$Région",
             "departments": {"$push": "$Département"}}},
        {"$out": "regions"}])

    database.drop_collection("LCSQA_stations")    
    
    store_pollution_data()
    # Create the "distribution_pollutants" collection giving, for
    # each station, the pollutant(s) whose air concentration is 
    # being recorded.
    database["working_days"].aggregate([
        {"$group":
            {"_id": "$code site",
             "monitored_pollutants":
                {"$push": "$Polluant"}}},
        {"$out": "distribution_pollutants"}])
    # Group the pollution data to allow fast calculation of the 
    # wanted averages (see function "get_values") and fast updates 
    # of the database (see function "update_database").
    for name in ["working_days","weekends"]:
        database[name].aggregate([
            {"$group":
                {"_id": {"station": "$code site",
                         "pollutant": "$Polluant",
                         "hour": "$hour"},
                 "values": {"$push": "$valeur brute"},
                 "dates": {"$push": "$dateTime"}}},
            {"$project":
                {"history": {"values": "$values",
                             "dates": "$dates"}}},
            {"$out": name}])
    # Save the current date in a new collection "last_update" 
    # (necessary to know how many pollution days are missing
    # when performing the next update).
    d = date.today()
    database["last_update"].insert_one(
        {"date": datetime(
            d.year, d.month, d.day)-timedelta(days=1)})
        
if __name__=="__main__":
    generate_database()
