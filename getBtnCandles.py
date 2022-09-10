import calendar
import requests
import datetime as dt
import calendar
import time as t
import csv
from pymongo import MongoClient
import sys
from env import *

# Set up env file
print("Loading .env")
env = Env(".env")
print("Loaded.")

# Establish MongoDB client
def mongoDbConnect():
    CONNECTION_STRING = env.contents["DB_CONNECTION_STRING"]
    client = MongoClient(CONNECTION_STRING)
    return client["btn"]


# Make request to Coinbase API for BTN candle
def makeRequest(startDt, endDt):
    url = f"https://api.exchange.coinbase.com/products/btc-usd/candles?granularity=60&start={startDt}&end={endDt}"
    headers = {"Accept": "application/json"}
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        return res.json()
    elif res.status_code == 429:
        # too many requests, something broke with the timing
        t.sleep(1)
        print("Bad request so sleeping 1 second..")
        return makeRequest(startDt, endDt)
    else:
        raise Exception(res.status_code, res.text)


# Update MongoDB database
def postDocs(collection, docs):
    try:
        collection.insert_many(docs)
    except Exception as err:
        print(f"Failed to insert document. {err}")


# Find number of days in a year
def getDaysInYear(year):
    days = 365
    if calendar.isleap(year):
        days = 366

    dtPairs = list()
    startDate = dt.date(year, 1, 1)

    for i in range(1, days + 1):
        dateShift = startDate + dt.timedelta(days=i - 1)
        beginTime = dt.datetime.min.time()
        endTime = dt.datetime.max.time()
        dtPair = {
            "start": dt.datetime.combine(dateShift, beginTime),
            "end": dt.datetime.combine(dateShift, endTime),
        }

        dtPairs.append(dtPair)

    return dtPairs


# Takes a day start and end and calculates hourly datetime intervals
def getTimeIntervalsPairsForDay(dayStartDt, dayEndDt):
    intervals = [dayStartDt]
    for i in range(1, 24):
        tD1 = dt.timedelta(hours=i)
        tD2 = dt.timedelta(hours=i, seconds=1)
        shifted1 = dayStartDt + tD1
        shifted2 = dayStartDt + tD2
        intervals.append(shifted1)
        intervals.append(shifted2)
    intervals.append(dayEndDt)

    # build list with start, end, average interval
    intervalPairs = list()
    for i in range(0, len(intervals) - 1, 2):
        averageInter = intervals[i + 1] - dt.timedelta(minutes=30)
        intervalPairs.append(
            {
                "start": intervals[i].isoformat(),
                "end": intervals[i + 1].isoformat(),
                "average": averageInter.isoformat(),
            }
        )

    return intervalPairs


def main():
    # Establish connection to db
    print("Connecting to MongoDB...")
    dbConn = mongoDbConnect()
    if dbConn == None:
        print("Failed to connect to MongoDB.")
        sys.exit(1)
    else:
        print("Connection established.")
    collection = dbConn["ticker"]

    year = int(input("What year do you want ticker data from?\n>"))
    days = getDaysInYear(year)

    with open(f"{str(year)}_ticker.csv", "w", newline="") as outFile:
        writer = csv.writer(outFile)
        writer.writerow(["timestamp", "price"])  # header

        for day in days:
            print(f"Processing day {day['start']}..")

            intervals = getTimeIntervalsPairsForDay(day["start"], day["end"])
            counter = 0
            startReqTime = t.time()
            for interval in intervals:
                res = makeRequest(interval["start"], interval["end"])

                docsToInsert = list()
                for i in range(len(res) - 1, 0, -1):
                    time = dt.datetime.utcfromtimestamp(res[i][0])
                    averagePrice = (res[i][1] + res[i][2]) / 2
                    doc = {"timestamp": time, "price": averagePrice}
                    docsToInsert.append(doc)
                    writer.writerow([time.isoformat(), averagePrice])

                # push to mongodb
                postDocs(collection, docsToInsert)

                counter += 1

                if counter >= 10:
                    counter = 0
                    endReqTime = t.time()
                    elapsedTimeMs = (endReqTime - startReqTime) * 1000
                    print(
                        f"More than 10 requests made, let's measure time: {elapsedTimeMs}."
                    )
                    if elapsedTimeMs >= 10000:
                        print("Waiting 1 second..")
                        t.sleep(1)
                    startReqTime = t.time()

            print("Finished day.")


if __name__ == "__main__":
    main()
