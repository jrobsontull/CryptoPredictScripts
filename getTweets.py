import calendar
import requests
import datetime as dt
import calendar
import time as t
import csv
from pymongo import MongoClient
import sys
from dateutil import parser
from env import *
from colors import TColors as color

# Set up env file
print("Loading .env")
env = Env(".env")
print("Loaded.")

# Twitter API auth token
bearer_token = env.contents["TWITTER_API_TOKEN"]

# Establish MongoDB client
def mongoDbConnect():
    CONNECTION_STRING = env.contents["DB_CONNECTION_STRING"]
    client = MongoClient(CONNECTION_STRING)
    return client["btn"]


# Setup headers for twitter request
def establishTwitterOAuth(req):
    req.headers["Authorization"] = f"Bearer {bearer_token}"
    req.headers["User-Agent"] = "v2RecentSearchPython"
    return req


# Request tweets from Twitter API
# Returns tweet data and header limits
def twitterGet(startDt, endDt, maxTweets=10, nextToken=""):
    url = "https://api.twitter.com/2/tweets/search/all"
    params = {
        "query": "(bitcoin OR #bitcoin) -is:retweet lang:en",
        "tweet.fields": "created_at",
        "max_results": maxTweets,
        "start_time": startDt,
        "end_time": endDt,
    }
    if len(nextToken) > 1:
        params["next_token"] = nextToken

    response = requests.get(url, auth=establishTwitterOAuth, params=params)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    else:
        limits = {
            "remaining": response.headers["x-rate-limit-remaining"],
            "resetTime": response.headers["x-rate-limit-reset"],
        }
        return response.json(), limits


# Find number of days in a year
def defaultYearDays(year):
    days = 365
    if calendar.isleap(year):
        days = 366
    return days


def getDatetimeDaysForSearch(year, dayStart, dayEnd):
    dtPairs = list()
    startDate = dt.date(year, 1, 1)

    for i in range(dayStart, dayEnd + 1):
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
    for i in range(1, 48):
        tD1 = dt.timedelta(minutes=i * 30)
        tD2 = dt.timedelta(minutes=i * 30, seconds=1)
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
                "start": intervals[i].isoformat("T") + "Z",
                "end": intervals[i + 1].isoformat("T") + "Z",
                "average": averageInter.isoformat("T") + "Z",
            }
        )

    return intervalPairs


# Pause requests until limits reset
def checkApiLimits(limits):
    remaining = int(limits["remaining"])
    print(color.WARNING + "[Info]: " + color.ENDC + f"API remaining limit: {remaining}")
    if remaining <= 0:
        epochReset = dt.datetime.utcfromtimestamp(int(limits["resetTime"]))
        remainingTime = (epochReset - dt.datetime.utcnow()).total_seconds()
        remainingTime + 2  # add 2 more seconds for safety
        print(
            color.WARNING
            + "[Info]: "
            + color.ENDC
            + f"15 minute API limit reached. Sleeping for {remainingTime} before making further requests."
        )
        t.sleep(remainingTime)


# Sleep if too many requests in 1 second
def checkReqTimeLimit(start, end):
    elapsedTimeSeconds = end - start
    if elapsedTimeSeconds <= 1:
        print(
            color.WARNING
            + "[Info]: "
            + color.ENDC
            + f"Per second API limit reached. Waiting for {(1 - elapsedTimeSeconds + 0.1):.2f} seconds."
        )
        t.sleep(1 - elapsedTimeSeconds + 0.1)


# Update MongoDB database
def postDocs(collection, docs):
    try:
        collection.insert_many(docs)
    except Exception as err:
        print(f"Failed to insert document. {err}")


# Write tweets out and push to db
def processTweets(csvWriter, tweetsList, collection):
    docsToPush = list()

    for i in range(len(tweetsList) - 1, 0, -1):
        csvWriter.writerow(
            [
                tweetsList[i]["id"],
                tweetsList[i]["created_at"].replace("Z", ""),
                tweetsList[i]["text"].replace("\n", ""),
            ]
        )
        docsToPush.append(
            {
                "timestamp": parser.parse(tweetsList[i]["created_at"]),
                "tweetId": tweetsList[i]["id"],
                "text": tweetsList[i]["text"].replace("\n", ""),
            }
        )

    # Push to mongoDB
    postDocs(collection, docsToPush)


def main():
    # Establish connection to db
    print("Connecting to MongoDB...")
    dbConn = mongoDbConnect()
    if dbConn == None:
        print("Failed to connect to MongoDB.")
        sys.exit(1)
    else:
        print("Connection established.")
    collection = dbConn["tweets"]  # db collection

    searchYear = int(input("\nWhat year to grab tweets from? (XXXX)\n- "))

    startSearchDay = int(input("\nWhat day to start tweets from? (1-365)\n- "))
    endSearchDay = int(
        input("\nWhat day to start tweets from? (1-365 or 0 to calculate end)\n- ")
    )
    if endSearchDay == 0:
        endSearchDay = defaultYearDays(searchYear)

    days = getDatetimeDaysForSearch(searchYear, startSearchDay, endSearchDay)

    intervals = getTimeIntervalsPairsForDay(days)

    with open(
        f"{str(searchYear)}_tweets.csv", "w", newline="", encoding="UTF-8"
    ) as outFile:
        writer = csv.writer(outFile)
        writer.writerow(["tweetId", "timestamp", "text"])  # header

        for i, interval in enumerate(intervals):
            # Get first half hour in an interval
            print(
                color.OKGREEN
                + f"\n[Interval {i}]: "
                + color.ENDC
                + f"Processing {interval['start']} to {interval['average']}"
            )

            allTweets = list()

            nextToken = ""
            reqStartTime = t.time()
            response, limits = twitterGet(
                interval["start"], interval["end"], 10, nextToken
            )

            allTweets = response["data"]  # array of tweets

            reqEndTime = t.time()

            while (
                len(allTweets) < int(env.contents["TWEETS_PER_INTERVAL"])
                and response["meta"]["next_token"]
            ):
                # Keep grabbing tweets for this time interval
                print(
                    color.WARNING
                    + "[Info]: "
                    + color.ENDC
                    + f"Collecting more tweets to 20"
                )

                checkApiLimits(limits)
                checkReqTimeLimit(reqStartTime, reqEndTime)

                nextToken = response["meta"]["next_token"]
                tweetsLeftToGet = int(env.contents["TWEETS_PER_INTERVAL"]) - len(
                    allTweets
                )
                maxTweetsParam = 10 if tweetsLeftToGet < 10 else tweetsLeftToGet

                reqStartTime = t.time()
                response, limits = twitterGet(
                    interval["start"], interval["end"], maxTweetsParam, nextToken
                )
                reqEndTime = t.time()

                tweetsRes2 = response["data"]  # array of tweets
                allTweets += tweetsRes2

                reqEndTime = t.time()

            # Process data
            processTweets(writer, allTweets, collection)
            print(color.OKGREEN + "Interval finished\n" + color.ENDC)

            # Check API limits before proceeding
            checkReqTimeLimit(reqStartTime, reqEndTime)
            checkApiLimits(limits)


if __name__ == "__main__":
    main()
