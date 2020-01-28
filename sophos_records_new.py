#program to analyze stockmarket.sophos_records
from pymongo import MongoClient
import pprint   #pretty print
from datetime import datetime
from datetime import timedelta
from datetime import date
import calendar
import math

#connecting to mongodb server
client = MongoClient("localhost", 27017)

#using our required database
db = client.stockmarket #or client["stock_market"]

#print(db.list_collection_names())

"""
cursor = db.sophos_records.find()

cursor.next() =>gives next document
cursor.rewind() =>sets it to first document
below is to conver it to list
list contains json oject
"""

format= "%d/%m/%Y"



#print(records[0]["newDate"] < datetime.datetime.today())

""" Code to get no of days from current and display the results """

holidays = [date(2020, 1, 1), date(2020, 1, 26), date(2020, 2, 21), date(2020, 3, 25), date(2020, 5, 1), date(2020, 5, 25),
            date(2020, 8, 15),date(2020, 10, 2), date(2020, 10, 26), date(2020, 11, 1), date(2020, 11, 16), date(2020, 12, 25)]


days = int(input())

startDate = date.today() - timedelta(days=days)



"""
#for version 4

records = list(db.sophos_records.aggregate([
    {"$project":{"_id":0}},
    {"$addFields":{"newDate":{"$dateFromString":{"dateString":"$date", "format":format}}}},
    {"$match":{"newDate":{"$gte":startDate}}}
    ]))


"""
allRecords = list(db.sophos_records.aggregate([
    {"$project":{"_id":0}}
    ]))

records = []    #stores records which statisfy the given date
for i in allRecords:
    dateStr = i["date"]
    y = int(dateStr[6:])
    m = int(dateStr[3:5])
    d = int(dateStr[0:2])
    usageDate = date(y, m, d)
    if(usageDate >= startDate):
        i["date"] = usageDate
        records.append(i)




totalHolidaysCount = 0
totalHolidaysList = []      #weekends, holidays in the given range will be stored here
usageInfo = {}
for document in records:
    #print(type(document["newDate"]))
    recordDate = document["date"] #converting datetime.datetime to datetime.date
    #print(recordDate)
    if (recordDate in holidays) or (date.weekday(recordDate) == 5) or (date.weekday(recordDate) == 6):
        if(recordDate not in totalHolidaysList):
            totalHolidaysList.append(recordDate)
    else:
        macAddr = document["hwaddr"]
        deviceName = document["device_name"]
        userName = document["name"]
        totalConnTime = document["total_connected_time"]
        seconds = (int(totalConnTime[0:2]) * 3600) + (int(totalConnTime[3:5]) * 60) + (int(totalConnTime[6:]))
        if macAddr in usageInfo:
            if recordDate not in usageInfo[macAddr]["dates"]:
                usageInfo[macAddr]["totalSeconds"]+=seconds
                usageInfo[macAddr]["activeDaysCount"]+=1
            else:
                usageInfo[macAddr]["totalSeconds"]+=seconds
        else:
            usageInfo[macAddr] = {"macAddr":macAddr, "totalSeconds":seconds, "activeDaysCount":1, "deviceName":deviceName, "userName":userName, "dates":[recordDate]}
            



totalWorkingDays = days - len(totalHolidaysList)        #by subtracting weekends, holidays from the total days

finalString = 'macAddres, deviceName, totalWorkingDays, activeDays, totalConnectedTime, avgConnectedTime\n';            
for user in usageInfo:
    totalSec = usageInfo[user]["totalSeconds"]
    #days = totalSec / (3600 * 24)   #to convert into days each day has 24*3600 seconds  
    hours = totalSec  // 3600 #convert remaining seconds into hours
    minutes = (totalSec  % 3600) // 60 #convert remaining seconds into minutes
    sec = (totalSec % 3600) % 60 #remaining seconds
    avgTotalSec = math.floor(totalSec / usageInfo[user]["activeDaysCount"])
    avgHours = avgTotalSec  // 3600 #convert remaining seconds into hours
    avgMinutes = (avgTotalSec  % 3600) // 60 #convert remaining seconds into minutes
    avgSec = (avgTotalSec % 3600) % 60 #remaining seconds
    #finalString += usageInfo[user]["macAddr"] +","+usageInfo[user]["deviceName"]+","+str(totalWorkingDays)+","+str(usageInfo[user]["activeDaysCount"])+","+str(hours)+":"+minutes+":"+sec+","+avgHours+":"+avgHours+":"+avgSec+"\n"
    
    
    finalString+="{},{},{},{},{}:{}:{},{}:{}:{}".format(usageInfo[user]["macAddr"], usageInfo[user]["deviceName"],totalWorkingDays, usageInfo[user]["activeDaysCount"],
                                                                                                    hours, minutes, sec, avgHours, avgMinutes, avgSec)

    #for i in range(0,len(totalHolidaysList)):
        #totalHolidaysList[i] = str(totalHolidaysList[i])
    #holidayStr = "-".join(totalHolidaysList)
    #finalString+=","+holidayStr
    finalString+="\n"

file = open("/home/user/Desktop/data.csv","w")
file.write(finalString)
file.close()
        






