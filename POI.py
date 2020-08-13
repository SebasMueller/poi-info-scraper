import requests
import urllib.request
import json
import html
import re
from haversine import haversine, haversine_vector
import heapq
import pandas as pd
from dateutil import parser
limit = 1
radius = 6
# lat = 52.635875 #uk - norfolk
# lng = 1.301 #uk - norfolk
lat = 52.50003299 #germany - berlin
lng = 13.3913285 #germany - berlin

def set_up_rewe_database():
    API_URL = "https://www.rewe.de/market/content/marketsearch"
    headers = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-language": "en-US,en;q=0.9",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1" 
              }
    pageStoreCount = 1
    page = 0
    reweArray = []
    intermediaryDayKeys = { 'MONDAY' : 0, 'TUESDAY' : 1, 'WEDNESDAY' : 2, 'THURSDAY' : 3, 'FRIDAY' : 4, 'SATURDAY' : 5, 'SUNDAY' : 6, None : None}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    while pageStoreCount > 0:
        params = {  'searchString' : "REWE",
                    'pageSize' : 500,
                    'page' : page}
        try:
            req_url = API_URL + '?' + '&'.join(k + '=' + str(v) for k, v in params.items())
            rq = urllib.request.urlopen(urllib.request.Request(url=req_url, data=None, headers=headers))
            if rq.status != 200:
                return False
            data = rq.read().decode('utf-8')
            res = json.loads(data)
            if res["total"] == 0:
                return False
            res = res["markets"]
            pageStoreCount = len(res)
        except:
            return False
        # print(res[347]["openingHours"]["dayAndTimeRanges"])
        for index in range(len(res)):
            dayDoneSet = set()
            daysHeap = []
            openingHours = res[index]["openingHours"]["dayAndTimeRanges"]
            for dayRange in range(len(openingHours)):
                startDay = intermediaryDayKeys[openingHours[dayRange]["startDay"]]
                endDay = intermediaryDayKeys[openingHours[dayRange]["endDay"]]
                opens = openingHours[dayRange]["opens"]
                closes = openingHours[dayRange]["closes"]
                actualHours = [{'open' : opens, 'close' : closes}]
                #deal with 1 day ranges
                if endDay == None:
                    endDay = startDay
                #deal with wrap around windows e.g. sunday to tuesday
                if startDay > endDay:
                    startDay += -7
                for day in range(startDay, endDay+1):
                    if day < 0:
                        day += 7
                    #if statement to deal with incorrect responses where multiple ranges cover the same day (implemnted due to a bug in their system)
                    if day not in dayDoneSet:
                        dayDoneSet.add(day)
                        keyHours = {'day' : dayKeys[day],
                        'open' : True,
                        'hours' : actualHours
                        }
                        heapq.heappush(daysHeap, [day,keyHours])
            # simple heap method to check for any missing days (method which could be deployed to all other API functions quite easily)
            checkedUpToDay = -1
            for day in heapq.nsmallest(7, daysHeap):
                checkedUpToDay += 1
                while day[0] > checkedUpToDay:
                    closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                            'open' : False  }
                    heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
                    checkedUpToDay += 1
            while len(daysHeap) < 7:
                if len(daysHeap) > 0:
                    checkedUpToDay = daysHeap[-1][0] + 1
                else:
                    # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
                    checkedUpToDay = 0
                closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                            'open' : False  }
                heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            if len(daysHeap) != 7:
                return False
            hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
            coords = res[index]["geoLocation"]
            reweArray.append([(float(coords["latitude"]),float(coords["longitude"])), hoursArray])
        page += 1
    reweDB = pd.DataFrame(reweArray, columns=['Coordinates', 'OpeningHours'])
    reweDB.to_csv("REWE.csv")
    return True

def set_up_netto_database():
    API_URL = "https://netto.de/umbraco/api/StoresData/StoresV2"
    nettoArray = []
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    try:
        rq = requests.get("https://netto.de/umbraco/api/StoresData/StoresV2")
        res = rq.json()
        if len(res) == 0:
            return False
    except:
        return False
    for index in range(len(res)):
        dayDoneSet = set()
        daysHeap = []
        openingHours = res[index]["hours"]
        for dayIndex in range(len(openingHours)):
            #potential weakness if a bugged date is retrieved and the datetime parser raises an exception?
            day = parser.parse(openingHours[dayIndex]["date"]).weekday()
            if day not in dayDoneSet:
                try:
                    dayDoneSet.add(day)
                    #keyHours processing code
                    if openingHours[dayIndex]["closed"] == False:
                        opens = parser.parse(openingHours[dayIndex]["open"]).strftime("%H:%M")
                        closes = parser.parse(openingHours[dayIndex]["close"]).strftime("%H:%M")
                        actualHours = {'open' : opens,
                                'close' : closes}
                        keyHours = {'day' : dayKeys[day],
                        'open' : True,
                        'hours' : [actualHours]
                        }
                        heapq.heappush(daysHeap, [day,keyHours])
                    else:
                        closedDayHours = {  'day' : dayKeys[day],
                        'open' : False  }
                        heapq.heappush(daysHeap, [day, closedDayHours])
                except:
                    closedDayHours = {  'day' : dayKeys[day],
                        'open' : False  }
                    heapq.heappush(daysHeap, [day, closedDayHours])
        #Check for any missing days and ensure all days are in order for insertion into our database
        checkedUpToDay = -1
        for day in heapq.nsmallest(7, daysHeap):
            checkedUpToDay += 1
            while day[0] > checkedUpToDay:
                closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                        'open' : False  }
                heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
                checkedUpToDay += 1
        while len(daysHeap) < 7:
            if len(daysHeap) > 0:
                checkedUpToDay = daysHeap[-1][0] + 1
            else:
                # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
                checkedUpToDay = 0
                closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                            'open' : False  }
                heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
        if len(daysHeap) != 7:
            return False
        hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
        coords = res[index]["coordinates"]
        nettoArray.append([(float(coords[1]),float(coords[0])), hoursArray])
    nettoDB = pd.DataFrame(nettoArray, columns=['Coordinates', 'OpeningHours'])
    nettoDB.to_csv("Netto.csv")
    return True



def get_rewe_data(lat, lng):
    desiredCoords = [lat, lng]
    reweDB = pd.read_csv("REWE.csv", index_col="Unnamed: 0", converters={'Coordinates': eval, 'OpeningHours' : eval})
    reweDB["Distances"] = haversine_vector([desiredCoords]*len(reweDB["Coordinates"]), list(reweDB["Coordinates"]))
    closestStoreIndex = reweDB["Distances"].idxmin()
    return reweDB["OpeningHours"][closestStoreIndex]

def get_netto_data(lat, lng):
    #pass lat and lng as arrays of length 1 if you want the storeDistance to be returned too
    if isinstance(lat, list) :
        returnDistanceToo = True
        lat = lat[0]
        lng = lng[0]
    else:
        returnDistanceToo = False
    desiredCoords = [lat, lng]
    nettoDB = pd.read_csv("Netto.csv", index_col="Unnamed: 0", converters={'Coordinates': eval, 'OpeningHours' : eval})
    nettoDB["Distances"] = haversine_vector([desiredCoords]*len(nettoDB["Coordinates"]), list(nettoDB["Coordinates"]))
    closestStoreIndex = nettoDB["Distances"].idxmin()
    if returnDistanceToo:
        return nettoDB["Distances"][closestStoreIndex], nettoDB["OpeningHours"][closestStoreIndex]
    else:
        return nettoDB["OpeningHours"][closestStoreIndex]
    
def get_netto_marken_discount_data(lat, lng):
    #pass lat and lng as arrays of length 1 if you want the storeDistance to be returned too
    if isinstance(lat, list) :
        returnDistanceToo = True
        lat = lat[0]
        lng = lng[0]
    else:
        returnDistanceToo = False
    API_URL = "https://www.netto-online.de/INTERSHOP/web/WFS/Plus-NettoDE-Site/de_DE/-/EUR/ViewNettoStoreFinder-GetStoreItems"
        #The conversion rates below have been estimated through experimentation, and should approximately preserve radius (although the geometry will be a cross between a square and a circle)
    params = {  's' : float(lat) - radius / 70,
                'n' : float(lat) + radius / 70,
                'w' : float(lng) - radius / 110,
                'e' : float(lng) + radius / 110
             }
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        return False
    try:
        res = rq.json()
        if len(res) == 0:
            print(2)
            return False
    except:
        return False
    storeHeap = []
    for index in range(len(res)):
        storeLat, storeLng = float(res[index]["coord_latitude"]), float(res[index]["coord_longitude"])
        #compute distance between the two points using the haversine function
        storeDistance = haversine((lat, lng),(storeLat, storeLng))
        heapq.heappush(storeHeap, [storeDistance,res[index]])
    res = heapq.nsmallest(limit, storeHeap)
    i = 0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!!
    #e.g.:
    # while True:
    #     if name != res[0]['name'] or res[0]['other_name']:
    #         i += 1
    #     else:
    #         break
    intermediaryDayKeys = { 'Mo.' : 0, 'Di.' : 1, 'Mi.' : 2, 'Do.' : 3, 'Fr.' : 4, 'Sa.' : 5, 'So.' : 6, None : None}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    dayDoneSet = set()
    daysHeap = []
    storeDistance = res[i][0]
    openingHours = res[i][1]["store_opening"]
    dayRanges = openingHours.split("<br />")[:-1]
    for dayRange in range(len(dayRanges)):
        daysAndTimes = dayRanges[dayRange].split(":")
        days = daysAndTimes[0]
        days = days.split("-")
        startDay = intermediaryDayKeys[days[0]]
        if len(days) == 1:
            endDay = startDay
        elif len(days) == 2:
            endDay = intermediaryDayKeys[days[1]]
        else:
            return False
        #deal with wrap around windows e.g. sunday to tuesday
        if startDay > endDay:
            startDay += -7
        times = daysAndTimes[1]
        if "geschlossen" in times:
            for day in range(startDay, endDay+1):
                if day < 0:
                    day += 7
                #if statement to deal with incorrect responses where multiple ranges cover the same day (implemnted due to a bug in their system)
                if day not in dayDoneSet:
                    dayDoneSet.add(day)
                    closedDayHours = {  'day' : dayKeys[day],
                                'open' : False  }
                    heapq.heappush(daysHeap, [day, closedDayHours])
        else:
            try:
                times = times.replace(".",":")
                times = times.split()
                if len(times) != 4:
                    #means this method is non exhaustive (e.g. multiple time slots) and must be amended 
                    return False
                opens = times[0]
                closes = times[2]
                if len(opens) == 4:
                    opens = "0" + opens
                if len(closes) == 4:
                    closes = "0" + closes
                if len(opens) != 5 or len(closes) != 5:
                    #means this method is non exhaustive (e.g. doesnt catch store closure) and must be amended 
                    return False
                actualHours = [{'open' : opens, 'close' : closes}]
                for day in range(startDay, endDay+1):
                    if day < 0:
                        day += 7
                    #if statement to deal with incorrect responses where multiple ranges cover the same day (implemnted due to a bug in their system)
                    if day not in dayDoneSet:
                        dayDoneSet.add(day)
                        keyHours = {'day' : dayKeys[day],
                        'open' : True,
                        'hours' : actualHours
                        }
                        heapq.heappush(daysHeap, [day,keyHours])
            except:
                for day in range(startDay, endDay+1):
                    if day < 0:
                        day += 7
                    #if statement to deal with incorrect responses where multiple ranges cover the same day (implemnted due to a bug in their system)
                    if day not in dayDoneSet:
                        dayDoneSet.add(day)
                        closedDayHours = {  'day' : dayKeys[day],
                                    'open' : False  }
                        heapq.heappush(daysHeap, [day, closedDayHours])
    # simple heap method to check for any missing days (method which could be deployed to all other API functions quite easily)
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                        'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    if returnDistanceToo:
        return storeDistance, hoursArray
    else:
        return hoursArray

def get_netto_brands_data(lat, lng):
    netto = get_netto_data([lat], [lng])
    nettoMD = get_netto_marken_discount_data([lat], [lng])
    if netto[0] < nettoMD[0]:
        return netto[1]
    else:
        return nettoMD[1]

def get_sainsburys_data(lat,lng):
    API_URL = "https://stores.sainsburys.co.uk/api/v1/stores/"
    params = {
        'fields': 'slfe-list-2.21',
        'api_client_id': 'slfe',
        'store_type': 'main,local',
        'sort': 'by_distance',
        'within': radius,
        'limit': limit,
        'page': '1',
        'lat': lat,
        'lon': lng,
    }
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        # print(rq.json())
        return False
    try:
        res = rq.json()
        if res["page_meta"]["total"] == 0:
            return False
        res = res["results"]
    except:
        return False
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (res[0]['name'] or res[0]['other_name'])
    #e.g.:
    # while True:
    #     if name != res[0]['name'] or res[0]['other_name']:
    #         i += 1
    #     else:
    #         break
    if res[i]["distance"] > radius * 0.621371:
        return False
    openingHours = res[i]['opening_times']
    dayDoneSet = set()
    daysHeap = []
    for index in range(len(openingHours)):
        key = openingHours[index]['day']
        #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
        if key not in dayDoneSet:
            dayDoneSet.add(key)
            #keyHours processing code
            actualHours = []
            for timeSlotNumber in range(len(openingHours[index]['times'])):
                try:
                    opens = openingHours[index]['times'][timeSlotNumber]['start_time']
                    closes = openingHours[index]['times'][timeSlotNumber]['end_time']
                    actualHoursDict = { 'open' : opens,
                                        'close' : closes}
                    actualHours.append(actualHoursDict)
                except:
                    #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
                    closedDayHours = {  'day' : dayKeys[key],
                                    'open' : False  }
                    heapq.heappush(daysHeap, [key,closedDayHours])
                    break
            keyHours = {'day' : dayKeys[key],
                    'open' : True,
                    'hours' : actualHours
                    }
            heapq.heappush(daysHeap, [key,keyHours])
    #Check for any missing days and ensure all days are in order for insertion into our database
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
        closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
        heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    return hoursArray
    
def get_asda_data(lat,lng):
    import json
    API_URL = "https://storelocator.asda.com/index.html"
    params = { 'q' : "{0},{1}".format(lat,lng)}
    headers = {
    "accept": "application/json",
  }
    rq = requests.get(API_URL,params=params, headers = headers)
    if rq.status_code != 200:
        print(rq.json())
        return False
    # res = json.dumps(rq.text)
    try:
        res = rq.json()["response"]
        if res["count"] == 0:
            return False
    except:
        return False
    i = 0 
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (res["entities"][0]["name"])
    #e.g.:
    # while True:
    #     if name != res["entities"][i]["name"]:
    #         i += 1
    #     else:
    #         break
    if res["entities"][i]["distance"]["distanceKilometers"] > radius:
        return False
    openingHours = res["entities"][i]["profile"]["hours"]["normalHours"]
    hoursArray = []
    intermediaryDayKeys = { 'MONDAY' : 0, 'TUESDAY' : 1, 'WEDNESDAY' : 2, 'THURSDAY' : 3, 'FRIDAY' : 4, 'SATURDAY' : 5, 'SUNDAY' : 6}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    dayDoneSet = set()
    daysHeap = []
    for index in range(len(openingHours)):
            day = intermediaryDayKeys[openingHours[index]["day"]]
            #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if day not in dayDoneSet:
                dayDoneSet.add(day)
                if openingHours[index]["isClosed"] != False:
                    closedDayHours = {  'day' : dayKeys[day],
                                        'open' : False  }
                    heapq.heappush(daysHeap, [day,closedDayHours])
                else:
                    actualHours = []
                    for timeSlotNumber in range(len(openingHours[index]['intervals'])):
                        try:
                            actualHoursDict = {}
                            start = str(openingHours[index]['intervals'][timeSlotNumber]['start'])
                            if start == "0":
                                start = "00:00"
                            elif len(start) == 3:
                                start = "0{0}:{1}".format(start[0],start[1:])
                            else:
                                start = "{0}:{1}".format(start[:2],start[2:])
                            end = str(openingHours[index]['intervals'][timeSlotNumber]['end'])
                            if end == "0":
                                end = "00:00"
                            elif len(end) == 3:
                                end = "0{0}:{1}".format(end[0],end[1:])
                            else:
                                end = "{0}:{1}".format(end[:2],end[2:])
                            actualHoursDict['open'] = start
                            actualHoursDict['close'] = end
                            actualHours.append(actualHoursDict)
                        except:
                            #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
                            closedDayHours = {  'day' : dayKeys[day],
                                            'open' : False  }
                            heapq.heappush(daysHeap, [day,closedDayHours])
                            break
                    keyHours = {'day' : dayKeys[day],
                            'open' : True,
                            'hours' : actualHours
                            }
                    heapq.heappush(daysHeap, [day,keyHours])

    #Check for any missing days and ensure all days are in order for insertion into our database     
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
        closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
        heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    return hoursArray

def get_tesco_data(lat, lng):
    API_URL = "https://api.tesco.com/tescolocation/v3/locations/search"
    params = {  'offset' : 0,
                'limit' : limit,
                'sort' : 'near:"{0},{1}"'.format(lat,lng),
                'filter' : "category:Store AND isoCountryCode:x-uk",
                'fields' : "name,geo,openingHours"
                #known fields: "name,geo,openingHours,altIds.branchNumber,contact,facilities"
             }
    headers = {"x-appkey": "store-locator-web-cde"}
    rq = requests.get(API_URL, params=params, headers=headers)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()['results']
        if len(res) == 0:
            return False
    except:
        return False
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (res[0]['location']['name'])
    #e.g.:
    # while True:
    #     if name != res[i]['location']['name']:
    #         i += 1
    #     else:
    #         break
    if res[i]["distanceFrom"]["value"] > 0.621371*radius:
        return False
    openingHours = res[i]['location']['openingHours'][0]['standardOpeningHours']
    dayKeys = { 'mo' : 'Monday', 'tu' : 'Tuesday', 'we' : 'Wednesday', 'th' : 'Thursday', 'fr' : 'Friday', 'sa' : 'Saturday', 'su' : 'Sunday'}
    hoursArray = []
    for key in ['mo','tu','we','th','fr','sa','su']:
        try:
            if openingHours[key]['isOpen'] == 'true':
                actualHours = {'open' : openingHours[key]['open'][:2] + ":" + openingHours[key]['open'][2:],
                            'close' : openingHours[key]['close'][:2] + ":" + openingHours[key]['close'][2:]}
                keyHours = {'day' : dayKeys[key],
                        'open' : True,
                        'hours' : [actualHours]
                        }
            else:
                keyHours = {'day' : dayKeys[key],
                        'open' : False}
        except:
            keyHours = {'day' : dayKeys[key],
                        'open' : False}

        hoursArray.append(keyHours)
    return hoursArray

def get_morrisons_data(lat, lng):
    apikey = "kxBdM2chFwZjNvG2PwnSn3sj6C53dLEY"
    API_URL = "https://api.morrisons.com/location/v2//stores"
    params = { 'apikey' : apikey,
                'distance' : radius*1000,
                'lat' : lat,
                'lon' : lng,
                'limit' : limit,
                'offset' : 0,
                'storeformat' : "supermarket"
             }
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()["stores"]
        if len(res) == 0:
            return False
    except:
        return False
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. "Morrisons" + res[0]["storename"])
    #e.g.:
    # while True:
    #     if name != "Morrisons" + res[i]["storename"]:
    #         i += 1
    #     else:
    #         break
    if res[i]["distance"] > radius * 1000:
        return False 
    openingHours = res[i]['openingTimes']
    dayKeys = { 'mon' : 'Monday', 'tue' : 'Tuesday', 'wed' : 'Wednesday', 'thu' : 'Thursday', 'fri' : 'Friday', 'sat' : 'Saturday', 'sun' : 'Sunday'}
    hoursArray = []
    for key in ['mon','tue','wed','thu','fri','sat','sun']:
        try:
            actualHours = {'open' : openingHours[key]['open'][:5],
                        'close' : openingHours[key]['close'][:5]}
            keyHours = {'day' : dayKeys[key],
                    'open' : True,
                    'hours' : [actualHours]
                    }
        except:
            keyHours = {'day' : dayKeys[key],
                        'open' : False}

        hoursArray.append(keyHours)
    return hoursArray

def get_waitrose_data(lat, lng):
    API_URL_locations = "https://www.waitrose.com/shop/NearestBranchesCmd"
    API_URL_hours = "https://www.waitrose.com/shop/StandardWorkingHoursView"
    params_locations = {'latitude' : lat,
                        'longitude' : lng,
                        }
    rq = requests.get(API_URL_locations, params=params_locations)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()
        if not res['success'] or len(res['branchList']) == 0:
            return False
        res = res["branchList"]
    except:
        return False
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. "Waitrose" + res[0]["branchName"])
    #e.g.:
    # while True:
    #     if name != "Waitrose" + res[i]["branchName"]:
    #         i += 1
    #     else:
    #         break
    storeLat, storeLng = res[i]["latitude"], res[i]["longitude"]
    #compute distance between the two points using the haversine function
    storeDistance = haversine((lat, lng),(storeLat, storeLng))
    if storeDistance > radius:
        return False
    branchId = res[i]['branchId']
    params_hours = { 'branchId' : branchId}
    rq = requests.get(API_URL_hours, params=params_hours)
    hoursStringHTML = rq.text
    cleanr = re.compile('<.*?>|\s')
    hoursString = re.sub(cleanr, '', hoursStringHTML)
    #This section is to ensure if the shop is closed, correct times are displayed
    sundayIndex = hoursString.find("Sunday:")
    mondayIndex = hoursString.find("Monday:")
    tuesdayIndex = hoursString.find("Tuesday:")
    wednesdayIndex = hoursString.find("Wednesday:")
    thursdayIndex = hoursString.find("Thursday:")
    fridayIndex = hoursString.find("Friday:")
    saturdayIndex = hoursString.find("Saturday:")

    hoursArray = []
    #equivalent of the usual for loop V
    if mondayIndex == -1:
        keyHours = {'day' : "Monday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[mondayIndex+7:tuesdayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Monday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Monday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Monday",
                        'open' : False}
            hoursArray.append(keyHours)
            

    if tuesdayIndex == -1:
        keyHours = {'day' : "Tuesday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[tuesdayIndex+8:wednesdayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Tuesday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Tuesday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Tuesday",
                        'open' : False}
            hoursArray.append(keyHours)
    if wednesdayIndex == -1:
        keyHours = {'day' : "Wednesday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[wednesdayIndex+10:thursdayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Wednesday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Wednesday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Wednesday",
                        'open' : False}
            hoursArray.append(keyHours)
        
    if thursdayIndex == -1:
        keyHours = {'day' : "Thursday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[thursdayIndex+9:fridayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Thursday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Thursday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Thursday",
                        'open' : False}
            hoursArray.append(keyHours)
    if fridayIndex == -1:
        keyHours = {'day' : "Friday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[fridayIndex+7:saturdayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Friday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Friday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Friday",
                        'open' : False}
            hoursArray.append(keyHours)
    if saturdayIndex == -1:
        keyHours = {'day' : "Saturday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[saturdayIndex+9:]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Saturday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Saturday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Saturday",
                        'open' : False}
            hoursArray.append(keyHours)
    if sundayIndex == -1:
        keyHours = {'day' : "Sunday",
                        'open' : False}
        hoursArray.append(keyHours)
    else:
        try:
            day = hoursString[sundayIndex+7:mondayIndex]
            if day.find("los") != -1: #check if the world close or Close or closed or Closed or Closure etc.. appears
                keyHours = {'day' : "Sunday",
                        'open' : False}
                hoursArray.append(keyHours)
            firstDigitIndex = [x.isdigit() for x in day].index(True)
            actualHours = { 'open' : day[firstDigitIndex:firstDigitIndex+5],
                            'close' : day[firstDigitIndex+6:firstDigitIndex+11]}
            keyHours = {'day' : "Sunday",
                        'open' : True,
                        'hours' : actualHours}
            hoursArray.append(keyHours)
        except:
            keyHours = {'day' : "Sunday",
                        'open' : False}
            hoursArray.append(keyHours)
    return hoursArray

def get_aldi_data(lat, lng):
    API_URL = "https://www.aldi.co.uk/api/store-finder/search"
    params = {  'latitude' : lat,
                'longitude' : lng,
                'fromMultipleBranch' : False
             }
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()["results"]
        if len(res) == 0:
            return False
    except:
        return False
    i=0
    intermediaryDayKeys = { 'Mon' : 0, 'Tue' : 1, 'Wed' : 2, 'Thu' : 3, 'Fri' : 4, 'Sat' : 5, 'Sun' : 6}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0]["name"])
    #e.g.:
    # while True:
    #     if name != res[i]["name"]:
    #         i += 1
    #     else:
    #         break
    distance = float(res[i]["distance"].split(" ")[0]) * 1.60934
    if distance > radius:
        return False
    openingHours = res[i]["openingTimes"]
    hoursArray = []
    dayDoneSet = set()
    daysHeap = []
    for index in range(len(openingHours)):
            day = intermediaryDayKeys[openingHours[index]["day"]]
            #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if day not in dayDoneSet:
                dayDoneSet.add(day)
                #keyHours processing code
                key = intermediaryDayKeys[openingHours[index]['day']]  #turn key into int
                actualHours = []
                if openingHours[index]['closed'] == False:
                    hoursString = html.unescape(openingHours[index]['hours'])
                    while len(hoursString) > 11: #incase there are multiple time slots encoded within the string (format is unknown hence the strange code)
                        timeSlot = { 'open' : hoursString[:5],
                                        'close' : hoursString[8:13]}
                        actualHours.append(timeSlot)
                        hoursString = hoursString[13:]
                        if len(hoursString) > 0:
                            try:
                                firstDigitIndex = [x.isdigit() for x in hoursString].index(True)
                                hoursString = hoursString[firstDigitIndex:]
                            except:
                                break
                    keyHours = {'day' : dayKeys[key],
                        'open' : True,
                        'hours' : actualHours
                        }
                else:
                    keyHours = {'day' : dayKeys[key],
                        'open' : False
                        }
                heapq.heappush(daysHeap, [key, keyHours])
    #Check for any missing days and ensure all days are in order for insertion into our database
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
        closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
        heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    return hoursArray

def get_coop_data(lat, lng):
    API_URL = "https://api.coop.co.uk/locationservices/finder/food/"
    params = {  'location' : "{0},{1}".format(lat,lng),
                'distance' : radius*1000,
                'min_distance' : "0",
                'min_results' : "0",
                'format' : "json"}
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()
        if res['count'] == 0:
            return False
        res = res["results"]
    except:
        return False
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0]["name"])
    #e.g.:
    # while True:
    #     if name != res[i]["name"]:
    #         i += 1
    #     else:
    #         break
    if res[i]["distance"]["m"] > radius * 1000:
        return False
    openingHours = res[i]["opening_hours"]
    intermediaryDayKeys = { 'Monday' : 0, 'Tuesday' : 1, 'Wednesday' : 2, 'Thursday' : 3, 'Friday' : 4, 'Saturday' : 5, 'Sunday' : 6}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    dayDoneSet = set()
    daysHeap = []
    for index in range(len(openingHours)):
            key = intermediaryDayKeys[openingHours[index]['name']]  #turn key into int
            #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if key not in dayDoneSet:
                dayDoneSet.add(key)
                #keyHours processing code
                actualHours = []
                try:
                    if openingHours[index]["label"].find("los") != -1:
                        #incase the world closed/closure/Closed/Closure is in the label
                        closedDayHours = {  'day' : dayKeys[key],
                                    'open' : False  }
                        heapq.heappush(daysHeap, [key, closedDayHours])
                    else:
                        actualHoursDict = {}
                        actualHoursDict['open'] = openingHours[index]['opens']
                        actualHoursDict['close'] = openingHours[index]['closes']
                        actualHours = [actualHoursDict]
                        keyHours = {'day' : dayKeys[key],
                            'open' : True,
                            'hours' : actualHours
                            }
                        heapq.heappush(daysHeap, [key, keyHours])
                except:
                    #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
                    closedDayHours = {  'day' : dayKeys[key],
                                    'open' : False  }
                    heapq.heappush(daysHeap, [key, closedDayHours])
    # Check for any missing days and ensure all days are in order for insertion into our database
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
        closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
        heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    return hoursArray

def get_marks_and_spencers_data(lat, lng):
    API_URL = "https://api.marksandspencer.com/v1/stores"
    params = {  'apikey' : "aVCi8dmPbHgHrdCv9gNt6rusFK98VokK",
                'latlong' : "{0},{1}".format(lat,lng),
                'limit' : limit,
                'radius' : radius}
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.text)
        return False
    try:
        res = rq.json()
        if res['count'] == 0:
            return False
        res = res["results"]
    except:
        return False
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0]["name"])
    #e.g.:
    # while True:
    #     if name != res[i]["name"]:
    #         i += 1
    #     else:
    #         break
    if res[i]["distance"] > radius * 1000:
        return False
    openingHours = res[i]["coreOpeningHours"]
    intermediaryDayKeys = { 'Monday' : 0, 'Tuesday' : 1, 'Wednesday' : 2, 'Thursday' : 3, 'Friday' : 4, 'Saturday' : 5, 'Sunday' : 6}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    dayDoneSet = set()
    daysHeap = []
    for index in range(len(openingHours)):
            key = intermediaryDayKeys[openingHours[index]['day']]  #turn key into int
            #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if key not in dayDoneSet:
                dayDoneSet.add(key)
            #keyHours processing code
                try:
                    actualHoursDict = {}
                    actualHoursDict['open'] = openingHours[index]['open']
                    actualHoursDict['close'] = openingHours[index]['close']
                    actualHours = [actualHoursDict]
                    keyHours = {'day' : dayKeys[key],
                        'open' : True,
                        'hours' : actualHours
                        }
                    heapq.heappush(daysHeap, [key,keyHours])
                except:
                    #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
                    closedDayHours = {  'day' : dayKeys[key],
                                    'open' : False  }
                    heapq.heappush(daysHeap, [key,closedDayHours])
    # Check for any missing days and ensure all days are in order for insertion into our database
    checkedUpToDay = -1
    for day in heapq.nsmallest(7, daysHeap):
        checkedUpToDay += 1
        while day[0] > checkedUpToDay:
            closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
            heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
            checkedUpToDay += 1
    while len(daysHeap) < 7:
        if len(daysHeap) > 0:
            checkedUpToDay = daysHeap[-1][0] + 1
        else:
            # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
            checkedUpToDay = 0
        closedDayHours = {  'day' : dayKeys[checkedUpToDay],
                    'open' : False  }
        heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
    if len(daysHeap) != 7:
        return False
    hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
    return hoursArray

def get_iceland_data(lat, lng):
    API_URL = "https://www.iceland.co.uk/on/demandware.store/Sites-icelandfoodsuk-Site/default/Stores-GetNearestStores"
    params = {  'latitude' : lat,
                'longitude' : lng,
                'countryCode' : "GB",
                'distanceUnit' : "km", #NOTE might need to change to "mi" for api to function?
                'maxdistance' : radius}
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()
        if len(res['stores']) == 0:
            return False
        res = res["stores"]
    except:
        return False
    storeHeap = []
    for index in res:
        storeLat, storeLng = float(res[index]["latitude"]), float(res[index]["longitude"])
        #compute distance between the two points using the haversine function
        storeDistance = haversine((lat, lng),(storeLat, storeLng))
        heapq.heappush(storeHeap, [storeDistance,res[index]])
    res = heapq.nsmallest(limit, storeHeap)
    i = 0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0][1]["name"])
    #e.g.:
    # while True:
    #     if name != res[i][1]["name"]:
    #         i += 1
    #     else:
    #         break 
    if res[i][0] > radius:
        return False
    openingHours = res[i][1]["storeHours"]
    cleanr = re.compile('<.*?>|\s|-')
    hoursString = re.sub(cleanr, '', openingHours)
    openingHoursArray = hoursString.split("day")[1:]
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    hoursArray = []
    for index in range(len(openingHoursArray)):
        openingHoursArray[index] = openingHoursArray[index].split("M")[:2]
        for index1 in [0,1]:
            try:
                word = openingHoursArray[index][index1]
                wordSplit = word.split(":")
                if word[-1] == 'A':
                    if wordSplit[0] == "12":
                        word = "00:{0}".format(wordSplit[1][:-1])
                    elif len(wordSplit[0]) == 1:
                        word = "0" + word[:-1]
                    elif len(wordSplit[0]) == 2:
                        word = word[:-1]
                    else: 
                        word = False
                elif word[-1] == 'P':
                    if wordSplit[0] == "12":
                        word = word[:-1]
                    elif len(wordSplit[0]) < 3:
                        word = str( int(wordSplit[0]) + 12) + ":" + wordSplit[1][:-1]
                    else:
                        word = False
                openingHoursArray[index][index1] = word
            except:
                openingHoursArray[index][index1] = False
        if not openingHoursArray[index][0] or not openingHoursArray[index][1]:
            closedDayHours = {  'day' : dayKeys[index],
                                'open' : False  }
            hoursArray.append(closedDayHours)
        else:
            actualHoursDict = {}
            actualHoursDict['open'] = openingHoursArray[index][0]
            actualHoursDict['close'] = openingHoursArray[index][1]
            actualHours = [actualHoursDict]
            keyHours = {'day' : dayKeys[index],
                'open' : True,
                'hours' : actualHours
                }
            hoursArray.append(keyHours)
    return hoursArray

def get_edeka_data(lat, lng):
    API_URL = "https://www.edeka.de/api/marketsearch/markets"
    params = {  'coordinates' : "lat={0}&lon={1}".format(lat, lng)}
    rq = requests.get(API_URL, params=params)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()
        if res["totalCount"] == 0 or len(res['markets']) == 0:
            return False
        res = res["markets"]
    except:
        return False
    i = 0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0]["name"])
    #e.g.:
    # while True:
    #     if name != res[i]["name"]:
    #         i += 1
    #     else:
    #         break 
    storeLat, storeLng = float(res[i]["coordinates"]["lat"]), float(res[i]["coordinates"]["lon"])
    #compute distance between the two points using the haversine function
    storeDistance = haversine((lat, lng),(storeLat, storeLng))
    if storeDistance > radius:
        return False
    openingHours = res[i]["businessHours"]
    dayKeys = { 'monday' : 'Monday', 'tuesday' : 'Tuesday', 'wednesday' : 'Wednesday', 'thursday' : 'Thursday', 'friday' : 'Friday', 'saturday' : 'Saturday', 'sunday' : 'Sunday'}
    hoursArray = []
    for key in ['monday','tuesday','wednesday','thursday','friday','saturday','sunday']:
        try:
            actualHours = { 'open' : openingHours[key]['from'],
                            'close' : openingHours[key]['to']}
            keyHours = {'day' : dayKeys[key],
                    'open' : True,
                    'hours' : [actualHours]
                    }
        except:
            keyHours = {'day' : dayKeys[key],
                    'open' : False}
        hoursArray.append(keyHours)
    return hoursArray

def set_up_kaufland_database():
    API_URL = "https://www.kaufland.de/.klstorefinder.json"
    kauflandArray = []
    dayKeys = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    intermediaryDayKeys = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
        None: None,
    }
    try:
        rq = requests.get(API_URL)
        res = rq.json()
        if len(res) == 0:
            return False
    except:
        return False

    for index in range(len(res)):
        dayDoneSet = set()
        daysHeap = []
        openingHours = res[index]["wod"]
        dayCounter = 0
        for dayIndex in range(len(openingHours)):
            day = dayIndex
            
            #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if dayIndex not in dayDoneSet:
                try:
                    dayDoneSet.add(
                        intermediaryDayKeys[openingHours[dayIndex].split("|")[0]]
                    )
                    # keyHours processing code
                    opens = openingHours[dayIndex].split("|")[1]
                    closes = openingHours[dayIndex].split("|")[2]
                    if opens == closes:
                        closedDayHours = {
                            "day": openingHours[dayIndex].split("|")[0],
                            "open": False,
                        }
                        heapq.heappush(daysHeap, [day, closedDayHours])
                    actualHours = {"open": opens, "close": closes}
                    keyHours = {
                        "day": openingHours[dayIndex].split("|")[0],
                        "open": True,
                        "hours": [actualHours],
                    }
                    heapq.heappush(daysHeap, [day, keyHours])
                except:
                    closedDayHours = {
                        "day": openingHours[dayIndex].split("|")[0],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [day, closedDayHours])

        # Check for any missing days and ensure all days are in order for insertion into our database
        for day in daysHeap:
            if day[1]["day"] != dayKeys[day[0]]:
                day[0] = intermediaryDayKeys[day[1]["day"]]

        if len(daysHeap) < 7:
            for dayDone in dayKeys.keys():
                if dayDone not in dayDoneSet:
                    closedDayHours = {
                        "day": dayKeys[dayDone],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [dayDone, closedDayHours])

        hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
        latitude = res[index]["lat"]
        longitude = res[index]["lng"]
        kauflandArray.append([(float(latitude), float(longitude)), hoursArray])
    kauflandDB = pd.DataFrame(kauflandArray, columns=["Coordinates", "OpeningHours"])
    kauflandDB.to_csv("Kaufland.csv")
    return True

def get_kaufland_data(lat, lng):
    # pass lat and lng as arrays of length 1 if you want the storeDistance to be returned too
    if isinstance(lat, list):
        returnDistanceToo = True
        lat = lat[0]
        lng = lng[0]
    else:
        returnDistanceToo = False
    desiredCoords = [lat, lng]
    kauflandDB = pd.read_csv(
        "Kaufland.csv",
        index_col="Unnamed: 0",
        converters={"Coordinates": eval, "OpeningHours": eval},
    )
    kauflandDB["Distances"] = haversine_vector(
        [desiredCoords] * len(kauflandDB["Coordinates"]),
        list(kauflandDB["Coordinates"]),
    )
    closestStoreIndex = kauflandDB["Distances"].idxmin()
    if returnDistanceToo:
        return (
            kauflandDB["Distances"][closestStoreIndex],
            kauflandDB["OpeningHours"][closestStoreIndex],
        )
    else:
        return kauflandDB["OpeningHours"][closestStoreIndex]

def set_up_migros_database():

    API_URL = "https://web-api.migros.ch/widgets/stores"

    headers = {
        "Accept-Language": "de",
        "Origin": "https://filialen.migros.ch",
    }

    params = (
        ("key", "loh7Diephiengaiv"),
        ("filters[markets][0][0]", "super"),
        ("filters[markets][0][2]", "voi"),
        ("filters[markets][0][3]", "mp"),
        ("limit", "737"),
    )

    try:
        rq = requests.get(API_URL, headers=headers, params=params)
        res = rq.json()
        if len(res) == 0:
            return False
    except:
        return False

    migrosArray = []

    dayKeys = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    intermediaryDayKeys = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
        None: None,
    }

    for index in range(len(res["stores"])):
        dayDoneSet = set()
        daysHeap = []
        openingHours = res["stores"][index]["markets"][0]["opening_hours"][0][
            "opening_hours"
        ]
        dayCounter = 0
        for dayIndex in range(len(openingHours)):
            if dayIndex not in dayDoneSet:
                try:
                    day = openingHours[dayIndex]["day_of_week"] - 1
                    dayDoneSet.add(day)

                    # keyHours processing code
                    # changes done because some stores opens two times a day.
                    opens_firstHalf = openingHours[dayIndex]["time_open1"]
                    closes_firstHalf = openingHours[dayIndex]["time_close1"]
                    opens_secondHalf = openingHours[dayIndex]["time_open2"]
                    closes_secondHalf = openingHours[dayIndex]["time_close2"]
                    if (
                        opens_firstHalf
                        == closes_firstHalf
                        == opens_secondHalf
                        == closes_secondHalf
                    ):
                        closedDayHours = {
                            "day": dayKeys[day],
                            "open": False,
                        }
                        heapq.heappush(daysHeap, [day, closedDayHours])
                    else:
                        if opens_secondHalf == closes_secondHalf:
                            actualHours = {
                                "open": opens_firstHalf,
                                "close": closes_firstHalf,
                            }
                            keyHours = {
                                "day": dayKeys[day],
                                "open": True,
                                "hours": [actualHours],
                            }
                            heapq.heappush(daysHeap, [day, keyHours])
                        else:
                            actualHours_firstHalf = {
                                "open": opens_firstHalf,
                                "close": closes_firstHalf,
                            }
                            actualHours_secondHalf = {
                                "open": opens_secondHalf,
                                "close": closes_secondHalf,
                            }
                            keyHours = {
                                "day": dayKeys[day],
                                "open": True,
                                "hours": [
                                    actualHours_firstHalf,
                                    actualHours_secondHalf,
                                ],
                            }
                            heapq.heappush(daysHeap, [day, keyHours])

                except:
                    closedDayHours = {
                        "day": dayKeys[day],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [day, closedDayHours])

        # Check for any missing days and ensure all days are in order for insertion into our database
        for day in daysHeap:
            if day[1]["day"] != dayKeys[day[0]]:
                day[0] = intermediaryDayKeys[day[1]["day"]]

        if len(daysHeap) < 7:
            for dayDone in dayKeys.keys():
                if dayDone not in dayDoneSet:
                    closedDayHours = {
                        "day": dayKeys[dayDone],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [dayDone, closedDayHours])

        hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
        latitude = res["stores"][index]["location"]["geo"]["lat"]
        longitude = res["stores"][index]["location"]["geo"]["lon"]
        migrosArray.append([(float(latitude), float(longitude)), hoursArray])
    migrosDB = pd.DataFrame(migrosArray, columns=["Coordinates", "OpeningHours"])
    migrosDB.to_csv("Migros.csv")
    return True

def get_migros_data(lat, lng):
    # pass lat and lng as arrays of length 1 if you want the storeDistance to be returned too
    if isinstance(lat, list):
        returnDistanceToo = True
        lat = lat[0]
        lng = lng[0]
    else:
        returnDistanceToo = False
    desiredCoords = [lat, lng]
    migrosDB = pd.read_csv(
        "Migros.csv",
        index_col="Unnamed: 0",
        converters={"Coordinates": eval, "OpeningHours": eval},
    )
    migrosDB["Distances"] = haversine_vector(
        [desiredCoords] * len(migrosDB["Coordinates"]), list(migrosDB["Coordinates"]),
    )
    closestStoreIndex = migrosDB["Distances"].idxmin()
    if returnDistanceToo:
        return (
            migrosDB["Distances"][closestStoreIndex],
            migrosDB["OpeningHours"][closestStoreIndex],
        )
    else:
        return migrosDB["OpeningHours"][closestStoreIndex]


def get_carrefour_data(lat, lng):
    API_URL = "https://magasins.carrefour.eu/api/v3/near/locations/by/slug"
    params = (
        ("near", "{},{}".format(lat, lng)),
        ("size", "{}".format(limit)),
        ("radius", "20000"),
    )
    rq = requests.get(API_URL, params=params, verify=False)
    if rq.status_code != 200:
        print(rq.json())
        return False
    try:
        res = rq.json()
        if len(res) == 0:
            return False
    except:
        return False
    i = 0
    # INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (e.g. res[0]["name"])
    # e.g.:
    # while True:
    #     if name != res[i]["name"]:
    #         i += 1
    #     else:
    #         break
    storeLat, storeLng = (
        float(res[i]["address"]["latitude"]),
        float(res[i]["address"]["longitude"]),
    )
    # compute distance between the two points using the haversine function
    storeDistance = haversine((lat, lng), (storeLat, storeLng), unit="m")
    if storeDistance > radius:
        return False
    openingHours = res[i]["businessHours"]

    carrefourArray = []
    dayKeys = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    intermediaryDayKeys = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
        None: None,
    }

    for index in range(len(res)):
        dayDoneSet = set()
        daysHeap = []
        openingHours = res[index]["businessHours"]
        dayCounter = 0
        for dayIndex in range(len(openingHours)):
            day = dayIndex
            dayDone = openingHours[dayIndex]["startDay"] - 1
            # if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if dayDone not in dayDoneSet:
                try:
                    dayDoneSet.add(dayDone)
                    # keyHours processing code
                    opens = openingHours[dayIndex]["openTime"]
                    closes = openingHours[dayIndex]["closeTime"]
                    if opens == closes:
                        closedDayHours = {
                            "day": dayKeys[dayDone],
                            "open": False,
                        }
                        heapq.heappush(daysHeap, [day, closedDayHours])
                    actualHours = {"open": opens, "close": closes}
                    keyHours = {
                        "day": dayKeys[dayDone],
                        "open": True,
                        "hours": [actualHours],
                    }
                    heapq.heappush(daysHeap, [day, keyHours])

                except:
                    closedDayHours = {
                        "day": dayKeys[dayDone],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [day, closedDayHours])

        # Check for any missing days and ensure all days are in order for insertion into our database
        for day in daysHeap:
            if day[1]["day"] != dayKeys[day[0]]:
                day[0] = intermediaryDayKeys[day[1]["day"]]

        if len(daysHeap) < 7:
            for dayDone in dayKeys.keys():
                if dayDone not in dayDoneSet:
                    closedDayHours = {
                        "day": dayKeys[dayDone],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [dayDone, closedDayHours])

        hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
        latitude = res[index]["address"]["latitude"]
        longitude = res[index]["address"]["longitude"]
        carrefourArray.append([(float(latitude), float(longitude)), hoursArray])
    return carrefourArray

def set_up_mercadona_database():
    API_URL = "https://www.mercadona.com/estaticos/cargas/data.js"
    mercadonaArray = []
    dayKeys = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday",
    }
    intermediaryDayKeys = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
        None: None,
    }
    try:
        rq = requests.get(API_URL)
        obj = rq.text[rq.text.find("{") : -5] + "]}"
        res = json.loads(obj)

        if len(res) == 0:
            return False
    except:
        return False

    for index in range(len(res["tiendasFull"])):
        dayDoneSet = set()
        daysHeap = []
        daysArray = []
        closingDayArray = res["tiendasFull"][index]["fi"].split("#")
        openingDayArray = res["tiendasFull"][index]["in"].split("#")

        # finding current day in Spain(madrid)
        timeZoneEurope = pytz.timezone("Europe/Madrid")
        datetimeEurope = datetime.now(timeZoneEurope)
        currentday = datetimeEurope.strftime("%A")
        dayCounter = intermediaryDayKeys[currentday]

        # implemented because time string returned accordni to the current day in Spain(eg : "##C#C###")
        for val in range(7):
            daysArray.append(dayCounter)
            dayCounter += 1
            if dayCounter == 7:
                dayCounter = 0

        openingHours = daysArray

        for dayIndex in range(len(openingHours)):
            day = daysArray[dayIndex]

            # if statement to deal with days being included twice (implemnted due to a bug in rewe system)
            if day not in dayDoneSet:
                try:
                    dayDoneSet.add(day)
                    # keyHours processing code
                    opens = openingDayArray[dayIndex]
                    closes = closingDayArray[dayIndex]
                    if opens == closes != "":
                        closedDayHours = {
                            "day": dayKeys[daysArray[dayIndex]],
                            "open": False,
                        }
                        heapq.heappush(daysHeap, [dayIndex, closedDayHours])
                    else:
                        if opens == closes == "":
                            opens = "09:00"
                            closes = "21:30"
                        elif opens == "" and closes != "":
                            opens = "09:00"
                            closes = datetime.strptime(closes, "%H%M").strftime("%H:%M")
                        elif opens != "" and closes == "":
                            closes = "21:30"
                            opens = datetime.strptime(opens, "%H%M").strftime("%H:%M")
                        else:
                            opens = datetime.strptime(opens, "%H%M").strftime("%H:%M")
                            closes = datetime.strptime(closes, "%H%M").strftime("%H:%M")
                        actualHours = {"open": opens, "close": closes}
                        keyHours = {
                            "day": dayKeys[daysArray[dayIndex]],
                            "open": True,
                            "hours": [actualHours],
                        }
                        heapq.heappush(daysHeap, [dayIndex, keyHours])

                except:
                    closedDayHours = {
                        "day": dayKeys[daysArray[dayIndex]],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [dayIndex, closedDayHours])

        # Check for any missing days and ensure all days are in order for insertion into our database
        for day in daysHeap:
            if day[1]["day"] != dayKeys[day[0]]:
                day[0] = intermediaryDayKeys[day[1]["day"]]

        if len(daysHeap) < 7:
            for dayDone in dayKeys.keys():
                if dayDone not in dayDoneSet:
                    closedDayHours = {
                        "day": dayKeys[daysArray[dayIndex]],
                        "open": False,
                    }
                    heapq.heappush(daysHeap, [dayDone, closedDayHours])

        hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
        latitude = res["tiendasFull"][index]["lt"]
        longitude = res["tiendasFull"][index]["lg"]
        mercadonaArray.append([(float(latitude), float(longitude)), hoursArray])
    mercadonaDB = pd.DataFrame(mercadonaArray, columns=["Coordinates", "OpeningHours"])
    mercadonaDB.to_csv("Mercadona.csv")
    return True


def get_mercadona_data(lat, lng):
    # pass lat and lng as arrays of length 1 if you want the storeDistance to be returned too
    if isinstance(lat, list):
        returnDistanceToo = True
        lat = lat[0]
        lng = lng[0]
    else:
        returnDistanceToo = False
    desiredCoords = [lat, lng]
    mercadonaDB = pd.read_csv(
        "Mercadona.csv",
        index_col="Unnamed: 0",
        converters={"Coordinates": eval, "OpeningHours": eval},
    )
    mercadonaDB["Distances"] = haversine_vector(
        [desiredCoords] * len(mercadonaDB["Coordinates"]),
        list(mercadonaDB["Coordinates"]),
    )
    closestStoreIndex = mercadonaDB["Distances"].idxmin()
    if returnDistanceToo:
        return (
            mercadonaDB["Distances"][closestStoreIndex],
            mercadonaDB["OpeningHours"][closestStoreIndex],
        )
    else:
        return mercadonaDB["OpeningHours"][closestStoreIndex]


#Print statements to test setting up of local database opening hours functions.

# print(set_up_rewe_database())
# print(set_up_netto_database())
# print(set_up_kaufland_database())
# print(set_up_migros_database())
# print(set_up_mercadona_database())


#Print statements to test opening hours retrieval functions.

print(get_rewe_data(lat,lng))
print(get_netto_data(lat, lng))
print(get_netto_marken_discount_data(lat, lng))
print(get_netto_brands_data(lat, lng))
print(get_edeka_data(lat,lng))
# print(get_sainsburys_data(lat,lng))
# print(get_asda_data(lat,lng))
# print(get_tesco_data(lat,lng))
# print(get_morrisons_data(lat,lng))
# print(get_waitrose_data(lat,lng))
# print(get_aldi_data(lat,lng))
# print(get_coop_data(lat,lng))
# print(get_marks_and_spencers_data(lat,lng))
# print(get_iceland_data(lat,lng))
# print(get_kaufland_data(lat,lng))
# print(get_migros_data(lat, lng))
# print(get_carrefour_data(lat, lng))
# print(get_mercadona_data(lat, lng))

# # Copyable Heap structure code:

# dayDoneSet = set()
# daysHeap = []
# for dayObject in openingHours:
#         day = intermediaryDayKeys[dayObject["day"]]
#         #if statement to deal with days being included twice (implemnted due to a bug in rewe system)
#         if day not in dayDoneSet:
#             dayDoneSet.add(day)
#             #keyHours processing code
#             #INSERT HOUR PROCESSING CODE HERE
    
#             heapq.heappush(daysHeap, [day,keyHours]) #(REPLACE hoursArray.append(keyHours) with this statement)
# #Check for any missing days and ensure all days are in order for insertion into our database
# checkedUpToDay = -1
# for day in heapq.nsmallest(7, daysHeap):
#     checkedUpToDay += 1
#     while day[0] > checkedUpToDay:
#         closedDayHours = {  'day' : dayKeys[checkedUpToDay],
#                 'open' : False  }
#         heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
#         checkedUpToDay += 1
# while len(daysHeap) < 7:
#     if len(daysHeap) > 0:
#         checkedUpToDay = daysHeap[-1][0] + 1
#     else:
#         # THIS CHECK MEANS STORE IS OPEN 0 DAYS A WEEK, if we want to remove such stores, insert appropriate code here
#         checkedUpToDay = 0
#     closedDayHours = {  'day' : dayKeys[checkedUpToDay],
#                 'open' : False  }
#     heapq.heappush(daysHeap, [checkedUpToDay, closedDayHours])
# if len(daysHeap) != 7:
#     return False
# hoursArray = [i[1] for i in heapq.nsmallest(7, daysHeap)]
# return hoursArray



# Desired format for opening hours array

# [
#         {
#             day: weekday (e.g. Monday),
#             open: True | False,
#             hours: [ //This should be an array because there could be multiple opening hours for one day. E.g. 08:00-12:00 and 15:00-19:00.
#                 {
#                     open: zero-padded hour (24-hour clock):zero-padded minute (e.g.'06:00')
#                     close: zero-padded hour (24-hour clock):zero-padded minute (e.g.'22:00')
#                 }
#             ]
#         }
# ]