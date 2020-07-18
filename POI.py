import requests
import html
import re
limit = 3
radius = 15
lat = 52.635875
lng = 1.301
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
    res = rq.json()
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (res[0]['name'] or res[0]['other_name'])
    #e.g.:
    # while True:
    #     if name != res[0]['name'] or res[0]['other_name']:
    #         i += 1
    #     else:
    #         break
    openingHours = res['results'][i]['opening_times']
    hoursArray = []
    for index in range(len(openingHours)):
        key = openingHours[index]['day']
        actualHours = []
        while len(hoursArray) < key:
            #Incase store is closed on a day of the week and therefore not included in the list
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
        for timeSlotNumber in range(len(openingHours[index]['times'])):
            try:
                actualHoursDict = {}
                actualHoursDict['open'] = openingHours[index]['times'][timeSlotNumber]['start_time']
                actualHoursDict['close'] = openingHours[index]['times'][timeSlotNumber]['end_time']
                actualHours.append(actualHoursDict)
            except:
                #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
                closedDayHours = {  'day' : dayKeys[key],
                                'open' : False  }
                hoursArray.append(closedDayHours)
                break
        keyHours = {'day' : dayKeys[key],
                'open' : True,
                'hours' : actualHours
                }
        hoursArray.append(keyHours)
    while len(hoursArray) < 7:
            #Incase store is closed on a day of the week (at the end of the list/week) and therefore not included in the list
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
    #Incase indexes were muddled in the response (difficult to deal with due to desired output format of the days being an ordered array not a key'ed dictionary)
    if len(hoursArray) > 7:
        return False
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
    openingHours = res["entities"][i]["profile"]["hours"]["normalHours"]
    hoursArray = []
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    for index in range(len(openingHours)):
        if openingHours[index]["isClosed"] != False:
            closedDayHours = {  'day' : dayKeys[index],
                                'open' : False  }
            hoursArray.append(closedDayHours)
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
                closedDayHours = {  'day' : dayKeys[index],
                                'open' : False  }
                hoursArray.append(closedDayHours)
                break
        keyHours = {'day' : dayKeys[index],
                'open' : True,
                'hours' : actualHours
                }
        hoursArray.append(keyHours)
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
    res = rq.json()['results']
    i=0
    #INSERT POTENTIAL CHECK THAT STORE MATCHES DESIRED STORENAME!! (res[0]['location']['name'])
    #e.g.:
    # while True:
    #     if name != res[i]['location']['name']:
    #         i += 1
    #     else:
    #         break
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
    openingHours = res[i]["openingTimes"]
    hoursArray = []
    for index in range(len(openingHours)):
        key = intermediaryDayKeys[openingHours[index]['day']]  #turn key into int
        actualHours = []
        while len(hoursArray) < key:
            #Incase store is closed on a day of the week and therefore not included in the list 
            #(although this is unlikely for ALDI, this check was added for Sainsbury API since how they deal with closed days is unclear,
            #whereas it is unlikely ALDI's API would miss a day, so this check could potentially be removed if desired, however is left in for an extra layer of safety)
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
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
        
        hoursArray.append(keyHours)
    while len(hoursArray) < 7:
            #Incase store is closed on a day of the week (at the end of the list/week) and therefore not included in the list
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
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
    openingHours = res[i]["opening_hours"]
    intermediaryDayKeys = { 'Monday' : 0, 'Tuesday' : 1, 'Wednesday' : 2, 'Thursday' : 3, 'Friday' : 4, 'Saturday' : 5, 'Sunday' : 6}
    dayKeys = { 0 : 'Monday', 1 : 'Tuesday', 2 : 'Wednesday', 3 : 'Thursday', 4 : 'Friday', 5 : 'Saturday', 6 : 'Sunday'}
    hoursArray = []
    for index in range(len(openingHours)):#
        print(index)
        key = intermediaryDayKeys[openingHours[index]['name']]  #turn key into int
        print(openingHours[index]['name'])
        actualHours = []
        while len(hoursArray) < key:
            #Incase store is closed on a day of the week and therefore not included in the list 
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
        try:
            if openingHours[index]["label"].find("los") != -1:
                #incase the world closed/closure/Closed/Closure is in the label
                closedDayHours = {  'day' : dayKeys[key],
                            'open' : False  }
                hoursArray.append(closedDayHours)
            else:
                actualHoursDict = {}
                actualHoursDict['open'] = openingHours[index]['opens']
                actualHoursDict['close'] = openingHours[index]['closes']
                actualHours = [actualHoursDict]
                keyHours = {'day' : dayKeys[key],
                    'open' : True,
                    'hours' : actualHours
                    }
                hoursArray.append(keyHours)
        except:
            #If dictionary keys start_time or end_time dont exist, assume store is closed on that day
            closedDayHours = {  'day' : dayKeys[key],
                            'open' : False  }
            hoursArray.append(closedDayHours)
    while len(hoursArray) < 7:
            #Incase store is closed on a day of the week (at the end of the list/week) and therefore not included in the list
            closedDayHours = {  'day' : dayKeys[len(hoursArray)],
                                'open' : False  }
            hoursArray.append(closedDayHours)
    #Incase indexes were muddled in the response (difficult to deal with due to desired output format of the days being an ordered array not a key'ed dictionary)
    if len(hoursArray) > 7:
        return False
    return hoursArray



print(get_tesco_data(lat,lng))












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