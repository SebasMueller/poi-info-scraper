import requests
limit = 3
radius = 1
lat = 52.824201
lng = 1.389848

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
    return hoursArray

def get_tesco_data(lat, lng):
    API_URL = "https://api.tesco.com/tescolocation/v3/locations/search?offset=0"
    params =    { 'limit' : limit,
                'sort' : "near:%22{0},{1}%22".format(lat,lng),
                'filter' : "category:Store%20AND%20isoCountryCode:x-uk",
                'fields' : "name,geo,openingHours"
                #known fields: "name,geo,openingHours,altIds.branchNumber,contact,facilities"
                }
    headers = {"x-appkey": "store-locator-web-cde"}
    # rq = requests.get(API_URL, params=params, headers=headers)
    #(FOR SOME REASON THIS ^ LINE CAUSES ISSUES SO HAVE REPLACED WITH THE BELOW MANUAL LINE RATHER THAN USING THE PARAMS ARGUMENT OF requests.get())
    rq = requests.get(API_URL + "&limit={0}&sort={1}&filter={2}&fields={3}".format(params['limit'], params['sort'], params['filter'], params['fields']), headers=headers)
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
    for key in openingHours:

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

        hoursArray.append(keyHours)
    return hoursArray

print(get_sainsburys_data(lat,lng))









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