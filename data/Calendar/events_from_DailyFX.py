"""
    Input the date and get the list of events in that date
    Input the week (week of the year) and year to get the events in the week
    Input the year to get all events in the year
    Event contains the time, country, name, importance level, actual, forecast, previous
"""
import time
from datetime import datetime, timedelta

import requests 
from bs4 import BeautifulSoup 

WEEK_QUERY_FORMAT = '%Y/%m%d'

# clear screen function
def clear():
    # sleep for 1 sec before clean screen
    time.sleep(0.3)
    print('\033[2A') # move cursor up to the begining of the line
    print('\033[2K') # clear entire line
    print('\033[2A') # move cursor to the first position

# sorted the data by date (key)
def sortedData(data):
    sortedData = {}
    for key in sorted(data):
        sortedData[key] = data[key]
    return sortedData

# add to object
def addValueEvent(eventTime,country, eventName ,eventImportance, eventActual, eventForecast, eventPrevious):
    event = {}
    event['time'] = eventTime
    event['country'] = country
    event['name'] = eventName
    event['importance'] = eventImportance
    event['actual'] = eventActual
    event['forecast'] = eventForecast
    event['previous'] = eventPrevious
    return event

# if none type show the str to 'N/a'
def noneShow(obj):
    if obj is None:
        return 'N/a'
    else:
        return obj

# get the URL to feed with date input
def getURL(dateInput):
    # get the local time zone offset
    utcOffset = int(time.localtime().tm_gmtoff / 3600)
    # get the week query sunday of the week
    if dateInput.isocalendar()[2] != 7:
        weekQuery = (dateInput - timedelta(days=dateInput.isocalendar()[2])).strftime(WEEK_QUERY_FORMAT)
    else:
        weekQuery = dateInput.strftime(WEEK_QUERY_FORMAT)
    return 'https://www.dailyfx.com/calendar?tz=' + str(utcOffset) + '&week=' + weekQuery

# send the request using requests
# parse the response and return the result (using BeautifulSoup and html5lib parser)
def sendRequest(dateInput):
    try:
        feedURL = getURL(dateInput)
        print('Info: Getting data from', feedURL)
        clear()
        page = requests.get(feedURL)
        if len(page.text) == 0:
            print('Info: Err for request page')
        else:
            print('Info: Success request page')
        clear()
        soup = BeautifulSoup(page.text,'html5lib')
        return soup
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)          # __str__ allows args to be printed directly, but may be overridden in exception subclasses
        x, y = inst.args     # unpack args
        print('x =', x)
        print('y =', y)
        return -1

LIST_COUNTRY_AND_CURRENCY = {"USD": "us", "CAD": "ca", "EUR": "eu", "GBP": "gb", "CNY":"cn", "AUD":"au", "NZD": "nz","JPY":"jp", "CHF":"ch"}

# extract country is happened by event
def extCountry(eventName):
    country = eventName[:3]
    if country not in LIST_COUNTRY_AND_CURRENCY:
        return "other"
    else:
        return LIST_COUNTRY_AND_CURRENCY[country]

# pass the id of the table to fetch data
def getDataFromTableID(response, id):
    data = []
    table = response.find('table', id=id)
    # check the table is exist or not
    if table is None:
        # print("Warning!!!There no data...")
        return []
    body = table.find('tbody')
    # get the all the rows of the table
    rows = body.find_all('tr',attrs={"data-id": True}, recursive=False)
    # check each row in rows to get data
    for row in rows:
        event = {}
        cols = row.find_all('td', recursive=False)
        i = 0
        for col in cols:
            # get the time and name of event
            if col.has_attr('id'):
                if col.has_attr('hidden'):
                    continue
                else:
                    if col.div.text:
                        eventTime = col.div.text
                        # eventDateTime = body.tr.td.div.text.strip() + ' ' + eventTime
                        eventName = col.text.split(eventTime)[1]
                    else:
                        # eventDateTime = body.tr.td.div.text.strip() + ' 00:00'
                        eventTime = 'N/a'
                        eventName = col.text
                    country = extCountry(eventName)
            # get the level of the events (low, med, high)
            elif col.span is not None:
                if col.span.has_attr('id'):
                    eventImportance = col.span.text
            elif i == 4: #actual figure
                eventActual = noneShow(col.string)
            elif i == 5: #forecast figure
                eventForecast = noneShow(col.string)
            elif i == 6: #previous figure
                eventPrevious = noneShow(col.string)
            i += 1
        # event = Event(eventDateTime, eventName, eventImportance, eventActual, eventForecast, eventPrevious)
        data.append(addValueEvent(eventTime, country, eventName, eventImportance, eventActual, eventForecast, eventPrevious))
    return data

# feed data events on the date input
def eventsInDay(dateInput):
    try:
        data = {}
        response = sendRequest(dateInput)
        weekday = dateInput.isocalendar()[2]
        # get the id of table to get data    
        if  weekday == 7:
            s_id = 'daily-cal0'
        else:
            s_id = 'daily-cal' + str(weekday)
        print('Info: Processing on ', dateInput.strftime('%A %B %d,%Y'))
        clear()
        label = dateInput.strftime('.%m.%d')
        data[label] = getDataFromTableID(response, s_id)
        return data
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)          # __str__ allows args to be printed directly, but may be overridden in exception subclasses
        x, y = inst.args     # unpack args
        print('x =', x)
        print('y =', y)
        return -1

# validation year has how many weeks
def weeksInYear(year):
    return datetime(year,12,28).isocalendar()[1]

# feed data events whole week with the year and week input
def eventsInWeek(week, year):
    try:
        print(f'Info: Processing week {week} in {year}')
        clear()
        # validation the maxium week can enter
        if week > weeksInYear(year):
            return -1
        # create the object to contain data
        data = {}
        firstDateOfYear = datetime(year,1,1)
        weekdayFirstDateOfYear = firstDateOfYear.isocalendar()[2]
        if weekdayFirstDateOfYear != 7:
            startDate = firstDateOfYear - timedelta(days=weekdayFirstDateOfYear)
        else:
            startDate = firstDateOfYear
        # calculate the dateinput to feed data
        dateInput = startDate + timedelta(weeks=week)
        response = sendRequest(dateInput)
        for i in range(7):
            s_id = 'daily-cal' + str(i)
            if i != 0:
                dateInput += timedelta(days=1) # move to next date to get the date string
            label = dateInput.strftime('%m.%d')
            events = getDataFromTableID(response,s_id)
            data[label] = events
        return sortedData(data)
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)          # __str__ allows args to be printed directly, but may be overridden in exception subclasses
        x, y = inst.args     # unpack args
        print('x =', x)
        print('y =', y)
        return -1

# feed data events whole week of the date input
def eventsInYear(year):
    try:
        print(f'Info: Start to fetching data of in {year}')
        clear()
        data = {}
        # get the weekday of 1st date of the year
        weekDay1st = datetime(year,1,1).isocalendar()[2]
        # if not sunday startDate = datetime(year,1,1)
        # otherwise return the last sunday in the week
        if weekDay1st == 7:
            startDate = datetime(year,1,1)
        else:
            startDate = datetime(year,1,1) - timedelta(days=weekDay1st)
        totalWeeksInYear = weeksInYear(year)
        # iteration all the weeks in year
        for week in range(totalWeeksInYear):
            # calc the dateInput to send request
            dateInput = startDate + timedelta(weeks=week)
            # add the stop request data
            if dateInput > datetime.today():
                break
            # send request and get response
            response = sendRequest(dateInput)
            for i in range(7):
                s_id = 'daily-cal' + str(i)
                if i != 0:
                    dateInput += timedelta(days=1) # move to next date to get the date string
                fetchingDate = dateInput.strftime('%a, %b %d')
                label = dateInput.strftime('%m.%d')
                print(f'Info: Start to fetching data on {fetchingDate}')
                clear()
                events = getDataFromTableID(response,s_id)
                data[label] = events
                print(f'Info: Finished fetching data on {fetchingDate}')
                clear()
        print(f'Info: Finished fetching data in {year}')
        clear()
        return sortedData(data) # adding sorted data before save into storage
    except Exception as inst:
        print(type(inst))    # the exception instance
        print(inst.args)     # arguments stored in .args
        print(inst)          # __str__ allows args to be printed directly, but may be overridden in exception subclasses
        x, y = inst.args     # unpack args
        print('x =', x)
        print('y =', y)
        return -1