'''
This script pulls from the Bing maps api and gets the driving distance between two postcodes. First, it converts the
postcodes to lat/longs using a lookup csv then polls the bing maps api every 0.5 seconds for an output.
'''

import pandas as pd
import urllib.request
import json
import time
import configparser
import numpy as np

sd = pd.read_excel('W:/Staff Downloads/2019-08 - Staff Download.xlsx')
sd = sd.rename(columns = {'Pay_Number':'Pay No'}) #for merge

#get api key from file
config = configparser.ConfigParser()
config.read(r'//ntserver5/generaldb/workforcedb/Python/Danny/Postcode Analysis/config.ini')
BingMapsKey = config.get('API_Keys', 'Bing')

path = r'//ntserver5/generaldb/workforcedb/Python/Danny/Postcode Analysis/'

def driv_distance(xlat, xlong, ylat, ylong):
    routeurl = "http://dev.virtualearth.net/REST/V1/Routes/Driving?o=json&wp.0=" + str(xlat) + ","\
               + str(xlong) + "&wp.1=" + str(ylat) + "," + str(ylong) + "&distanceUnit=mile&key=" + BingMapsKey
    request = urllib.request.Request(routeurl)
    response = urllib.request.urlopen(request)
    r = response.read().decode(encoding="utf-8-sig")
    result = json.loads(r)
    distance = result["resourceSets"][0]["resources"][0]["travelDistance"]
    return (round(distance, 2))


df = pd.read_csv('W:/Python/Danny/Postcode Analysis/TQ-Data.csv')


postcodes = pd.read_csv(r'\\ntserver5\generaldb\workforcedb\Python\Danny\Postcode Analysis\Postcodes.csv')
df1 = pd.DataFrame(columns=['Pay No', 'Surname', 'First Name', 'Postcode', 'Latitude', 'Longitude','Vale_of_Leven',
                            'Gartnavel','Inverclyde', 'RAH', 'Dykebar', 'Leverndale', 'Stobhill'])

df = df.merge(postcodes, on="Postcode", how="inner")

locs = {'vale_of_leven':[55.993086, -4.591825],
        'gartnavel':[55.881997, -4.315496],
        'inverclyde':[55.944042, -4.809259],
        'RAH':[55.835947, -4.437663],
        'dykebar':[55.824128, -4.397919],
        'leverndale':[55.833956, -4.363879],
        'stobhill':[55.892568, -4.217291]}

rows = []
for index, row in df.iterrows():
    miles = {}
    for i in locs:
        miles['Pay No'] = row['Pay No']
        miles['Surname'] = row['Surname']
        miles['First Name'] = row['First Name']
        miles['Postcode'] = row['Postcode']
        miles[i] = driv_distance(locs[i][0], locs[i][1], row['Latitude'], row['Longitude'])
        time.sleep(0.5)

    miles['Gartnavel_Diff'] = miles['gartnavel'] - miles['vale_of_leven']
    miles['Inverclyde_Diff'] = miles['inverclyde'] - miles['vale_of_leven']
    miles['RAH_Diff'] = miles['RAH'] - miles['vale_of_leven']
    miles['Dykebar_Diff'] = miles['dykebar'] - miles['vale_of_leven']
    miles['Leverndale_Diff'] = miles['leverndale'] - miles['vale_of_leven']
    miles['Stobhill_Diff'] = miles['stobhill'] - miles['vale_of_leven']
    #TODO the below code should be replaced with a pd.cut at some point
    miles['Distance_Class'] = np.where(miles['vale_of_leven'] < 2, 'Within 2 miles',
                                       np.where(miles['vale_of_leven'] < 5, 'Within 5 miles',
                                                np.where(miles['vale_of_leven'] < 10, 'Within 10 miles',
                                                         np.where(miles['vale_of_leven'] < 15, 'Within 15 miles',
                                                                  'over 15 miles'))))






df1 = pd.DataFrame(rows)
df1 = df1.merge(sd[['Pay No','Pay_Band', 'Age']], how='inner', on='Pay No')
df1.to_csv('W:/Python/Danny/Postcode Analysis/New Distance.csv', index=False)


