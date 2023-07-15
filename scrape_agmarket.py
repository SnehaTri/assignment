import argparse
from datetime import datetime
from bs4 import BeautifulSoup
from lxml.html import fromstring 
import requests
import csv
import re
import pandas as pd
import psycopg2


parser = argparse.ArgumentParser()
parser.add_argument('-c','--commodity', help="Enter name of commodity", default='--select--')
parser.add_argument('-s','--start_date', help="Enter start date")
parser.add_argument('-e','--end_date', help="Enter end date")
parser.add_argument('-t','--time_agg', help="Enter time for aggregation")
parser.add_argument('-st','--states', help="Enter states names", type=str)

args = parser.parse_args()
print("Name of commodity : " + args.commodity)
print("Start date is: " + args.start_date + " and end date is " + args.end_date)
print("States are: " + str(args.states.split(",")[0]) + " " + str(args.states.split(",")[1]))


def format_of_datetime(date):
    date = date.split("-")
    time = datetime(int(date[0].lstrip("0")), int(date[1].lstrip("0")), int(date[2].lstrip("0")))
    time = time.strftime("%d-%B-%Y")
    return time


def url_by_states(commodity, start_date, end_date, states):
    url = "https://agmarknet.gov.in/"
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, "lxml")
        
    comm_dic, state_dic = {}, {}
        
    comm_select = [i for i in soup.find("select", {"id": "ddlCommodity"})]
    comm_select = [i for i in comm_select if i != '\n']
    state_select = [i for i in soup.find("select", {"id": "ddlState"})]
    state_select = [i for i in state_select if i != '\n']
    
    for com in comm_select:
        comm_dic[com.attrs['value']] = str(com.text) 
    for state in state_select:
        state_dic[state.attrs['value']] = str(state.text) 
    #print(comm_dic)
    #print(state_dic)
    start_date, end_date = format_of_datetime(start_date), format_of_datetime(end_date)
    comm = commodity.title()
    comm_id = list(comm_dic.keys())[list(comm_dic.values()).index(comm)]
    states = states.split(",")
    url_by_states = []
    for s in states:
        s_id = list(state_dic.keys())[list(state_dic.values()).index(s.title())]
        url = f"https://agmarknet.gov.in/SearchCmmMkt.aspx?Tx_Commodity={comm_id}&Tx_State={s_id}&Tx_District=0&Tx_Market=0&DateFrom={start_date}&DateTo={end_date}&Fr_Date={start_date}&To_Date={end_date}&Tx_Trend=2&Tx_CommodityHead={comm}&Tx_StateHead={str(s.title())}&Tx_DistrictHead=--Select--&Tx_MarketHead=--Select--"
        url = url.replace(" ", "+")
        url_by_states.append(url)
    print(url_by_states)
    return url_by_states

def webscarping_to_csv(url):
    data_df = pd.DataFrame(columns=['State Name', 'District Name', 'Market Name', 'Variety', 'Group',
       'Arrivals (Tonnes)', 'Min Price (Rs./Quintal)',
       'Max Price (Rs./Quintal)', 'Modal Price (Rs./Quintal)',
       'Reported Date'])
    for u in url:
        html_content = requests.get(u).text
        soup = BeautifulSoup(html_content, "lxml")
        #print(soup.prettify())
        table = soup.find("table")
        #print(table)
        rows = table.findAll("tr")[0:20]

        dfs = pd.read_html(u)
        df = dfs[0]
        df.drop(df.tail(3).index,inplace = True)
        data_df = pd.concat([data_df, df], axis = 0, ignore_index=True)
        print(data_df)
    #print(data_df)   
    data_df.to_csv(r'C:\Users\Rahul\Desktop\file1.csv', index=False)


def csv_to_db():
    conn = psycopg2.connect(database="agriiq",
                            user='postgres', password='pass',
                            host='127.0.0.1', port='5432')

    conn.autocommit = True
    cursor = conn.cursor()


    sql = '''CREATE TABLE agmarket_monthly(State Name char(20), District Name char(20), Market Name char(20), Variety char(20), Group char(20),\
    Arrivals (Tonnes) float, Min Price (Rs./Quintal) int, Max Price (Rs./Quintal) int, Modal Price (Rs./Quintal) int, Reported Date DATE);'''
    


    cursor.execute(sql)

    sql2 = '''COPY agmarket_monthly(State Name,District Name,Market Name, Variety, Group, Arrivals (Tonnes), Min Price (Rs./Quintal), Max Price (Rs./Quintal),\
    Modal Price (Rs./Quintal), Reported Date)
    FROM ''C:\\Users\\Rahul\\Desktop\\file1.csv''
    DELIMITER ','
    CSV HEADER;'''

    cursor.execute(sql2)

    sql3 = '''select * from details;'''
    cursor.execute(sql3)
    for i in cursor.fetchall():
        print(i)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    url = url_by_states(args.commodity, args.start_date, args.end_date, args.states)
    webscarping_to_csv(url)
    csv_to_db()



