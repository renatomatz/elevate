### scrapper for all the customer data
import requests
import csv
import os
import random

# os.chdir(r"C:\Users\bibo9\Google Drive\Personal DB\Projects\Tech Jam 2019\Repo\Datasource")

_APIKEY =  "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJDQlAiLCJ0ZWFtX2" +\
           "lkIjoiZWZkMjQyOTEtYzlmYy0zMzgxLWJlOTgtMDBjNmQwYTRjOTI0IiwiZXhwI" +\
           "jo5MjIzMzcyMDM2ODU0Nzc1LCJhcHBfaWQiOiJiNWI5M2E0NS04MWYxLTRiN2It" +\
           "OWMyNi04ZTZhMWVhNWY5YjcifQ.POQlG2FzI2DN47L4mBb096bxAZmfL9B2MXqD" +\
           "5oKf4IY"

class Data_collector:
    def __init__(self):
        self.apikey = _APIKEY
    
    def get_customer_info(self, startingpoints):
        """ Get 1000 customer info by starting point (*1000) starting 1"""
        output = []
        output_key = []
        cust_ids = []
        for startingpoint in startingpoints:
            response = requests.post(
                'https://api.td-davinci.com/api/raw-customer-data',
                headers={'Authorization': self.apikey},
                json={'continuationToken': "_".join(
                    [str(startingpoint), "1000"])}
            )
            response_data = response.json()
            if response_data['statusCode'] == 200:
                customers = response_data['result']['customers']
                if len(customers) > 0:
                    for row in customers:
                        temprow = {}
                        for key, item in row.items():
                            if key == 'addresses':
                                temprow['postalCode'] = item['principalResidence']['postalCode']
                                if "postalCode" not in output_key:
                                    output_key.append('postalCode')
                            elif type(item) == str or type(item) == float or type(item) == int:
                                temprow[key] = item
                                if key not in output_key:
                                    output_key.append(key)
                            if key == 'id':
                                cust_ids.append(item)
                        output.append(temprow)
        with open('output_customer.csv', mode='w', newline="") as csv_file:
            fieldnames = output_key
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output)
        output = []
        output_key = ["custId", "accountType"]
        for cust_id in cust_ids:
            response = requests.get(
                "https://api.td-davinci.com/api/customers/"+cust_id+"/accounts",
                headers={'Authorization': self.apikey}
            )
            response_data = response.json()
            if response_data['statusCode'] == 200:
                accounts = response_data['result']
                if len(accounts) > 0:
                    for key, item in accounts.items():
                        for i, subaccount in enumerate(item):
                            temprow = {"custId": cust_id, "accountType": key + "_" +str(i)}
                            for key1, item1 in subaccount.items():
                                if type(item1) == str or type(item1) == float:
                                    temprow[key1] = item1
                                    if key1 not in output_key:
                                        output_key.append(key1)
                            output.append(temprow)
        with open('output_account.csv', mode='w', newline="") as csv_file:
            fieldnames = output_key
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output)
        output = []
        output_key = ["custId"]
        for cust_id in cust_ids:
            response = requests.get(           
                "https://api.td-davinci.com/api/customers/"+cust_id+"/transactions",
                headers={'Authorization': self.apikey}
            )
            response_data = response.json()
            if response_data['statusCode'] == 200:
                transactions = response_data['result']
                if len(transactions) > 0:
                    for item in transactions:
                        temprow = {"custId": cust_id}
                        for key1, item1 in item.items():
                            if type(item1) == str or type(item1) == float:
                                temprow[key1] = item1
                                if key1 not in output_key:
                                    output_key.append(key1)
                            if key1 == "categoryTags":
                                temprow[key1] = ", ".join(item1)
                                if key1 not in output_key:
                                    output_key.append(key1)
                        output.append(temprow)
        with open('output_transaction.csv', mode='w', newline="") as csv_file:
            fieldnames = output_key
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output)
    
                    
x = Data_collector()

result = x.get_customer_info([random.randint(1,1000)])
