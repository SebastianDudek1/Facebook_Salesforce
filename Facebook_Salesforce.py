# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 16:35:42 2022

@author: Sebastian Dudek
"""
from facebook_business.api import FacebookAdsApi
import requests
import json
import pandas as pd
import time
from datetime import datetime
import numpy as np
from datetime import timedelta
import calendar
from simple_salesforce import Salesforce





class Lead_FB_SF():

    # Dane dotyczace aplikacji FB
    # Token jest wazny 90 dni
    access_token = """*****************"""

    app_secret = '***************'
    app_id = '*******************'

    FacebookAdsApi.init(app_id, app_secret, access_token=access_token,
                        api_version = 'v13.0')
    # ID Konta FB AI PL
    my_account = "***********************"
    # Dane do SF
    sf  =  Salesforce ( username = '*************' ,  password = '**************' , security_token = '************8' )
    # Dane do pola pesel / data z dzis
    pesel=str(datetime.today())[:10] +'_' +str(np.random.randint(1,100))


    Facebook_form__c = ['*************']


    TopicId = ['***************']


    def lead_ads_form_list(self):
        headers = {"cache-control": "no-cache"}
        url = """https://graph.facebook.com/v13.0/57544755889/leadgen_forms?access_token="""  + str(Lead_FB_SF.access_token)

        data = requests.request(
                            "GET",
                             url=url,
                             headers=headers
                                 )
        data = json.loads(data.content)
        # Tu mamy cala liste lead ads from
        data = pd.DataFrame(data=data['data'])
        return data



    def lead_ads_contact(self):
        lista_df=[]

        leadgen_forms = self.lead_ads_form_list()

        for i,y in leadgen_forms[['name','id']].iterrows():
            # Timedelta okresla ilosc dni importu
            date_start = datetime.today() - timedelta(1.2)
            utc_time = calendar.timegm(date_start.utctimetuple())
            idx = y[1]
            name_lead = y[0]


            url = "https://graph.facebook.com/v13.0/" + str(idx)  + "/leads?access_token=" + str(Lead_FB_SF.access_token)  + "&limit=5000" "&filtering=[{field:'time_created',operator:'GREATER_THAN', value:'"  + str(utc_time) + "'}]"

            data = requests.request(
                                        "GET",
                                         url=url,

                                             )
            data = json.loads(data.content)
            data =  pd.DataFrame( data['data'] )

            if data.empty:
                print('nie aktywne')
                time.sleep(1)
            else:
                print('aktywne')
                name = data.pop('field_data')
                name =list(name)
                lista= []

                for i in name:
                    lista2 = {}
                    for x in i:

                        di ={x['name'] : x['values']}
                        lista2.update(di)

                    lista.append(lista2)

                df = pd.DataFrame( data = lista )

                df = pd.concat([data,df], axis=1)

                for i in ['email', 'first_name', 'phone_number', 'last_name']:

                    df[i] = df[i].apply(lambda x:
                                str(x).replace('[','').replace(']',
                                    '').replace("'",'').replace('+48','')   )

                #trzeba zmienić na zmienną
                df.insert(1, 'Lead_form_id', str(idx))
                df.insert(1, 'Lead_form_name', str(name_lead))

                lista_df.append(df)

        lista_df = pd.concat(lista_df)
        lista_df = lista_df.drop_duplicates()

        return lista_df


    def data_to_sf(self):

        data = self.lead_ads_contact()

        data = data[[ 'Lead_form_name',  'email', 'first_name', 'phone_number', 'last_name']]
        data.columns = ['Facebook_form__c','Email', 'FirstName' ,'MobilePhone','LastName']
        data.insert(1, 'AccountId', '***************8')

        data.insert(1, 'PESEL__c', Lead_FB_SF.pesel)
        data=data.to_dict('records')
        data = [    data[x:x+30] for x in range(0, len(data), 30)  ]


        return data

    def import_contact(self):

        data = self.data_to_sf()


        for i in data:

            Lead_FB_SF.sf.bulk.Contact.upsert(i,'Email')

            time.sleep(12)


    def tm_contact(self):

        # TM_data pobiera nowo utworzone dane z importu
        TM_data=pd.DataFrame(Lead_FB_SF.sf.query_all("SELECT Id FROM Contact where  PESEL__c like '%s' and CreatedDate = TODAY  and LeadSource = null  "% Lead_FB_SF.pesel)['records'])

        if not (len(TM_data)==0):
            TM_data=TM_data[["Id"]]

            Data_utworzenia_pk=str(datetime.today())[:10]
            TM_data.insert(1, 'LeadSource', 'Petycja')
            TM_data.insert(2, 'OwnerId', '************')
            TM_data.insert(3, 'Lead_creation_Date__c', Data_utworzenia_pk)

            TM_data=TM_data.to_dict('records')


            TM_data = [    TM_data[x:x+25] for x in range(0, len(TM_data), 25)  ]

        return TM_data

    def import_tm_data(self):

        TM_data = self.tm_contact()
        len(TM_data)
        if not (len(TM_data)==0):

            for i in TM_data:
                    Lead_FB_SF.sf.bulk.Contact.update(i)
                    time.sleep(15)






    def data_topic(self):

        Petycja_data=pd.DataFrame(Lead_FB_SF.sf.query_all("SELECT Id,Facebook_form__c FROM Contact where  PESEL__c like '%s'  "% Lead_FB_SF.pesel)['records'])

        Petycja_data=Petycja_data[["Facebook_form__c","Id"]]


        Lista_topic= {'TopicId':Lead_FB_SF.TopicId,'Facebook_form__c':Lead_FB_SF.Facebook_form__c}

        Lista_topic = pd.DataFrame(data=Lista_topic)



        Petycje = pd.merge(Petycja_data,Lista_topic,  on='Facebook_form__c')

        Petycje=Petycje[["Id","TopicId"]]

        Petycje.columns = ['EntityId','TopicId']


        data=Petycje.to_dict('records')

        return data




    def import_topic(self):
        data = self.data_topic()
        data = [    data[x:x+35] for x in range(0, len(data), 35)  ]
        print("Rozpoczynam dodawanie petycji. Ilosc paczek:",len(data))


        for i in data:
            Lead_FB_SF.sf.bulk.TopicAssignment.insert(i)
            time.sleep(10)

        print('p3')


    def data_top_null(self):
        Petycja_data=pd.DataFrame(Lead_FB_SF.sf.query_all("SELECT Id,Facebook_form__c FROM Contact where Last_Petition__c=null and PESEL__c like '%s'  "% Lead_FB_SF.pesel)['records'])
        return Petycja_data

    def data_topic_null(self):

        Petycja_data=pd.DataFrame(Lead_FB_SF.sf.query_all("SELECT Id,Facebook_form__c FROM Contact where Last_Petition__c=null and PESEL__c like '%s'  "% Lead_FB_SF.pesel)['records'])


        Petycja_data=Petycja_data[["Facebook_form__c","Id"]]


        Lista_topic= {'TopicId':Lead_FB_SF.TopicId,'Facebook_form__c':Lead_FB_SF.Facebook_form__c}

        Lista_topic = pd.DataFrame(data=Lista_topic)



        Petycje = pd.merge(Petycja_data,Lista_topic,  on='Facebook_form__c')

        Petycje=Petycje[["Id","TopicId"]]

        Petycje.columns = ['EntityId','TopicId']


        data=Petycje.to_dict('records')

        return data




    def import_topic_null(self):
        data = self.data_topic_null()
        data = [    data[x:x+35] for x in range(0, len(data), 35)  ]
        print("Rozpoczynam dodawanie petycji. Ilosc paczek:",len(data))


        for i in data:
            Lead_FB_SF.sf.bulk.TopicAssignment.insert(i)
            time.sleep(10)

        print('p3')




    def import_all(self):
        self.import_contact()
        time.sleep(5)
        self.import_tm_data()
        time.sleep(5)
        self.import_topic()
        time.sleep(5)

        if self.data_top_null().empty:
            time.sleep(1)
        else:
            self.import_topic_null()



Lead_FB_SF().import_all()
