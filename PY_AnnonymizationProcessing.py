import logging
import pandas as pd
import json
import time, os
import snowflake.snowpark as snowpark
import time
from snowflake.snowpark import Session, DataFrame
from datetime import datetime,date

def sf_session():
    connection_sf = "creds_dev.json"
    # connect to Snowflake Prod
    with open(connection_sf) as f:
        connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
    return session

def update_kapSalesPerson(session_var,name, email):
    try:
        session_var.sql(f'''DELETE FROM OCEAN_ADM.KAP_SALESPERSONS WHERE NAME='{name}' AND EMAIL='{email}' ''').collect()
        return True
        
    except Exception as e:
        return e

def get_salespersons(session_var):
    try:
        sales_data = session_var.table("KAP_SALESPERSONS").to_pandas()
        return sales_data
    except Exception as e:
        return e
    
def get_dimuser(session_var):
        try:
            df_dim_user = session_var.sql(f''' SELECT * FROM OCEAN_DWH."DimUserGDPR" ''').to_pandas()
            return df_dim_user
        
        except Exception as e:
            return e
def update_annonymization(session_var,name,email):
        try:
            session_var.sql(f''' 
            INSERT INTO OCEAN_ADM.SALES_ANNONYMIZATION 
            SELECT 
            SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d',CONCAT(USER_SK)),25) AS ANNONYMIZATION_KEY,
            USER_SK,USER_ID,USER_ROLE_ID,IS_ACTIVE,USER_TYPE,USERNAME,FIRST_NAME,LAST_NAME,FULL_NAME,TITLE,
            COMPANY_NM,STREET,CITY,STATE,STATE_CD,POSTAL_CD,COUNTRY,COUNTRY_CD,KEYRUS_COUNTRY,ADDRESS,EMAIL,
            PHONE,ALIAS,TIME_ZONE_CD,CURRENCY_ISO_CD FROM OCEAN_DWH."DimUserGDPR" A WHERE A.FULL_NAME='{name}' AND A.EMAIL='{email}'
            ''').collect()
            return True
        
        except Exception as e:
            return e

def update_dimUser(session_var,name,email):
            try:
                session_var.sql(f'''
                UPDATE OCEAN_DWH."DimUserGDPR" SET 
                USER_SK         = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', USER_SK),25),
                USER_ID         = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', USER_ID),25),
                USER_ROLE_ID    = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', USER_ROLE_ID),25),
                USER_TYPE       = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', USER_TYPE),25),
                USERNAME        = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', USERNAME),25),
                FIRST_NAME      = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', FIRST_NAME),25),
                LAST_NAME       = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', LAST_NAME),25),
                FULL_NAME       = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', FULL_NAME),25),
                TITLE           = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', TITLE),25),
                COMPANY_NM      = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', COMPANY_NM),25),
                STREET          = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', STREET),25),
                CITY            = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', CITY),25),
                STATE           = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', STATE),25),
                STATE_CD        = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', STATE_CD),25),
                POSTAL_CD       = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', POSTAL_CD),25),
                COUNTRY         = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', COUNTRY),25),
                COUNTRY_CD      = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', COUNTRY_CD),25),
                KEYRUS_COUNTRY  = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', KEYRUS_COUNTRY),25),
                ADDRESS         = TO_VARIANT(SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', ADDRESS),25)),
                EMAIL           = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', EMAIL),25),
                PHONE           = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', PHONE),25),
                ALIAS           = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', ALIAS),25),
                TIME_ZONE_CD    = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', TIME_ZONE_CD),25),
                CURRENCY_ISO_CD = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', CURRENCY_ISO_CD),25)
                WHERE FULL_NAME='{name}'  AND EMAIL='{email}'
                ''').collect()
                return True
            except Exception as e:
                 return e

def update_fact(session_var,name,user_sk):
    try:
        print(f'''
        UPDATE OCEAN_DWH."FactSalesGDPR" A
        SET A.USER_SK = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', B.USER_SK),25)
        FROM OCEAN_ADM.SALES_ANNONYMIZATION B
        WHERE A.USER_SK = B.USER_SK AND B.USER_SK='{user_sk}'
        ''')
        session_var.sql(f'''
        UPDATE OCEAN_DWH."FactSalesGDPR" A
        SET A.USER_SK = SUBSTR(UUID_STRING('fe971b24-9572-4005-b22f-351e9c09274d', B.USER_SK),25)
        FROM OCEAN_ADM.SALES_ANNONYMIZATION B
        WHERE A.USER_SK = B.USER_SK AND B.USER_SK='{user_sk}'
        ''').collect()
        return True
    except Exception as e:
            return e

""" The main handler starts here """

def main():
    session_var = sf_session()
    df          = get_salespersons(session_var)
    df_dimUser  = get_dimuser(session_var)

    df          = df[df["Confirm Exit"] == True]
    df['TBR']   = pd.to_datetime(df['Exit Date'] + pd.offsets.DateOffset(years=2)).dt.date
    #df['Difference'] = (df['TBR'].apply(pd.Timestamp) - df['Exit Date'].apply(pd.Timestamp)).dt.days
    
    if len(df)>0:
        #print(df)
        for x in df.index:
            if (date.today() >= df['TBR'][x]):
                name    = df.loc[x,'NAME']
                email   = df.loc[x,'EMAIL']
                try:
                    print (f'''
                    SELECT * FROM OCEAN_ADM."SALES_ANNONYMIZATION" WHERE FULL_NAME='{name}' and EMAIL='{email}' ''')
                    user_mask = session_var.sql(f'''
                    SELECT * FROM OCEAN_ADM."SALES_ANNONYMIZATION" WHERE FULL_NAME='{name}' and EMAIL='{email}' ''').collect() 

                    #user_sk = df_dimUser[df_dimUser['FULL_NAME'] == name]
                    #print(user_sk)
                    if len(user_mask) == 0:
                        try:
                            print(df_dimUser.loc[(df_dimUser['FULL_NAME'] == name) & (df_dimUser['EMAIL'] == email)])
                            print(df_dimUser)
                            dimUser_rec = df_dimUser.loc[(df_dimUser['FULL_NAME'] == name) & (df_dimUser['EMAIL'] == email)
                                                 ,['USER_SK']]
                            user_sk = dimUser_rec.values[0][0]
                            #print(user_sk)
                            print("Inserting into Annonymization table.. ",update_annonymization(session_var,name,email))
                            print("Updating the DimUser.. ",update_dimUser(session_var,name,email))
                            print("Updating the FactSales.. ",update_fact(session_var,name,user_sk))
                            print("Removing the sales person.. ",update_kapSalesPerson(session_var,name, email))
                        except Exception as e:
                            print("Error: ",e)
                        
                        #remove the record from KAP_SALESPERSON
                    else:
                        print("Record exists already.")

                except Exception as e:
                    print(e)
            else:
                print("Data removal date has not arrived for: ",df.loc[x,'NAME'])
    else:
        print("No record to process under GDPR")    
            #print(df['Exit Date'][x], df['NAME'][x])
    


main()
