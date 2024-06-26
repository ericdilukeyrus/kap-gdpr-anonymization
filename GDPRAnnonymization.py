import streamlit as st
import pandas as pd
from datetime import date
import json
import time
from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from itertools import product
import hmac


st.set_page_config(
    page_title= 'Sales Anonymization'
)

st.title(f" :white[{'Sales Anonymization'}]")

def check_password():
    """Returns `True` if the user had a correct password."""

    def login_form():
        """Form with widgets to collect user information"""
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in st.secrets[
            "passwords"
        ] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the username or password.
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    # Return True if the username + password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show inputs for username + password.
    login_form()
    if "password_correct" in st.session_state:
        st.error("Username or password incorrect")
    return False


if not check_password():
    st.stop()

def cart_prod(l1, l2):
   return list(product(l1, l2))

def add_bg_from_url():
    st.markdown(
         f"""
         <style>
         .stApp {{
            background-color: #0c549c;
            margin: auto;
            width: 50%;
            padding: 10px;
            color: white;
             
         }}
         </style>
         """,
         unsafe_allow_html=True
     )

def get_salesperson():
    if 'snowflake_connection' not in st.session_state:
        #Connect to Snowflake
        with open('creds_dev.json') as f:
            connection_param = json.load(f)
        st.session_state.snowflake_connection = Session.builder.configs(connection_param).create()   
        session = st.session_state.snowflake_connection
    else: 
        session = st.session_state.snowflake_connection
    
    df = session.table("KAP_SALESPERSONS").to_pandas()
    #df = df[df['Confirm Exit'] != True]
    df.fillna("None",inplace=True)
    return df

#add_bg_from_url() 


original_DF = get_salesperson()

if 'df' not in st.session_state:
    st.session_state['df'] = original_DF

df =  original_DF[original_DF['Confirm Exit'] != True]


st.markdown(
         f"""
         <style>
         .stApp {{
            background-color: #e1e5e8;
            margin: auto;
            width: 50%;
            padding: 10px;
            color: black;
             
         }}
         </style>
         """,
         unsafe_allow_html=True
     )

sales_user = st.multiselect(
    "Sales Person",
    list(set(list(df["NAME"]))),
    label_visibility="visible"
    )

title = st.multiselect(
    "Title",
    list(set(list(df["TITLE"]))),
    label_visibility="visible"
    )

with st.form("freeze_periods_form"):
        
        if len(sales_user)>0 and len(title) == 0:
            df =  df[df['NAME'].isin(sales_user)]
        elif len(title)>0 and len(sales_user) == 0:
            df = df[df['TITLE'].isin(title)]
        elif len(sales_user)>0 and len(title) > 0:
            df = df[df.set_index(['NAME','TITLE']).index.isin(list(cart_prod(sales_user, title)))]
        
        if len(sales_user)>0:
            df =  df[df['NAME'].isin(sales_user)]
        else:
            df = df
        
        
        st.subheader("Select sales person to confirm exit from the system", divider='gray')
        st.warning('Warning: This is an irreversible action. Once removed, the person cannot be restored.', icon="⚠️")
        
        edited_data = st.data_editor(
            df,
            column_config={
                "EMAIL": st.column_config.Column("EMAIL", disabled=True),
                "NAME": st.column_config.Column("NAME", disabled=True),
                "TITLE": st.column_config.Column("TITLE", disabled=True),
                "Exit Date": st.column_config.DateColumn("Exit Date",min_value=date(1900, 1, 1),max_value=date(2050, 12, 31),format="YYYY-MM-DD",step=1,),
                "Confirm Exit": st.column_config.CheckboxColumn("Confirm Exit", disabled= False)
            },
            column_order = ["NAME","TITLE","Exit Date","Confirm Exit"],
            use_container_width=True,
            hide_index=True
            ) 
        submit_button = st.form_submit_button(" :green[Submit]")

if submit_button:
    with st.spinner('Processing'):
        try:
            #df_dtr = df[df['Confirm Exit'] == True]
            #df_dtr['Removal Date'] = pd.to_datetime(df_dtr['Exit Date'] + pd.offsets.DateOffset(years=2)).dt.strftime('%Y-%m-%d')
            #df_dtr['Removal Date'] = pd.to_datetime(df_dtr['Exit Date'] + pd.offsets.DateOffset(years=2)).dt.date
            #df_dtr = pd.concat([original_DF,edited_data,df_dtr]).drop_duplicates(['USER_ID','NAME'],keep='last').sort_values('USER_ID')
            
            df = pd.concat([original_DF,edited_data]).drop_duplicates(['USER_ID','NAME'],keep='last').sort_values('USER_ID')
            st.session_state.snowflake_connection.sql("TRUNCATE TABLE KAP_SALESPERSONS").collect()
            st.session_state.snowflake_connection.write_pandas(df,'KAP_SALESPERSONS', overwrite=False)
            time.sleep(20)
            st.success('Record(s) updated successfully!')
            st.rerun()
            #st.write(st.session_state.snowflake_connection.sql("""CALL OCEAN_ADM.SALES_ANONYMIZATION('hellpp')""").collect())
            
        except Exception as e:
            st.warning(e)