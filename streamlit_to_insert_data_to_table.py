# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(":apple: Fruit Details :apple: :balloon:")
st.write(
    """Enter **fruit name** and **root depth code** below.
    """
)

# Get the current credentials
session = get_active_session()

fn = st.text_input('Fruit Name:')
rdc = st.selectbox('Root Depth:', ('S', 'M', 'D'))

if st.button("Submit"):
    # st.write("Fruit Name entered is " + fn)
    # st.write("Root Depth Code chosen is " + rdc)
    
    sql_insert = 'insert into GARDEN_PLANTS.FRUITS.FRUIT_DETAILS select \''+fn+'\',\''+rdc+'\''
    # st.write(sql_insert)
    
    result = session.sql(sql_insert)
    st.write(result)
