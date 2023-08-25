#Libraries used:

from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
import time
import re
import plotly.express as px
import mysql.connector
import pymongo
from pymongo import MongoClient
import sqlalchemy
from sqlalchemy import create_engine

#--------------Setting the Home page using streamlit-------------#

icon=Image.open("youtube_logo.png")
st.set_page_config(page_title="YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={"About":"# This Project is made by Priyanka Pal"}
)
st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")

#-----------------------Input from user---------------#

api_key=st.text_input("Enter your api_key")

channel_ids=st.text_input("Enter the Channel_Id")

channel_list=[channel_ids]

#----------------Defining a function to extract youtube data-----------------------#

youtube=build("youtube","v3",developerKey=api_key)

def get_channel_details(_youtube,channel_ids):
    
    all_data=[]
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_ids
    )
    response=request.execute()

    for i in range(len(response["items"])):
        
        data=dict(Channel_Id = response["items"][i]["id"],
              Channel_Name=response["items"][i]["snippet"]["title"],
              Subscribers=response["items"][i]["statistics"]["subscriberCount"],
              View_Count=response["items"][i]["statistics"]["viewCount"],
              Total_Videos=response["items"][i]["statistics"]["videoCount"],
              Playlist_Id=response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
              Description=response["items"][i]["snippet"]["description"]
                 )
        
        all_data.append(data)
    
    return all_data

def get_playlist_id(channel_df):
    
    playlist_ids=[]
    for i in (channel_df["Playlist_Id"]):
        playlist_ids.append(i)

     
    return playlist_ids

def get_video_ids(_youtube,playlist_data):
    
    
    video_id=[]
    
    for i in playlist_data:
        
        next_page_token=None
        more_pages=True
    
        while more_pages:
            request = youtube.playlistItems().list(
                part = 'contentDetails',
                playlistId = i,
                maxResults = 50,
                pageToken = next_page_token)
            response = request.execute()

            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])

            next_page_token = response.get("nextPageToken")
            
            if next_page_token is None:
                more_pages = False

    return video_id

def get_video_details(_youtube,video_ids):
    video_stats=[]
    
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=",".join(video_ids[i:i+50]))
    response = request.execute()
    
    for video in response["items"]:
        
        
        data=dict(Channel_Name = video['snippet']['channelTitle'],
                  Channel_Id = video['snippet']['channelId'],
                  Video_Id=video["id"],
                  Title=video["snippet"]["title"],
                  Description=video["snippet"]["description"],
                  Published_date=video["snippet"]["publishedAt"],
                  Views=video["statistics"]["viewCount"],
                  Likes=video["statistics"]["likeCount"],
                  Favourite_Counts=video["statistics"]["favoriteCount"],
                  Comments=video["statistics"]["commentCount"],
                  Duration=video["contentDetails"]["duration"],
                  Thumbnail=video["snippet"]["thumbnails"]["default"]["url"],
                  Caption_status=video["contentDetails"]["caption"])

        video_stats.append(data)
    return video_stats

def get_comments(_youtube,video_ids):
    
    
    comments_data= []
    next_page_token = None
    try:
        for i in video_ids:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = i,
                    textFormat="plainText",
                    maxResults = 100,
                    pageToken=next_page_token)
                response = request.execute()

                for item in response["items"]:
                    published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]


                    comments = dict(Comment_Id = item["id"],
                                    Video_Id = item["snippet"]["videoId"],
                                    Comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    Comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    Comment_published_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                                   )
                    
                    comments_data.append(comments)

                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
                    
    except Exception as e:
        print("an error occurred",str(e))
    return comments_data

#----------------------------Storing data to MongoDB--------------------#

st.subheader(":blue[Extract Data and Store it to MongoDB]")

submit=st.button("Upload Data to MongoDB Database")

#connecting to mongodb database

client=MongoClient('mongodb://localhost:27017/')

db=client['Youtube_Database']

col1=db['Channel_details']
col2=db['Video_details']
col3=db['Comments_data']


if submit:
    
    if channel_ids:
        
        channel_stats=get_channel_details(youtube,channel_ids)

        channel_df=pd.DataFrame(channel_stats)

        playlist_data=get_playlist_id(channel_df)

        video_ids=get_video_ids(youtube,playlist_data)

        video_data=get_video_details(youtube,video_ids)

        comment_data=get_comments(youtube,video_ids)
        
        with st.spinner("Wait a Sec....."):
            time.sleep(5)


            if channel_stats:
                col1.insert_many(channel_stats)
            if video_data:
                col2.insert_many(video_data)
            if comment_data:
                col3.insert_many(comment_data)
                
        with st.spinner("Please wait....."):
            time.sleep(5)
            st.success("Done!,Data Upoaded Successfully to MongoDB")
            st.snow()

#-----------------------------Migrating data to sql database-------------#

st.subheader(":blue[Migrate Data to MySQL Database]")

submit1=st.button("upload data to mysql")

#connecting to mysql database


mydb = mysql.connector.connect(
  host="localhost",
  port=3306,
  user="root",
  password="user_password",
auth_plugin='mysql_native_password'
)
mycursor=mydb.cursor()

mycursor.execute('CREATE DATABASE IF NOT EXISTS youtube_db')

mycursor.execute("use youtube_db")

mycursor.execute("CREATE TABLE IF NOT EXISTS channels(channel_id varchar(255) Primary Key not null,channel_name varchar(255),subscribers int,view_count int,total_videos int,playlist_id varchar(255),description text)")

mycursor.execute("CREATE TABLE IF NOT EXISTS videos(channel_name varchar(255),channel_id varchar(255),video_id varchar(255) Primary Key,title varchar(255),description text,published_date varchar(255),views int,likes int,favourite_counts int,comments int,duration varchar(255),thumbnail varchar(255),caption_status varchar(255),FOREIGN KEY (channel_id) REFERENCES channels (channel_id))")

mycursor.execute("CREATE TABLE IF NOT EXISTS comments(comment_id varchar(255) Primary key,video_id varchar(255),comment_text text,comment_author varchar(255),comment_published_date varchar(255))")

#Creating an engine to export data easily

my_conn=create_engine("mysql+mysqldb://root:user_password@localhost/youtube_db")

if submit1:
    
    channel_stats=get_channel_details(youtube,channel_ids)

    channel_df=pd.DataFrame(channel_stats)

    playlist_data=get_playlist_id(channel_df)
  
    video_ids=get_video_ids(youtube,playlist_data)

    video_data=get_video_details(youtube,video_ids)

    video_df=pd.DataFrame(get_video_details(youtube,video_ids))

    comment_data=get_comments(youtube,video_ids)

    comment_df=pd.DataFrame(get_comments(youtube,video_ids))
  
    try:
        channel_df.to_sql(con=my_conn,name="channels",if_exists="append",index=False)
    except:
        print("Duplicate Entry Found")
    try:
        video_df.to_sql(con=my_conn,name="videos",if_exists="append",index=False)
    except:
        print("Duplicate Entry Found")
       
    try:
        comment_df.to_sql(con=my_conn,name="comments",if_exists="append",index=False)
    except:
        print("Duplicate Entry Found")
   
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)

    for percent_complete in range(100):
        time.sleep(0.1)
        my_bar.progress(percent_complete + 1, text=progress_text)
    st.success("Done!,Data uploaded to MySQL Database.")
    st.balloons()

    

#-------------------------------------------------------------------------------------#
# Check available channel data

Check_channel = st.checkbox('**Check available channel data for analysis**')

if Check_channel:
   # Create database connection
    engine = create_engine("mysql+mysqldb://root:user_password@localhost/youtube_db")

    # Execute SQL query to retrieve channel names

    query = "SELECT channel_name FROM channels;"
    results = pd.read_sql(query, engine)

    # Get channel names as a list
    channel_names_fromsql = list(results['channel_name'])

    # Create a DataFrame from the list and reset the index to start from 1
    df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
    # Reset index to start from 1 instead of 0
    df_at_sql.index += 1  

    # Show dataframe
    st.dataframe(df_at_sql)


#-----------------Querying the sql database-----------------#

st.subheader(':blue[Query the SQL data warehouse]')

st.write("Choose any query to receive insights.")

questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

if questions == '1. What are the names of all the videos and their corresponding channels?':
    mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    mycursor.execute("""SELECT channel_name 
    AS Channel_Name, total_videos AS Total_Videos
                        FROM channels
                        ORDER BY total_videos DESC""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    
elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                        FROM videos
                        ORDER BY views DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    st.write(" :green[Top 10 most viewed videos :]")
    fig = px.bar(df,
                 x=mycursor.column_names[2],
                 y=mycursor.column_names[1],
                 orientation='h',
                 color=mycursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    mycursor.execute("""SELECT title AS Video_Name, comments AS Total_Comments
                        FROM videos 
                        ORDER BY comments DESC""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    st.write(":green[Total Number of Comments made on each Video:]")
    fig = px.bar(df,
                 x=mycursor.column_names[1],
                 y=mycursor.column_names[0],
                 orientation='h'
                )
    st.plotly_chart(fig,use_container_width=True)
  
elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                        FROM videos
                        ORDER BY likes DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    st.write(":green[Top 10 most liked videos :]")
    fig = px.bar(df,
                 x=mycursor.column_names[2],
                 y=mycursor.column_names[1],
                 orientation='h',
                 color=mycursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                        FROM videos
                        ORDER BY likes DESC""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_name AS Channel_Name, view_count AS Views
                        FROM channels
                        ORDER BY views DESC""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    st.write(":green[Channels vs Views :]")
    fig = px.bar(df,
                 x=mycursor.column_names[0],
                 y=mycursor.column_names[1],
                 orientation='v',
                 color=mycursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)

elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    mycursor.execute("""SELECT channel_name AS Channel_Name
                        FROM videos
                        WHERE published_date LIKE '2022%'
                        GROUP BY channel_name
                        ORDER BY channel_name""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_name, 
                    SUM(duration_sec) / COUNT(*) AS average_duration
                    FROM (
                        SELECT channel_name, 
                        CASE
                            WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT(
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                        SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                        ))
                            WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT(
                            '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                        ))
                            WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                            TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                            END AS duration_sec
                    FROM videos
                    ) AS subquery
                    GROUP BY channel_name""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names
                      )
    st.write(df)



elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                        FROM videos
                        ORDER BY comments DESC
                        LIMIT 10""")
    df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
    st.write(df)
    st.write(":green[Videos with most comments :]")
    fig = px.bar(df,
                 x=mycursor.column_names[1],
                 y=mycursor.column_names[2],
                 orientation='v',
                 color=mycursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)




    



        

    
        

















