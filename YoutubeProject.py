import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from googleapiclient.discovery import build
from pprint import pprint
import pymongo
from pymongo import MongoClient
import datetime
from datetime import datetime
from time import sleep
from datetime import timezone
import logging

api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyCDxBdA6WfXvSjXGVLIgD6q1XU8QCjP0uk'
channel_id1='UCrzbzc5Ao5Qdkv3qImK-0XA' #4s Writes
channel_id1='UCEWRdkfyfJsU5ljgOyaAWqw' #NewsLeader Ranjith
channel_id1="UCpRCbEFVT0JEEKHsq9sYiNw" #Unlimited Motivational Club
channel_id='UCwr-evhuzGZgDFrq_1pLt_A'  #Error Makes Clever Academy
youtube = build(api_service_name, api_version, developerKey=api_key)

#Channel Data
def get_channel_data(api_key, channel_id):
    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        channel_data = response.get("items", [])[0] if response.get("items") else None
        if channel_data:
            channel_data = {
                "Channel_Id": channel_id,
                "Channel_Name": channel_data["snippet"]["title"],
                "Channel_Description": channel_data["snippet"]["description"],
                "Channel_PListid": channel_data["contentDetails"]["relatedPlaylists"]["uploads"],
                "Channel_viewCount": channel_data["statistics"]["viewCount"],
                "Channel_subcriberCount": channel_data["statistics"]["subscriberCount"],
                "Channel_videoCount": channel_data["statistics"]["videoCount"]
            }
            return channel_data
        else:
            return None
    except Exception as e:
        handle_api_errors(e)
        return None

#Playlist
def get_playlist_data(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.playlists().list(part="snippet,contentDetails", channelId=channel_id)
    response = request.execute()
    playlist_data_list = []
    for item in response.get("items", []):
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        playlist_data = {
            "Channel_Id": channel_id,
            "PList_ChannelName": snippet.get("channelTitle", ""),
            "PList_PTitle": snippet.get("title", ""),
            "PList_Id": item["id"],
            "PList_Desc": snippet.get("description", ""),
            "PList_publishedAt": snippet.get("publishedAt", ""),
            "PList_itemCount": content_details.get("itemCount", 0)
        }
        playlist_data_list.append(playlist_data)
    return playlist_data_list


#Video Data
def make_video_details_request(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        video_data = response.get("items", [])[0] if response.get("items") else None
        return video_data
    except Exception as e:
        handle_api_errors(e)
        return None

def get_video_details(api_key, channel_id, max_results=3, max_retries=3, retry_delay=5):
    logging.basicConfig(level=logging.INFO)
    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        for _ in range(max_retries):
            try:
                search_response = youtube.search().list(
                    part="id",
                    channelId=channel_id,
                    maxResults=max_results,
                    order="date"
                ).execute()
                video_data_list = []
                for item in search_response.get("items", []):
                    item_id = item["id"]
                    if "videoId" in item_id:
                        video_id = item_id["videoId"]
                        video_info = make_video_details_request(youtube, video_id)
                        if video_info:
                            snippet = video_info.get("snippet", {})
                            statistics = video_info.get("statistics", {})
                            view_count = int(statistics.get("viewCount", 0))
                            like_count = int(statistics.get("likeCount", 0))
                            dislike_count = int(statistics.get("dislikeCount", 0))
                            publish_date_str = snippet.get("publishedAt", "")
                            publish_date = datetime.strptime(publish_date_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                            
                            if publish_date.tzinfo is None:
                                publish_date = publish_date.replace(tzinfo=datetime.timezone.utc)
                            
                            video_data = {
                                "Video_Id": video_id,
                                "Channel_Id": snippet.get("channelId", ""),
                                "Channel_Name": snippet.get("channelTitle", ""),
                                "Video_Title": snippet.get("title", ""),
                                "Video_Description": snippet.get("description", ""),
                                "Video_PublishDate": publish_date,
                                "Video_ViewCount": view_count,
                                "Video_LikeCount": like_count,
                                "Video_DislikeCount": dislike_count,
                                "Video_FavoriteCount": int(statistics.get("favoriteCount", 0)),
                                "Video_CommentCount": int(statistics.get("commentCount", 0)),
                                "Video_Duration": snippet.get("duration", ""),
                                "Video_Thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url", "")
                            }
                            video_data_list.append(video_data)
                return video_data_list
            except Exception as e:
                handle_api_errors(e)
                sleep(retry_delay)
        return None
    except Exception as e:
        handle_api_errors(e)
        return None


# COMMENT DATA
def comment_details(video_ids):
    comments = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(part="snippet,replies", videoId=video_id, maxResults=100)
            response = request.execute()
            if len(response['items']) > 0:
                for j in range(len(response['items'])):
                    comments.append({
                        'Comment_ChannelId': response['items'][j]['snippet']['channelId'],
                        'video_id': video_id,
                        'Comment_VideoId': response['items'][j]['snippet']['videoId'],
                        'Comment_Id': response['items'][j]['snippet']['topLevelComment']['id'],
                        'Comment_Text': response['items'][j]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_Author': response['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'Comment_PublishAt': response['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt']
                    })
        except Exception as e:
            comments.append({
                'Comment_ChannelId': None,
                'video_id': video_id,
                'Comment_VideoId': None,
                'Comment_Id': None,
                'Comment_Text': None,
                'Comment_Author': None,
                'Comment_PublishAt': None
            })
    return comments


# Youtube
def main(api_key, channel_id):
    try:
        channel_data = get_channel_data(api_key, channel_id)
        playlists = get_playlist_data(api_key, channel_id)
        videos_result = get_video_details(api_key, channel_id)
        comments_data = comment_details(videos_result)

        playlists = playlists if playlists else []
        videos_result = videos_result if videos_result else []
        comments_data = comments_data if comments_data else []

        Youtube_Data = {
            'channel_data': channel_data,
            'playlists': playlists,
            'videos_result': videos_result,
            'comments_data': comments_data
        }
        return Youtube_Data
    except Exception as e:
        print(f"An error occurred in the main function: {e}")
        return None
YoutubeData = main(api_key, channel_id)
# pprint(YoutubeData)


#MONGODB 
client = pymongo.MongoClient("mongodb+srv://Praveena:Niivish09@atlascluster.xpk1ff7.mongodb.net/?retryWrites=true&w=majority&ipv6=false")
db = client["Youtube_Data"]

def insert_data_into_mongodb(api_key, channel_id):
    try:
        channel_data = get_channel_data(api_key, channel_id)
        playlists = get_playlist_data(api_key, channel_id)
        videos_result = get_video_details(api_key, channel_id)
        video_ids = [video['Video_Id'] for video in videos_result]
        print(f"Video IDs: {video_ids}")

        comments_data = comment_details(video_ids)
        print(f"Comments Data: {comments_data}")

        playlists = playlists if playlists else []
        videos_result = videos_result if videos_result else []
        comments_data = comments_data if comments_data else []

        data1 = db["channel_details"]
        data1.insert_one({
            "ChannelData": channel_data,
            "PlaylistData": playlists,
            "VideoData": videos_result,
            "CommentData": comments_data
        })
        print("Data inserted into MongoDB successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

#insert_data_into_mongodb(api_key, channel_id)


#SQL Channel Table
def Channel_Sql():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Prasanna@23",
            database="youtube_project"
        )
        cursor = mydb.cursor()
        mydb.commit()
        Table1 = '''CREATE TABLE IF NOT EXISTS Channel (
            Channel_Id VARCHAR(255) PRIMARY KEY, 
            Channel_Name VARCHAR(255),
            Channel_Description TEXT,
            Channel_PListid VARCHAR(50),
            Channel_ViewsCount INT,
            Channel_subcriberCount VARCHAR(255),
            Channel_videoCount INT 
        )'''

        cursor.execute(Table1)
        mydb.commit()
        print("Channels Table created successfully")

        ChannelList = []
        db = client["Youtube_Data"]
        data1 = db["channel_details"]

        for document in data1.find({}, {"_id": 0, "ChannelData": 1}):
            if "ChannelData" in document:
                ChannelList.append(document["ChannelData"])
                df = pd.DataFrame(ChannelList)

        for index, row in df.iterrows():
            insert_query = '''INSERT into Channel(Channel_Id,
                                                Channel_Name,
                                                Channel_Description,
                                                Channel_PListid,
                                                Channel_ViewsCount,
                                                Channel_subcriberCount,
                                                Channel_videoCount)
                            VALUES(%s,%s,%s,%s,%s,%s,%s)'''

            values = (
                row['Channel_Id'],
                row['Channel_Name'],
                row['Channel_Description'],
                row['Channel_PListid'],
                row['Channel_viewCount'],
                row['Channel_subcriberCount'],
                row['Channel_videoCount']
            )

            try:
                cursor.execute(insert_query, values)
                mydb.commit()
                print("Channel values inserted successfully")
            except mysql.connector.Error as err:
                print("Already Inserted")

    except mysql.connector.Error as e:
        return f"Error connecting to MySQL: {e}"

    return "Operation completed successfully"
Channel_Sql()


#SQL Playlist Table
def Playlist_Sql():
    try:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Prasanna@23",
            database="youtube_project"
        )
        cursor = mydb.cursor()
        mydb.commit()
        Table2 = '''CREATE TABLE IF NOT EXISTS Playlist (
            Channel_Id VARCHAR(255),
            PList_ChannelName VARCHAR(255),
            PList_PTitle VARCHAR(255),
            PList_Id VARCHAR(255) PRIMARY KEY,
            PList_Desc TEXT,
            PList_publishedAt VARCHAR(255),
            PList_itemCount INT,
            FOREIGN KEY (Channel_Id) REFERENCES Channel(Channel_Id)
        )'''
        cursor.execute(Table2)
        mydb.commit()
        print("Playlist Table created successfully")

        PlayList = []
        db = client["Youtube_Data"]
        data1 = db["channel_details"]

        for document in data1.find({}, {"_id": 0, "PlaylistData": 1}):
            if "PlaylistData" in document:
                PlayList.extend(document["PlaylistData"])
        df = pd.DataFrame(PlayList)   

        df = pd.DataFrame(PlayList)
        for index, row in df.iterrows():
            channel_id = row.get('Channel_Id', '')
            insert_query = '''INSERT IGNORE into Playlist(Channel_Id,
                                            PList_ChannelName,
                                            PList_PTitle,
                                            PList_Id,
                                            PList_Desc,
                                            PList_publishedAt,
                                            PList_itemCount)
                    VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            values = (
                channel_id,
                row.get('PList_ChannelName', ''),  
                row.get('PList_PTitle', ''),
                row.get('PList_Id', ''),
                row.get('PList_Desc', ''),
                row.get('PList_publishedAt', ''),
                row.get('PList_itemCount', 0)
            )
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
                print("Playlist values inserted successfully")
            except mysql.connector.Error as err:
                print(f"Error inserting playlist values: {err}")
                return "Operation completed successfully" 
    except Exception as e:
        return f"Error: {e}"
Playlist_Sql()

#SQL Video Table
def convert_publish_date(video_publish_date):
    if isinstance(video_publish_date, list):
        video_publish_date = video_publish_date[0]  

    if isinstance(video_publish_date, datetime):
        return video_publish_date
    else:
        print(f"Invalid data type for Video_PublishDate: {type(video_publish_date)}, Input: {video_publish_date}")
        return None

def Video_Sql():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Prasanna@23",
        database="youtube_project"
    )

    cursor = mydb.cursor()

    try:
        table_creation_query = '''CREATE TABLE IF NOT EXISTS Video (
            Video_Id VARCHAR(255) PRIMARY KEY,
            Channel_Id VARCHAR(255),
            Channel_Name VARCHAR(255),
            Video_Title VARCHAR(255),
            Video_Description TEXT,
            Video_PublishDate DATETIME,
            Video_ViewCount INT,
            Video_LikeCount INT,
            Video_DislikeCount INT,
            Video_FavoriteCount INT,
            Video_CommentCount INT,
            Video_Duration VARCHAR(50),
            Video_Thumbnail VARCHAR(255),
            FOREIGN KEY (Channel_Id) REFERENCES Channel(Channel_Id)
        )'''
        cursor.execute(table_creation_query)
        mydb.commit()

        print("Video Table created successfully")

        # Fetch video data from MongoDB
        db = client["Youtube_Data"]
        data1 = db["channel_details"]
    
        insert_query = '''
        INSERT IGNORE INTO Video (Video_Id, Channel_Id, Channel_Name, Video_Title, Video_Description, 
            Video_PublishDate, Video_ViewCount, Video_LikeCount, Video_DislikeCount, 
            Video_FavoriteCount, Video_CommentCount, Video_Duration, Video_Thumbnail)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        for document in data1.find({}, {"_id": 0, "VideoData": 1}):
            if "VideoData" in document:
                video_data_list = document["VideoData"]

                for video_data in video_data_list:
                    publish_date = convert_publish_date(video_data['Video_PublishDate'])
                    if publish_date:
                        values = (
                            video_data['Video_Id'],
                            video_data['Channel_Id'],
                            video_data['Channel_Name'],
                            video_data['Video_Title'],
                            video_data['Video_Description'],
                            publish_date,
                            video_data['Video_ViewCount'],
                            video_data['Video_LikeCount'],
                            video_data['Video_DislikeCount'],
                            video_data['Video_FavoriteCount'],
                            video_data['Video_CommentCount'],
                            video_data['Video_Duration'],
                            video_data['Video_Thumbnail']
                        )

                        try:
                            print(f"Inserting values for Video_Id: {video_data['Video_Id']}, Video_PublishDate: {video_data['Video_PublishDate']}")
                            cursor.execute(insert_query, values)
                        except Exception as insert_error:
                            print(f"Error inserting values: {insert_error} for Video_Id: {video_data['Video_Id']}, Video_PublishDate: {video_data['Video_PublishDate']}")

        mydb.commit()
        print("Video values inserted successfully")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        mydb.close()

try:
    Video_Sql()
except Exception as e:
    print(f"Error: {e}")

           
#SQL Comment Table          
def Comments_Sql():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Prasanna@23",
        database="youtube_project"
    )
    cursor = mydb.cursor()
    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                        Video_Id varchar(80),
                        Comment_Text text, 
                        Comment_Author varchar(150),
                        Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        print("Comments Table Created Successfully")

        CommentList = []
        db = client["Youtube_Data"]
        data1 = db["channel_details"]

        for document in data1.find({}, {"_id": 0}):
            print("Keys in document:", document.keys()) 
            if "CommentData" in document:
                for i in range(len(document["CommentData"])):
                    CommentList.append(document["CommentData"][i])

        df3 = pd.DataFrame(CommentList)

        for index, row in df3.iterrows():
            comment_published = datetime.strptime(row['Comment_PublishAt'], "%Y-%m-%dT%H:%M:%SZ")

            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                        Video_Id,
                                        Comment_Text,
                                        Comment_Author,
                                        Comment_Published)
                VALUES (%s, %s, %s, %s, %s)
            '''
            values = (
                row['Comment_Id'],
                row['video_id'],
                row['Comment_Text'],
                row['Comment_Author'],
                comment_published  
            )

            try:
                cursor.execute(insert_query, values)
                mydb.commit()
                print("Comments Data inserted Successfully")
            except Exception as e:
                print(f"This comment already exists in the comments table: {e}")

    except Exception as e:
        print(f"Error: {e}")

Comments_Sql()

#View Table
def tables():
    Channel_Sql()
    Playlist_Sql()
    Video_Sql()
    Comments_Sql()
    return "Tables Created successfully"


def VChannel_Sql():
    ChannelList = []
    db = client["Youtube_Data"]
    data1 = db["channel_details"]
    for document in data1.find({}, {"_id": 0}):
        if "ChannelData" in document:
            ChannelList.append(document["ChannelData"])
    return ChannelList

def VPlaylist_Sql():
    PlayList = []
    data1 = db["channel_details"]

    for document in data1.find({}, {"_id": 0, "PlaylistData": 1}):
        if "PlaylistData" in document:
            PlayList.extend(document["PlaylistData"])
    playlists_table = st.dataframe(PlayList)
    return playlists_table

def VVideo_Sql():
    VideoList = []
    db = client["Youtube_Data"]
    data1 = db["channel_details"]
    for document in data1.find({}, {"_id": 0, "VideoData": 1}):
        for i in range(len(document["VideoData"])):
            VideoList.append(document["VideoData"][i])
    videos_table = st.dataframe(VideoList)
    return videos_table

def VComments_Sql():
    CommentList = []
    db = client["Youtube_Data"]
    data = db["channel_details"]
    for document in data.find({}, {"_id": 0, "CommentData": 1}):
        if "CommentData" in document:
            for i in range(len(document["CommentData"])):
                CommentList.append(document["CommentData"][i])
    return CommentList


#STREAMLIT
scrolling_text = "<h1 style='color:red; font-style: italic; font-weight: bold;'><marquee>YOUTUBE DATA HARVESTING AND WAREHOUSING</marquee></h1>"
st.markdown(scrolling_text, unsafe_allow_html=True)

st.title("Welcome to Visit!")
st.sidebar.title("YouTube Harvesting Project")
st.sidebar.markdown("""
    - **Objective:** Harvesting data from YouTube
    - **Tools Used:**
        - Python
        - YouTube API
    - **Steps:**
        1. Authenticate with YouTube API
        2. Retrieve video details
        3. Extract relevant information
        4. Analyze and visualize data
    """)

image_url1 = "data:image/jpeg;base64,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"
st.sidebar.image(image_url1, caption='Your Image Caption', use_column_width=True)

st.markdown("<a href='https://www.youtube.com' target='_blank'>Visit YouTube</a>", unsafe_allow_html=True)

#SQL connection
mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Prasanna@23",
            database="youtube_project"
        )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))
 
try:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Prasanna@23",
        database="youtube_project"
    )
    if st.button("Submit"):
        with mydb.cursor() as cursor:
    
            if question == '1. What are the names of all the videos and their corresponding channels?':
                query1 = "select Video_Title, Channel_Name from Video;"
                cursor.execute(query1)
                t1 = cursor.fetchall()
                st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

            elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
                query2 = "select Channel_Name,Channel_videoCount from Channel order by Channel_videoCount desc;"
                cursor.execute(query2)
                t2=cursor.fetchall()
                st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

            elif question == '3. What are the top 10 most viewed videos and their respective channels?':
                query3 = '''select Video_ViewCount , Channel_Name,Video_Title from Video
                                    where Video_ViewCount is not null order by Video_ViewCount desc limit 10;'''
                cursor.execute(query3)
                t3 = cursor.fetchall()
                st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

            elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
                query4 = "select Video_CommentCount ,Video_Title from Video where Video_CommentCount is not null;"
                cursor.execute(query4)
                t4=cursor.fetchall()
                st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

            elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                query5 = '''select Video_Title, Channel_Name, Video_LikeCount from Video
                                where Video_LikeCount is not null order by Video_LikeCount desc;'''
                cursor.execute(query5)
                t5 = cursor.fetchall()
                st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"])) 

            elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                query6 = '''select Video_LikeCount,Video_Title from Video;'''
                cursor.execute(query6)
                t6 = cursor.fetchall()
                st.write(pd.DataFrame(t6, columns=["like count","video title"]))

            elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                query7 = "select Channel_Name, Channel_ViewsCount from Channel;"
                cursor.execute(query7)
                t7=cursor.fetchall()
                st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

            elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
                query8 = '''select Video_Title, Video_PublishDate, Channel_Name from Video
                            where extract(year from Video_PublishDate) = 2022;'''
                cursor.execute(query8)
                t8=cursor.fetchall()
                st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

            elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                query9 =  "SELECT Channel_Name, AVG(Video_Duration) AS average_duration FROM Video GROUP BY Channel_Name;"
                cursor.execute(query9)
                t9=cursor.fetchall()
                t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
                T9=[]
                for index, row in t9.iterrows():
                    channel_title = row['ChannelTitle']
                    average_duration = row['Average Duration']
                    average_duration_str = str(average_duration)
                    T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
                st.write(pd.DataFrame(T9))


            elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                query10 = '''select Video_Id, Video_CommentCount from Video where Video_CommentCount is not null order by Video_CommentCount desc;'''
                cursor.execute(query10)
                t10 = cursor.fetchall()
                st.write(pd.DataFrame(t10, columns=['Video Id', 'NO Of Comments']))
  
    if question:
        st.write(f"## Results for Question: {question}")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    mydb.close()

