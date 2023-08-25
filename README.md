Introduction:

YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and analyse data from numerous YouTube channels. SQL, MongoDB, and Streamlit are used in the project to develop a user-friendly application that allows users to retrieve, save, and query YouTube channel,video and comment data.

TOOLS AND LIBRARIES USED:

This project requires the following components:

GOOGLE API CLIENT:

The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.


STREAMLIT:

Streamlit library was used to create a user-friendly UI that enables users to interact with the programme and carry out data retrieval and analysis operations.

PYTHON:

Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

 MONGODB ATLAS:

MongoDB Atlas is a comprehensive cloud-based database service designed specifically for MongoDB. In this project, MongoDB Atlas is utilized to store the data obtained from YouTube's Data API v3. By leveraging MongoDB Atlas, developers can benefit from a fully managed and hassle-free database solution that ensures the reliable and scalable storage and retrieval of data, thereby facilitating efficient data management.

MySQL:

MySQL is a relational database management system based on SQL – Structured Query Language. The application is used for a wide range of purposes, including data warehousing, e-commerce, and logging applications.It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.

REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlit

3.PIL

4.pymongo

5.pandas

6.mysql.connector

7.sqlalchemy

8.time

9.datetime

Approach:
Start by setting up a Streamlit application using the python library "streamlit”, which    provides an easy-to-use interface for users to enter a YouTube channel ID,view channel details, and select channels to migrate.
Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilising the Google API client library for Python.

Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the same data in the database in 3 different collections.

Transferring the collected data from multiple channels namely the channels,videos and comments to a SQL data warehouse, utilising a SQL database like MySQL or PostgreSQL for this purpose.

Utilise SQL queries to join tables within the SQL data warehouse and retrieve
specific channel data based on user input
            
FEATURES:

The following functions are available in the YouTube Data Harvesting and Warehousing application:

Retrieval of channel ,video and  comment data from YouTube using the YouTube API.

Storage of data in a MongoDB database as a data lake.

Migration of data from the data lake to a SQL database

Leveraging SQL to query the data warehouse.

Creating graphs and charts using streamlit data visualisation tool to make it simpler for users to interpret the data.


LinkedIn Profile: https://www.linkedin.com/posts/priyanka-pal-303367224_greetings-to-all-im-priyanka-and-id-like-activity-7100163774791188481-Npm0?utm_source=share&utm_medium=member_desktop


