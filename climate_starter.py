#!/usr/bin/env python
# coding: utf-8

# In[12]:


get_ipython().run_line_magic('matplotlib', 'inline')
from matplotlib import style
style.use('fivethirtyeight')
import matplotlib.pyplot as plt


# In[13]:


import numpy as np
import pandas as pd


# In[14]:


import datetime as dt


# # Reflect Tables into SQLAlchemy ORM

# In[15]:


# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func


# In[16]:


engine = create_engine("sqlite:///Resources/hawaii.sqlite")


# In[17]:


# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# In[18]:


# We can view all of the classes that automap found
Base.classes.keys()


# In[19]:


# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


# In[20]:


# Create our session (link) from Python to the DB
session = Session(engine)


# # Exploratory Climate Analysis

# In[42]:


# Design a query to retrieve the last 12 months of precipitation data and plot the results
last_date=session.query(Measurement.date).order_by(Measurement.date.desc()).first()

for date in last_date:
    split_last_date=date.split("-")
    
split_last_date
last_year=int(split_last_date[0]); last_month=int(split_last_date[1]); last_day=int(split_last_date[2])
# Calculate the date 1 year ago from the last data point in the database
query_date =dt.date(last_year, last_month, last_day) - dt.timedelta(days=365)
print(query_date)

# Perform a query to retrieve the data and precipitation scores
last_12months_prcp=session.query(Measurement.date, Measurement.prcp).filter(Measurement.date>=query_date).order_by(Measurement.date).all()

# Save the query results as a Pandas DataFrame and set the index to the date column
df_12months=pd.DataFrame(last_12months_prcp,columns=['date', 'prcp'])
df_12months.set_index('date', inplace=True)

# Sort the dataframe by date
df_12months.head()

## Use Pandas Plotting with Matplotlib to plot the data
df_12months.plot()
plt.show()


# ![precipitation](Images/precipitation.png)

# In[44]:


# Use Pandas to calcualte the summary statistics for the precipitation data
df_12months.describe()


# ![describe](Images/describe.png)

# In[46]:


# Design a query to show how many stations are available in this dataset?
total_stations=session.query(Measurement.station).group_by(Measurement.station).count()
total_stations


# In[47]:


# What are the most active stations? (i.e. what stations have the most rows)?
active_stations=session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
# List the stations and the counts in descending order.
active_stations


# In[48]:


# Using the station id from the previous query, calculate the lowest temperature recorded, 
# highest temperature recorded, and average temperature most active station?
sel = [Measurement.station,
      func.min(Measurement.tobs),
      func.max(Measurement.tobs),
      func.avg(Measurement.tobs)]
temps=session.query(*sel).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
temps


# In[51]:


# Choose the station with the highest number of temperature observations.
top_station=temps[0]

last_12months_tobs_top_station=session.query(Measurement.date, Measurement.tobs).filter(Measurement.station==top_station).filter(Measurement.date>=query_date).order_by(Measurement.date).all()
# Query the last 12 months of temperature observation data for this station and plot the results as a histogram
top_stationdf=pd.DataFrame(last_12months_tobs_top_station,columns=['date', 'tobs'])
top_stationdf.plot.hist(bins=12)
plt.show()


# ![precipitation](Images/station-histogram.png)

# In[59]:


# This function called `calc_temps` will accept start date and end date in the format '%Y-%m-%d' 
# and return the minimum, average, and maximum temperatures for that range of dates
def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    
    return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

# function usage example
print(calc_temps('2012-02-28', '2012-03-05'))


# In[60]:


# Use your previous function `calc_temps` to calculate the tmin, tavg, and tmax 
# for your trip using the previous year's data for those same dates.
results = calc_temps("2016-02-28", "2016-03-05")
results


# In[66]:


# Plot the results from your previous query as a bar chart. 
# Use "Trip Avg Temp" as your Title
# Use the average temperature for the y value
# Use the peak-to-peak (tmax-tmin) value as the y error bar (yerr)
fig, ax = plt.subplots()
error = results[0][2] - results[0][0]
plt.bar(0, results[0][1], yerr=error,align='center',alpha=0.5,ecolor='black')
plt.title("Trip Avg Temp")
plt.xticks([],[])
plt.tight_layout()
plt.savefig(f"TripAvgTemp.png")
plt.show


# In[67]:


# Calculate the total amount of rainfall per weather station for your trip dates using the previous year's matching dates.
# Sort this in descending order by precipitation amount and list the station, name, latitude, longitude, and elevation
target = [Measurement.station, Station.name,         Station.latitude, Station.longitude,         Station.elevation,         func.avg(Measurement.prcp)]
final=session.query(*target).filter(Measurement.date < "2012-03-05").filter(Measurement.date > "2012-02-28").group_by(Measurement.station).order_by(func.avg(Measurement.prcp).desc()).all()

print(final)


# ## Optional Challenge Assignment

# In[20]:


# Create a query that will calculate the daily normals 
# (i.e. the averages for tmin, tmax, and tavg for all historic data matching a specific month and day)

def daily_normals(date):
    """Daily Normals.
    
    Args:
        date (str): A date string in the format '%m-%d'
        
    Returns:
        A list of tuples containing the daily normals, tmin, tavg, and tmax
    
    """
    
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    return session.query(*sel).filter(func.strftime("%m-%d", Measurement.date) == date).all()
    
daily_normals("01-01")


# In[21]:


# calculate the daily normals for your trip
# push each tuple of calculations into a list called `normals`

# Set the start and end date of the trip

# Use the start and end date to create a range of dates

# Stip off the year and save a list of %m-%d strings

# Loop through the list of %m-%d strings and calculate the normals for each date


# In[22]:


# Load the previous query results into a Pandas DataFrame and add the `trip_dates` range as the `date` index


# In[23]:


# Plot the daily normals as an area plot with `stacked=False`

