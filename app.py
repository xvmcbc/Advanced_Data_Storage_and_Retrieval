# Import class Flask
from flask import Flask, jsonify

# Import dependencies
import pandas as pd
import datetime as dt
from datetime import datetime

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

# Engine creation
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
conn = engine.connect()

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# FLASK-----------------------------------------------------------------------------------------------------------------------------------------
# Generate the app
app = Flask(__name__)

# Home page. List all routes that are available.
@app.route("/")
def home():
    return (
        f'<h3>Welcome to the Climate API</h3>'
        f'-----------------------------------------------<br>'
        f'<b>Available Routes:</b><br>'
        f"<a href="'/api/v1.0/precipitation'">Link Precipitation</a><br>"
        f"<a href="'/api/v1.0/stations'">Link Stations</a><br>"
        f"<a href="'/api/v1.0/tobs'">Link Tobs</a><br>"
        f"<br>"
        f"With parameters to get Min, Max, Avg Temperatures use: /api/v1.0/'<'start'>' and /api/v1.0/'<'start'>'/'<'end'>'<br>"
        f"Examples:<br>"
        f"<a href="'http://127.0.0.1:5000/api/v1.0/2015-08-23'">http://127.0.0.1:5000/api/v1.0/2015-08-23</a><br>"
        f"<a href="'http://127.0.0.1:5000/api/v1.0/2015-08-23/2017-08-23'">http://127.0.0.1:5000/api/v1.0/2015-08-23/2017-08-23</a><br>"
    )

# Precipitation---------------------------------------------------------------------------------------------------------------------------------
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Obtain the latest date on the database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Decompose the date we obtained as last date to get the 12 months before 
    str_date = last_date[0]
    str_year = int(str_date[0:4]) - 1
    str_month = int(str_date[5:7])
    str_day = int(str_date[8:10])
    year_bf = dt.date(str_year, str_month, str_day)

    # Retrieve the last 12 months of precipitation data.  Select only prcp and date. Load the query results into a Pandas DataFrame
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= year_bf).all()
    precip_data = pd.DataFrame(results, columns=['Date', 'Prcp'])
    # Eliminate the nulls
    precip_data.dropna(inplace=True)

    # Convert the query results to a Dictionary using date as the key and prcp as the value
    precip_dict = precip_data.set_index('Date').to_dict()

    #Return the JSON representation of your dictionary   
    return jsonify(precip_dict)

# Stations-------------------------------------------------------------------------------------------------------------------------------------
@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Retrieve a list of stations from the dataset
    stations = session.query(Station.station).all()
    stations_df = pd.DataFrame(stations, columns=['Station'])

    # Return a JSON list of stations from the dataset.
    stations_dict = stations_df.to_dict()
    return jsonify(stations_dict)

# Temperature Observations-----------------------------------------------------------------------------------------------------------------
@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Obtain the latest date on the database
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    # Decompose the date we obtained as last date to get the 12 months before 
    str_date = last_date[0]
    str_year = int(str_date[0:4]) - 1
    str_month = int(str_date[5:7])
    str_day = int(str_date[8:10])
    year_bf = dt.date(str_year, str_month, str_day)

    # Query for the dates and temperature observations from a year from the last data point.
    tobs_ly = session.query(Measurement.date,Measurement.tobs).filter(Measurement.date >= year_bf).order_by(Measurement.date).all()
    tobs_df = pd.DataFrame(tobs_ly, columns=['Date','Temp'])

    # Return a JSON list of Temperature Observations (tobs) for the previous year
    tobs_dict = tobs_df.set_index('Date').to_dict()
    return jsonify(tobs_dict)

# Min, Avg and Max Temperature according a start parameter ------------------------------------------------------------------------------------
@app.route("/api/v1.0/<start>")
def statistics(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Decompose the start date we obtained as a parameter to convert it to a date data type
    s_year = int(start[0:4])
    s_month = int(start[5:7])
    s_day = int(start[8:10])
    s_date = dt.date(s_year, s_month, s_day)
    
    # When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date
    t_min = session.query(func.min(Measurement.tobs)).filter(Measurement.date >= s_date).all()
    t_max = session.query(func.max(Measurement.tobs)).filter(Measurement.date >= s_date).all()
    t_avg = session.query(func.avg(Measurement.tobs)).filter(Measurement.date >= s_date).all()

    # Structure the data in a dictionary
    dict_temp =	{
        "Temp_MAX": "0",
        "Temp_MIN": "0",
        "Temp_AVG": "0" }
    
    # Assign the values obtained    
    dict_temp["Temp_MAX"] = t_max[0]
    dict_temp["Temp_MIN"] = t_min[0]
    dict_temp["Temp_AVG"] = t_avg[0]

    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start
    return jsonify(dict_temp) 

# Min, Avg and Max Temperature according a two parameters---------------------------------------------------------------------------------------
@app.route("/api/v1.0/<start>/<end>")
def statistics_2(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Decompose the start and end date we obtained as a parameters to convert it to a date data types
    s_year_s = int(start[0:4])
    s_month_s = int(start[5:7])
    s_day_s = int(start[8:10])
    s_date_s = dt.date(s_year_s, s_month_s, s_day_s)

    s_year_e = int(end[0:4])
    s_month_e = int(end[5:7])
    s_day_e = int(end[8:10])
    s_date_e = dt.date(s_year_e, s_month_e, s_day_e)

    # Given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive
    t_min = session.query(func.min(Measurement.tobs)).filter(Measurement.date >= s_date_s).filter(Measurement.date <= s_date_e).all()
    t_max = session.query(func.max(Measurement.tobs)).filter(Measurement.date >= s_date_s).filter(Measurement.date <= s_date_e).all()
    t_avg = session.query(func.avg(Measurement.tobs)).filter(Measurement.date >= s_date_s).filter(Measurement.date <= s_date_e).all()

    # Structure the data in a dictionary
    dict_temp =	{
        "Temp_MAX": "0",
        "Temp_MIN": "0",
        "Temp_AVG": "0"}
        
    # Assign the values obtained       
    dict_temp["Temp_MAX"] = t_max[0]
    dict_temp["Temp_MIN"] = t_min[0]
    dict_temp["Temp_AVG"] = t_avg[0]

    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for dates between 
    # the start and end date inclusive
    return jsonify(dict_temp) 

if __name__ == "__main__":
    app.run(debug=True)
