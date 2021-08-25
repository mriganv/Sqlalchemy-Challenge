import numpy as np
import sqlalchemy
import datetime as dt
import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

##################
# Database Setup
##################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

###############
# Flask Setup
###############

app = Flask(__name__)

###############
# Flask Routes
###############

#############
# Home Page
#############

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/yyyy-mm-dd<br/>"
        f"/api/v1.0/yyyy-mm-dd/yyyy-mm-dd<br/>"
        f"<br/>"
        f"Please enter dates in (yyyy-mm-dd) format<br/>"
        f"Data available between 2010-01-01 to 2017-08-23"
        
    )

####################################################################################################################################

#####################
# v1.0/precipitation
#####################

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    recent_date = session.query(Measurement.date).order_by(
                Measurement.date.desc()).first()[0]

    # Starting from the most recent data point in the database.
    #convert string date to date format
    recent_date = (dt.datetime.strptime(recent_date, "%Y-%m-%d")).date()

    # Calculate the date one year from the last date in data set.
    one_yearago_date = recent_date - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).\
                filter((Measurement.date >= one_yearago_date)
                & (Measurement.date <= recent_date)).all()

    session.close()

    # Create a dictionary from the row data and append to a list of dates_prcp
    dates_prcp = []
    for date, prcp in results:
        datesprcp_dict = {}
        datesprcp_dict["Date"] = date
        datesprcp_dict["Prcp"] = prcp
        dates_prcp.append(datesprcp_dict)

    return jsonify(dates_prcp)

####################################################################################################################################

#####################
# /api/v1.0/stations
#####################

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    # results = session.query(Measurement.station).all()
    results = session.query(Station.station).\
        order_by(Station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

####################################################################################################################################

#################
# /api/v1.0/tobs
#################

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query to find the most active station 
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).first()[0]

    # Find the most recent date in the data set.
    recent_date = session.query(Measurement.date).order_by(
        Measurement.date.desc()).first()[0]

    # Starting from the most recent data point in the database.
    #convert string date to date format
    recent_date = (dt.datetime.strptime(recent_date, "%Y-%m-%d")).date()

    # Calculate the date one year from the last date in data set.
    one_yearago_date = recent_date - dt.timedelta(days=365)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(Measurement.date, Measurement.tobs).\
        filter((Measurement.date >= one_yearago_date)
                & (Measurement.date <= recent_date)).\
        filter(Measurement.station == "USC00519281").all()

    session.close()

    # Create a dictionary from the row data and append to a list of dates_tobs
    dates_tobs = []

    for date, tobs in results:
        datestobs_dict = {}
        datestobs_dict["Date"] = date
        datestobs_dict["Tobs"] = tobs
        # --- append each result's dictionary to the dates_tobs list ---
        dates_tobs.append(datestobs_dict)

    # --- return the JSON list of normals ---
    return jsonify(dates_tobs)

####################################################################################################################################


####################
# /api/v1.0/<start>
####################

@app.route("/api/v1.0/<start>")
def agg_returns(start):
    
    session = Session(engine)
    first_date = session.query(Measurement.date).order_by(
        Measurement.date.asc()).first()[0]
    last_date = session.query(Measurement.date).order_by(
        Measurement.date.desc()).first()[0]
    # Query for the date, min, max and average temperature
    sel = [Measurement.date, func.min(Measurement.tobs), func.avg(
            Measurement.tobs), func.max(Measurement.tobs)]
    # For all temperatures in the given date
    results = session.query(*sel).\
                filter(func.strftime("%Y-%m-%d", Measurement.date) >= func.strftime("%Y-%m-%d", start)).\
                    group_by(Measurement.date).all()

    # --- close the session ---
    session.close()
    a = datetime.datetime.strptime(first_date, "%Y-%m-%d")
    b = datetime.datetime.strptime(last_date, "%Y-%m-%d")
    b = b + datetime.timedelta(days=1)
    date_array = \
        (a + datetime.timedelta(days=x) for x in range(0, (b-a).days))
    dates = []
    for date_object in date_array:
        dates.append(date_object.strftime("%Y-%m-%d"))

    if start in dates:
        Temperatures = []

        for temp_date, tmin, tavg, tmax in results:

            #   Create a dictionary from the row data and append to a list of temps_dict
            temps_dict = {}
            temps_dict["Date"] = temp_date
            temps_dict["T-Min"] = tmin
            temps_dict["T-Avg"] = tavg
            temps_dict["T-Max"] = tmax

            # --- append each result's dictionary to the Temperatures list ---
            Temperatures.append(temps_dict)
        return jsonify(Temperatures)

    if start not in dates:
        return jsonify({"error": f"Not a valid {start} Start Date. Date Range is {first_date} to {last_date}"}), 404

    
    
####################################################################################################################################


#####################################
# /api/v1.0/<start_date>/<end_date>"
#####################################


@app.route('/api/v1.0/<start>/<end>')
def get_t_start_stop(start, end):
    session = Session(engine)
    last_date = session.query(Measurement.date).order_by(
        Measurement.date.desc()).first()[0]
    first_date = session.query(Measurement.date).order_by(
    Measurement.date.asc()).first()[0]
    session = Session(engine)
    sel = [Measurement.date, func.min(Measurement.tobs), func.avg(
    Measurement.tobs), func.max(Measurement.tobs)]
    results =  session.query(*sel).\
                filter(func.strftime("%Y-%m-%d", Measurement.date) >= func.strftime("%Y-%m-%d", start)).\
                filter(func.strftime("%Y-%m-%d", Measurement.date) <= func.strftime("%Y-%m-%d", end)).\
                    group_by(Measurement.date).all()

    # --- close the session ---
    session.close()

    a = datetime.datetime.strptime(first_date, "%Y-%m-%d")
    b = datetime.datetime.strptime(last_date, "%Y-%m-%d")
    b = b + datetime.timedelta(days=1)
    date_array = \
    (a + datetime.timedelta(days=x) for x in range(0, (b-a).days))
    dates = []
    for date_object in date_array:
        dates.append(date_object.strftime("%Y-%m-%d"))

    if (start and end) in dates:
        Temperatures = []

        for temp_date, tmin, tavg, tmax in results:
            #   Create a dictionary from the row data and append to a list of temps_dict
            temps_dict = {}
            temps_dict["Date"] = temp_date
            temps_dict["T-Min"] = tmin
            temps_dict["T-Avg"] = tavg
            temps_dict["T-Max"] = tmax

            # --- append each result's dictionary to the Temperatures list ---
            Temperatures.append(temps_dict)
        # --- return the JSON list of normals ---
        return jsonify(Temperatures)

    if start not in dates and end in dates:
        return jsonify({"error": f"Not a valid {start} Start Date. Date Range is {first_date} to {last_date}"}), 404

    if start not in dates and end not in dates:
        return jsonify({"error": f"Not a valid {start} Start Date and not a valid {end} End Date. Date Range is {first_date} to {last_date}"}), 404
        
    if start in dates and end not in dates:
        return jsonify({"error": f"Not a valid {end} End Date. Date Range is {first_date} to {last_date}"}), 404
    
    ######################################################################################################################################    
    
if __name__ == "__main__":
    app.run(debug=True)
