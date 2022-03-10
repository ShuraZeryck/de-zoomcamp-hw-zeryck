# Join yellow and green
# Adapted from windowing.py

from datetime import timedelta
import faust
from taxi_rides2 import TaxiRide2


app = faust.App('datatalksclub.hw.stream', broker='kafka://localhost:9092')

topic_y = app.topic('datatalkclub.yellow_taxi_ride2.json', value_type=TaxiRide2)
topic_g = app.topic('datatalkclub.green_taxi_ride.json', value_type=TaxiRide2)

topic_both = app.topic('datatalkclub.yellow_taxi_ride.json', 'datatalkclub.green_taxi_ride.json', value_type=TaxiRide2)


# table1 = app.Table('yellow_rides_windowed', default=int).tumbling(
#     timedelta(minutes=1),
#     expires=timedelta(hours=1),
# )

# table2 = app.Table('green_rides_windowed', default=int).tumbling(
#     timedelta(minutes=1),
#     expires=timedelta(hours=1),
# )

table_both = app.Table('rides_windowed_joined', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)



@app.agent(topic_both)
async def join_streams(stream):
    async for window in stream:
        table_both[window] = topic_y.stream().join(topic_g.stream())




if __name__ == '__main__':
    app.main()
