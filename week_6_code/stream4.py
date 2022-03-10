# Join yellow and green
# Adapted from windowing.py

from datetime import timedelta
import faust
from taxi_rides2 import TaxiRide2


app1 = faust.App('datatalksclub.hw.stream', broker='kafka://localhost:9092')
topic1 = app1.topic('datatalkclub.yellow_taxi_ride2.json', value_type=TaxiRide2)

app2 = faust.App('datatalksclub.hw.stream2', broker='kafka://localhost:9092')
topic2 = app2.topic('datatalkclub.green_taxi_ride.json', value_type=TaxiRide2)


table1 = app1.Table('yellow_rides_windowed', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)

table2 = app2.Table('green_rides_windowed', default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)

# table_both = app1.Table('yellow_rides_windowed', default=int).tumbling(
#     timedelta(minutes=1),
#     expires=timedelta(hours=1),
# )


# app3 = faust.App('datatalksclub.hw.stream1', broker='kafka://localhost:9092')
# topic3 = app3.topic('datatalkclub.hw.stream', value_type=TaxiRide2)


@app1.agent(topic1)
async def join_streams(stream):
    async for window in stream:
        both[window] = table1[window].joins.Join(table2[window])
        print(both)




if __name__ == '__main__':
    app1.main()
    app2.main()
