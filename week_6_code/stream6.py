from datetime import timedelta
import faust
from taxi_rides2 import TaxiRide2



app = faust.App('datatalksclub.hw.stream6', broker='kafka://localhost:9092')

# topic_y = app.topic('datatalkclub.yellow_taxi_ride2.json', value_type=TaxiRide2)
# topic_g = app.topic('datatalkclub.green_taxi_ride.json', value_type=TaxiRide2)

topic_both = app.topic('datatalkclub.yellow_taxi_ride.json', 'datatalkclub.green_taxi_ride.json', value_type=TaxiRide2)


# s_y = app.stream(topic_y)
# s_g = app.stream(topic_g)

# s_both = app.stream(topic_both)



@app.agent(topic_both)
async def join_streams(stream):
    async for value in s_both:
        print(value)




# table_both = app.Table('rides_windowed_joined', default=int).tumbling(
#     timedelta(minutes=1),
#     expires=timedelta(hours=1),
# )


# @app.agent(topic_both)
# async def join_streams(stream):
#     async for value in s_both:
#         table_both # ...




if __name__ == '__main__':
    app.main()
