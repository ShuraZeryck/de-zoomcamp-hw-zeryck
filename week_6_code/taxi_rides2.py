import faust


class TaxiRide2(faust.Record, validation=True):
    vendorId: str
    trip_distance: float
    total_amount: float
