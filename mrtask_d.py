
# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageTripTime(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]
    
    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            data = line.strip().split(',')
            pickup_location = data[7]
            trip_time = float(data[4])
            yield pickup_location, (trip_time, 1)
    
    def reducer(self, key, values):
        total_trip_time = 0
        total_count = 0
        for trip_time, count in values:
            total_trip_time += trip_time
            total_count += count
        yield None, (total_trip_time / total_count, key)
    
    def final_reducer(self, _, values):
        sorted_values = sorted(values, reverse=True)
        for average_trip_time, pickup_location in sorted_values:
            yield pickup_location, average_trip_time


if __name__ == '__main__':
    AverageTripTime.run()
