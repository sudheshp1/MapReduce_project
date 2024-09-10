
# What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep

class PaymentTypesCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]
    
    def mapper(self, _, line):
        if not line.startswith('VendorID'):
            data = line.strip().split(',')
            payment_type = data[9]
            yield payment_type, 1
    
    def reducer(self, key, values):
        total_count = sum(values)
        yield None, (total_count, key)
    
    def final_reducer(self, _, values):
        sorted_values = sorted(values, reverse=True)
        for count, payment_type in sorted_values:
            yield payment_type, count


if __name__ == '__main__':
    PaymentTypesCount.run()
