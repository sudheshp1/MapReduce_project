import happybase
# create connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)
def batch_load_data(filename, tablename):
    print("starting batch insert of "+filename)
    file = open(filename, 'r')
    connection.open()
    table = connection.table(tablename)
    i = 0
    with table.batch(batch_size=50000) as record:
        for line in file:
            if i!=0:
                temp = line.strip().split(",")
                record.put(temp[1]+temp[2] ,{'cf1:VendorID': str(temp[0]), 'cf1:tpep_pickup_datetime':str(temp[1]), 'cf1:tpep_dropoff_datetime': str(temp[2]), 'cf1:passenger_count': str(temp[3]),'cf1:trip_distance': str(temp[4]), 'cf1:RatecodeID': str(temp[5]), 'cf1:store_and_fwd_flag':str(temp[6]), 'cf1:PULocationID': str(temp[7]), 'cf1:DOLocationID': str(temp[8]), 'cf1:payment_type':str(temp[9]),'cf1:fare_amount': str(temp[10]), 'cf1:extra': str(temp[11]), 'cf1:mta_tax': str(temp[12]),'cf1:tip_amount': str(temp[13]), 'cf1:tolls_amount': str(temp[14]), 'cf1:improvement_surcharge': str(temp[15]), 'cf1:total_amount': str(temp[16]), 'cf1:congestion_surcharge': str(temp[17]), 'cf1:airport_fee': str(temp[18])})
                i+=1
    file.close()
    print("batch insert done")
    connection.close()
    
batch_load_data('yellow_tripdata_2017-03.csv', 'trip_log')
batch_load_data('yellow_tripdata_2017-04.csv', 'trip_log')