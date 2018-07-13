import sys
sys.path.insert(0, '.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    '''
    Create a Spark program to read the house data from in/RealEstate.csv,
    output the average price for houses with different number of bedrooms.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

    3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.

    '''
    conf = SparkConf().setAppName("housePrice").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("in/RealEstate.csv")
    realEstateRdd = lines.map(lambda line:  Utils.COMMA_DELIMITER.split(line)).filter(lambda fields: fields[0] != 'MLS')
    realEstateRdd = realEstateRdd.map(lambda fields: (int(fields[3]), (float(fields[2]), 1)))
    aggregatedRdd = realEstateRdd.reduceByKey(lambda row1, row2: (row1[0] + row2[0], row1[1] + row2[1]))
    aggregatedRdd = aggregatedRdd.map(lambda row: (row[0], row[1][0] / row[1][1]))
    values = aggregatedRdd.collect()

    for value in values:
        print(f'Number of rooms: {value[0]}, average price: {value[1]}')
