REGISTER file:/home/adadlani/Downloads/piggybank-0.11.0.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Setting number of reducer tasks to 10
SET default_parallel 1;

-- Loaing the input from parameter
Flight = LOAD '$INPUT' using CSVLoader();

-- parsing only required columns as Flight1_data
Flight_data = FOREACH Flight GENERATE $0 as Year, $8 as UniqueCarrier, (double)$14 as ArrDelay;

Flight_data = GROUP Flight_data by (Year,UniqueCarrier) ;

Flight_ArrDelay = FOREACH Flight_data GENERATE FLATTEN(group) AS (Year, UniqueCarrier), AVG(Flight_data.ArrDelay);

STORE Flight_ArrDelay into '$COUNT';
