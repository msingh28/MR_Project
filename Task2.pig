REGISTER file:/home/monisha/Downloads/piggybank-0.11.0.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Setting number of reducer tasks to 10
SET default_parallel 1;

-- Loaing the input from parameter
Flight = LOAD '$INPUT' using CSVLoader();

-- parsing only required columns as Flight_data
Flight_data = FOREACH Flight GENERATE $0 as Year, $16 as Origin, $17 as Destination, $8 as UniqueCarrier, (double)$14 as ArrDelay;

Flight_data = GROUP Flight_data by (Year, Origin, Destination) ;

Flight_ArrDelay = FOREACH Flight_data GENERATE FLATTEN(group) AS (Year, Origin, Destination), COUNT(Flight_data) AS count;

grpd = GROUP Flight_ArrDelay By Year;

top2 = FOREACH grpd {
		sorted = ORDER Flight_ArrDelay by count desc;
		top = limit sorted 2;
		GENERATE FLATTEN(top);
	}

STORE top1 into '$OUTPUT';
