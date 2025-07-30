Case Study: Ride Sharing Analytics – Trip Data Processing
📌 Business Problem Statement
You are working as a data engineer for a ride-sharing company like Ola or Uber. The company stores massive amounts of trip-level data including distance, fare, and timestamp. Additionally, a separate dataset maintains driver details.

Your task is to:

Clean and process trip data.

Calculate meaningful metrics.

Join it with driver data.

Provide insights like top-earning drivers.

📂 Dataset Context
1. trips.csv
Trip-level ride-sharing information:

trip_id		driver_id	distance_km	fare_amount	timestamp
1		101		10.0		100.0		1656678900
2		102		5.0		50.0		1656682500
3		101		8.0		96.0		1656686100
4		103		0.0		0.0		1656689700

distance_km: Distance traveled in kilometers.

fare_amount: Total fare charged for the trip.

timestamp: UNIX time of trip end.

2. drivers.csv
Driver-level profile data:

driver_id	driver_name	city
101		Alice		New York
102		Bob		Chicago
103		Charlie		LA

🔁 Implementation Flow and Expectations
✅ Step 1: Load Data
Function: load_data(spark, trips_path, drivers_path)

Use explicitly defined schemas.

Load both datasets with headers using spark.read.csv.

🔽 Expected: Two clean DataFrames – trips_df, drivers_df.

✅ Step 2: Clean Trips Data
Function: clean_trips(trips_df)

Remove duplicate trip records.

Drop any rows with null values.

🔽 Expected:
A cleaned DataFrame with unique, complete trip records.

✅ Step 3: Convert UNIX Timestamps
Function: convert_unix_to_date(trips_df)

Add a new column trip_datetime converting UNIX timestamp to readable datetime using from_unixtime().

🔽 Expected Output Sample:

trip_id	timestamp	trip_datetime
1	1656678900	2022-07-01 07:15:00
2	1656682500	2022-07-01 08:15:00

✅ Step 4: Calculate Fare per KM
Function: calculate_avg_fare_per_km(trips_df)

Add a column fare_per_km = fare_amount / distance_km.

Use round(..., 2) to limit to 2 decimal points.

🚫 Edge Case: If distance_km == 0, output may be null.

🔽 Expected Output Sample:

fare_amount	distance_km	fare_per_km
100.0	10.0	10.00
96.0	8.0	12.00

✅ Step 5: Join with Driver Data
Function: join_with_driver(trips_df, drivers_df)

Perform inner join on driver_id.

Add columns driver_name, city to trip records.

🔽 Expected Output Sample:

trip_id	driver_id	driver_name	city
1	101	Alice	New York
2	102	Bob	Chicago

✅ Step 6: Compute Top-N Earning Drivers
Function: top_n_earning_drivers(joined_df, n)

Group by driver_id and driver_name.

Aggregate fare_amount using sum() as total_earning.

Order by total_earning DESC.

Return top n drivers.

🔽 Expected Output for Top 2:

driver_id	driver_name	total_earning
101	Alice	196.0
102	Bob	50.0

✅ Final Output Snapshot
Calling the complete function chain on the sample data and running top_n_earning_drivers(joined_df, 2) would yield:

+----------+------------+--------------+
|driver_id |driver_name|total_earning |
+----------+------------+--------------+
|101       |Alice       |196.0         |
|102       |Bob         |50.0          |
+----------+------------+--------------+

