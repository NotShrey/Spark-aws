 Implementation Flow and Expectations
✅ Step 1: Load Patient Data
Function: create_patient_df(spark, patient_path)

infer the schema from data

Load the patients.csv file using spark.read.csv with header=True.

🔽 Expected Output:
A DataFrame containing patient records with correctly typed columns.

✅ Step 2: Load Doctor Data
Function: create_doctor_df(spark, doctor_path)

infer the schema from data

Load the doctors.csv file using spark.read.csv with headers.

🔽 Expected Output:
A doctor DataFrame with all columns accurately typed and ready for joins.

✅ Step 3: Join Patient and Doctor Data
Function: join_patient_doctor_df(patients_df, doctors_df)

Perform an inner join on doctor_id.

Combine patient details with corresponding doctor details.

🔽 Expected Output Sample:

patient_id doctor_id	patient_name	doctor_name	specialization	units_received	units_sent
1	D1	Alice	Dr. Smith	Cardiology	3	10

✅ Step 4: Total Units Received per Doctor
Function: group_by_units_received_sum(df)

Group the joined DataFrame by doctor_id, doctor_name, or similar.

Calculate the total sum of units_received per doctor.

🔽 Expected Output Sample:

doctor_id	doctor_name	total_units_received
D1	Dr. Smith	5
D2	Dr. Jones	1

✅ Step 5: Patient Count per Doctor
Function: group_by_patient_count(df)

Group the joined DataFrame by doctor_id or doctor_name.

Count the number of patient visits per doctor.

Use alias("patient_count") for the count column.

🔽 Expected Output Sample:

doctor_id	doctor_name	patient_count
D1	Dr. Smith	2
D2	Dr. Jones	1