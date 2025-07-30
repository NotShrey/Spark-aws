🎬 Case Study: Streaming Platform Watch History – User Engagement Analysis
📌 Business Problem Statement
You are working as a data engineer at a subscription-based streaming platform (like Netflix or Amazon Prime). The company collects large-scale data on what users watch, for how long, and what genre it belongs to. You also maintain user profile data including subscription details.

Your task is to:

Clean and process watch history data.

Join it with user profile data.

Calculate user engagement metrics.

Provide insights such as frequent users and most watched genres.

📂 Dataset Context
watch_history.csv
Watch-level data capturing user activity on the platform:

user_id	show_id	genre	watch_duration	watch_timestamp
U1	S1	Comedy	30	2024-07-01T15:30:00
U2	S2	Thriller	45	2024-07-01T16:00:00
U1	S3	Comedy	60	2024-07-02T12:15:00

watch_duration: Duration in minutes

watch_timestamp: ISO 8601 timestamp when the viewing session happened

users.csv
User profile data:

user_id	name	age	subscription_type
U1	Alice	28	Premium
U2	Bob	35	Basic
U3	Charlie	23	Standard

🔁 Implementation Flow and Expectations
✅ Step 1: Load Watch History Data
Function: create_watch_history_df(spark, path)

Read the watch history CSV using an explicitly defined schema.

Add a new column watch_date parsed from watch_timestamp using to_date().

🔽 Expected Schema:

user_id	show_id	genre	watch_duration	watch_timestamp	watch_date

✅ Step 2: Load User Data
Function: create_user_df(spark, path)

Load users.csv with header and defined schema.

🔽 Expected Schema:

| user_id | name | age | subscription_type |

✅ Step 3: Join Watch History and User Data
Function: join_user_watch_df(watch_df, user_df)

Perform an inner join on user_id.

Return a combined DataFrame containing all user and watch info.

🔽 Expected Schema:

| user_id | show_id | genre | watch_duration | watch_timestamp | watch_date | name | age | subscription_type |

✅ Step 4: Compute Average Watch Duration Per User
Function: compute_avg_watch_duration(watch_df)

Group by user_id and compute average of watch_duration.

Rename column to avg_watch_duration.

🔽 Expected Output Sample:

user_id	avg_watch_duration
U1	45.0
U2	50.0

✅ Step 5: Categorize Users by Watch Duration
Function: categorize_watchers(watch_df)

Add a new column watch_category using when:

<30 mins: "Light"

30–60 mins: "Moderate"

>60 mins: "Heavy"

🔽 Expected Sample:

user_id	watch_duration	watch_category
U1	30	Moderate
U2	45	Moderate
U3	90	Heavy

✅ Step 6: Get List of Unique Genres Watched
Function: get_unique_genres(watch_df)

Extract a Python list of distinct genres from the DataFrame.

🔽 Expected Output:

python
Copy
Edit
['Comedy', 'Thriller', 'Drama']
✅ Step 7: Filter Frequent Users
Function: filter_frequent_users(watch_df, min_watches)

Group by user_id and count number of shows watched.

Filter users with watch count >= min_watches.

🔽 Expected Output (min_watches = 2):

user_id	watch_count
U1	3
U3	2

✅ Step 8: Identify Most Watched Genre
Function: most_watched_genre(watch_df)

Group by genre and count.

Return the genre watched most frequently.

🔽 Expected Output:

python
Copy
Edit
"Comedy"

