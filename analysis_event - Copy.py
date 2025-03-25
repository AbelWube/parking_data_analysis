import psycopg2
import pandas as pd
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# User input
parking_id = int(input("Enter the parking ID: "))  # Parking lot ID
event_start = input("Enter the event start date (YYYY-MM-DD): ")  # Event start date
event_duration = int(input("Enter the event duration in days: "))  # Event duration

# Convert event start date to datetime
event_start_date = pd.to_datetime(event_start)
event_end_date = event_start_date + pd.Timedelta(days=event_duration - 1)

# Database connection
hostname = 'localhost'
database = 'parkingOccupHasselt_2025mar13'
username = 'postgres'
password = 'Zenani@Abel19'
port_id = '5432'

conn = psycopg2.connect(
    host=hostname, dbname=database, user=username, password=password, port=port_id
)
cur = conn.cursor()

# Fetch dataset range
query_min_max = f"""
    SELECT MIN(DATE(datetime)) AS min_date, MAX(DATE(datetime)) AS max_date
    FROM occupancy
    WHERE id = {parking_id}
"""
df_min_max = pd.read_sql(query_min_max, conn)
data_start_date = df_min_max["min_date"].iloc[0]
data_end_date = df_min_max["max_date"].iloc[0]

# Define before and after periods based on dataset range
before_start_date = data_start_date  
before_end_date = event_start_date - pd.Timedelta(days=1)  

after_start_date = event_end_date + pd.Timedelta(days=1)  
after_end_date = data_end_date  

# SQL query to exclude event period in other years
query = f"""
    WITH parking_data AS (
        SELECT 
            id, 
            DATE(datetime) AS date, 
            EXTRACT(DOW FROM datetime) AS day_of_week,
            EXTRACT(YEAR FROM datetime) AS year,
            AVG(available) AS avg_available
        FROM occupancy
        WHERE id = {parking_id}
        AND available > -1
        AND capacity > 0
        GROUP BY id, DATE(datetime), EXTRACT(DOW FROM datetime), EXTRACT(YEAR FROM datetime)
    )
    
    SELECT 
        date,
        day_of_week,
        avg_available,
        CASE 
            WHEN date BETWEEN DATE '{before_start_date.strftime('%Y-%m-%d')}' 
                          AND DATE '{before_end_date.strftime('%Y-%m-%d')}' THEN 'Non-Event'
            WHEN date BETWEEN DATE '{event_start}' 
                          AND DATE '{event_end_date.strftime('%Y-%m-%d')}' THEN 'Event'
            WHEN date BETWEEN DATE '{after_start_date.strftime('%Y-%m-%d')}' 
                          AND DATE '{after_end_date.strftime('%Y-%m-%d')}' THEN 'Non-Event'
        END AS period
    FROM parking_data
    WHERE date BETWEEN DATE '{before_start_date.strftime('%Y-%m-%d')}' 
                   AND DATE '{after_end_date.strftime('%Y-%m-%d')}'  
    AND NOT EXISTS (
        SELECT 1 FROM parking_data p2
        WHERE p2.year <> EXTRACT(YEAR FROM date)
        AND p2.date BETWEEN DATE '{event_start}' AND DATE '{event_end_date.strftime('%Y-%m-%d')}'
    )
    ORDER BY date;
"""

# Fetch data from the database
df = pd.read_sql(query, conn)
conn.close()

# Check if data was returned
if df.empty:
    print("No data found for the specified date ranges.")
else:
    # Mapping numerical day_of_week to names
    day_name_map = {
        0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 
        4: 'Thursday', 5: 'Friday', 6: 'Saturday'
    }
    df['day_name'] = df['day_of_week'].replace(day_name_map)

    # Select specific day for analysis
    specific_day = input("Enter the day of the week to compare (e.g., Monday): ")
    df_day = df[df['day_name'] == specific_day]

    non_event_data = df_day[df_day['period'] == 'Non-Event']
    event_data = df_day[df_day['period'] == 'Event']
    
    print(non_event_data)
    print(event_data)
    


    if event_data.empty or non_event_data.empty:
        print(f"Not enough data for {specific_day} in either the Event or Non-Event period.")
    else:
        # Perform T-test
        def perform_t_test(event_data, non_event_data):
            if len(event_data) == 1:
                pop_mean = np.mean(non_event_data['avg_available'])
                t_stat, p_value = stats.ttest_1samp(event_data['avg_available'], pop_mean)
                test_type = "One-Sample t-test"
            else:
                stat, p_var = stats.levene(event_data['avg_available'], non_event_data['avg_available'])
                equal_var = p_var >= 0.05
                t_stat, p_value = stats.ttest_ind(
                    event_data['avg_available'], 
                    non_event_data['avg_available'], 
                    equal_var=equal_var
                )
                test_type = "Welch’s t-test" if not equal_var else "Independent t-test"
            
            result = f"{test_type}: T-stat = {t_stat:.4f}, P-value = {p_value:.4f} -> "
            result += "Statistically significant (reject null hypothesis)" if p_value < 0.05 else "Not statistically significant (fail to reject null hypothesis)"
            return result

        print(f"\nT-test Results for {specific_day}:")
        print(perform_t_test(event_data, non_event_data))

        # Outlier Detection (Z-score & IQR)
        def detect_outliers(data, reference_data):
            # Z-score method
            mean_non_event = np.mean(reference_data)
            std_non_event = np.std(reference_data)
            z_scores = (data - mean_non_event) / std_non_event
            z_outliers = np.where(np.abs(z_scores) > 3)[0]  # Z-score threshold = 3

            # IQR Method
            Q1 = np.percentile(reference_data, 25)
            Q3 = np.percentile(reference_data, 75)
            print("Q1: ")
            print(Q1)
            print("Q3: ")
            print(Q3)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            iqr_outliers = np.where((data < lower_bound) | (data > upper_bound))[0]

            return z_outliers, iqr_outliers


        # Apply outlier detection
        zscore_outliers_event, iqr_outliers_event = detect_outliers(
            event_data['avg_available'], non_event_data['avg_available']
        )

        print(f"\nOutliers detected for {specific_day} (Event Period) based on Non-Event Distribution:")
        print(f"Z-score outliers (Index Positions): {zscore_outliers_event}")
        print(f"IQR outliers (Index Positions): {iqr_outliers_event}")

        # Visualization
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='period', y='avg_available', data=df_day)
        plt.title(f"Parking Availability for {specific_day} - Event vs Non-Event")
        plt.ylabel("Average Available Spaces")
        plt.xlabel("Category (Event vs Non-Event)")
        plt.show()
    