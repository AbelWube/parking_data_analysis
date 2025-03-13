import psycopg2
import pandas as pd
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt
from math import ceil

# input
parking_id = int(input("Enter the parking ID: "))  
event_start = input("Enter the event start date (YYYY-MM-DD): ")  
event_duration = int(input("Enter the event duration in days: "))  

a = ceil(event_duration / 7) * 7  

# Convert event start date to datetime
event_start_date = pd.to_datetime(event_start)
event_end_date = event_start_date + pd.Timedelta(days=event_duration - 1)

# Compute before event period
before_start_date = event_start_date - pd.Timedelta(days=a)
before_end_date = before_start_date + pd.Timedelta(days=event_duration - 1)

# Compute after event period
after_start_date = event_start_date + pd.Timedelta(days=a)
after_end_date = after_start_date + pd.Timedelta(days=event_duration - 1)

# Database connection
hostname = 'localhost'
database = 'parkingOccup_2025jan22'
username = 'postgres'
password = 'Zenani@Abel19'
port_id = '5432'

conn = psycopg2.connect(
    host=hostname, dbname=database, user=username, password=password, port=port_id
)
cur = conn.cursor()

query = f"""
    WITH parking_data AS (
        SELECT 
            id, 
            DATE(datetime) AS date, 
            EXTRACT(DOW FROM datetime) AS day_of_week,
            AVG(available) AS avg_available
        FROM occupancy
        WHERE id = {parking_id}
        AND available > -1
        AND capacity > 0
        GROUP BY id, DATE(datetime), EXTRACT(DOW FROM datetime)
    )
    
    SELECT 
        date,
        day_of_week,
        avg_available,
        CASE 
            WHEN date BETWEEN DATE '{before_start_date.strftime('%Y-%m-%d')}' 
                          AND DATE '{before_end_date.strftime('%Y-%m-%d')}' THEN 'Before Event'
            WHEN date BETWEEN DATE '{event_start}' 
                          AND DATE '{event_end_date.strftime('%Y-%m-%d')}' THEN 'During Event'
            WHEN date BETWEEN DATE '{after_start_date.strftime('%Y-%m-%d')}' 
                          AND DATE '{after_end_date.strftime('%Y-%m-%d')}' THEN 'After Event'
        END AS period
    FROM parking_data
    WHERE date BETWEEN DATE '{before_start_date.strftime('%Y-%m-%d')}' 
                   AND DATE '{after_end_date.strftime('%Y-%m-%d')}'  
    ORDER BY date;
"""

df = pd.read_sql(query, conn)
conn.close()

# Remove rows with period as None
df = df[df['period'].notna()]

# Mapping numerical day_of_week to names
day_name_map = {
    0: 'Sunday', 1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday'
}
df['day_name'] = df['day_of_week'].replace(day_name_map)

# Separate weekdays and weekends
df['is_weekend'] = df['day_of_week'].apply(lambda x: 'Weekend' if x in [0, 6] else 'Weekday')

# Print 
print("\nFull Dataset Before Analysis:")
print(df)

# Split into subgroups
before_weekday = df[(df['period'] == 'Before Event') & (df['is_weekend'] == 'Weekday')]
before_weekend = df[(df['period'] == 'Before Event') & (df['is_weekend'] == 'Weekend')]

during_weekday = df[(df['period'] == 'During Event') & (df['is_weekend'] == 'Weekday')]
during_weekend = df[(df['period'] == 'During Event') & (df['is_weekend'] == 'Weekend')]

after_weekday = df[(df['period'] == 'After Event') & (df['is_weekend'] == 'Weekday')]
after_weekend = df[(df['period'] == 'After Event') & (df['is_weekend'] == 'Weekend')]

# Perform Independent T-tests
def t_test_and_interpret(group1, group2, label1, label2):
    if len(group1) == 0 or len(group2) == 0:
        return f"Insufficient data for {label1} vs {label2}, skipping test."
    
    t_stat, p_value = stats.ttest_ind(group1, group2)
    result = f"T-stat = {t_stat:.4f}, P-value = {p_value:.4f} -> "
    
    if p_value < 0.05:
        result += f"Statistically significant (reject null hypothesis) for {label1} vs {label2}"
    else:
        result += f"Not statistically significant (fail to reject null hypothesis) for {label1} vs {label2}"
    
    return result

# Weekday comparisons
print("\nT-test Results for Weekdays:")
print("Before vs During:", t_test_and_interpret(before_weekday['avg_available'], during_weekday['avg_available'], 'Before', 'During'))
print("During vs After:", t_test_and_interpret(during_weekday['avg_available'], after_weekday['avg_available'], 'During', 'After'))
print("Before vs After:", t_test_and_interpret(before_weekday['avg_available'], after_weekday['avg_available'], 'Before', 'After'))

# Weekend comparisons
print("\nT-test Results for Weekends:")
print("Before vs During:", t_test_and_interpret(before_weekend['avg_available'], during_weekend['avg_available'], 'Before', 'During'))
print("During vs After:", t_test_and_interpret(during_weekend['avg_available'], after_weekend['avg_available'], 'During', 'After'))
print("Before vs After:", t_test_and_interpret(before_weekend['avg_available'], after_weekend['avg_available'], 'Before', 'After'))

# Box Plot for visualization
plt.figure(figsize=(10, 6))
sns.boxplot(x='period', y='avg_available', hue='is_weekend', data=df)
plt.title("Parking Availability Before, During, and After Event")
plt.ylabel("Average Available Spaces")
plt.xlabel("Event Period")
plt.legend(title="Day Type")
plt.show()
