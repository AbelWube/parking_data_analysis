import psycopg2
import pandas as pd
import scipy.stats as stats
import seaborn as sns
import matplotlib.pyplot as plt


parking_id = int(input("Enter the parking ID: "))  
policy_enforcement_date = input("Enter the parking policy enforcement date (YYYY-MM-DD): ")  


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
            AVG(available) AS avg_available,
            EXTRACT(DOW FROM DATE(datetime)) AS day_of_week  -- Get day of week (0=Sunday, 6=Saturday)
        FROM occupancy
        WHERE id = {parking_id}
        AND available > -1
        AND capacity > 0
        GROUP BY id, DATE(datetime), EXTRACT(DOW FROM DATE(datetime))
    )

    SELECT 
        date,
        avg_available,
        CASE 
            WHEN date < DATE '{policy_enforcement_date}' THEN 'Before Policy'
            WHEN date >= DATE '{policy_enforcement_date}' THEN 'After Policy'
        END AS period,
        CASE 
            WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS week_type
    FROM parking_data
    WHERE date BETWEEN DATE '{policy_enforcement_date}' - INTERVAL '365 days' AND (SELECT MAX(date) FROM parking_data)
    ORDER BY date;
"""


df = pd.read_sql(query, conn)


conn.close()

if df.empty:
    print("No data found for the given inputs.")
    exit()


print("\nComplete Dataset for Each Day:")
print(df)


before_weekday = df[(df['period'] == 'Before Policy') & (df['week_type'] == 'Weekday')]
before_weekend = df[(df['period'] == 'Before Policy') & (df['week_type'] == 'Weekend')]
after_weekday = df[(df['period'] == 'After Policy') & (df['week_type'] == 'Weekday')]
after_weekend = df[(df['period'] == 'After Policy') & (df['week_type'] == 'Weekend')]


print("\nBefore Policy - Weekday Dataset:")
print(before_weekday)

print("\nBefore Policy - Weekend Dataset:")
print(before_weekend)

print("\nAfter Policy - Weekday Dataset:")
print(after_weekday)

print("\nAfter Policy - Weekend Dataset:")
print(after_weekend)


if len(before_weekday) < 2 or len(after_weekday) < 2:
    print("\nInsufficient data for ANOVA comparison on weekdays.")
    exit()

if len(before_weekend) < 2 or len(after_weekend) < 2:
    print("\nInsufficient data for ANOVA comparison on weekends.")
    exit()


min_sample_size_weekday = min(len(before_weekday), len(after_weekday))
min_sample_size_weekend = min(len(before_weekend), len(after_weekend))


before_weekday_sampled = before_weekday.sample(n=min_sample_size_weekday, random_state=42)
after_weekday_sampled = after_weekday.sample(n=min_sample_size_weekday, random_state=42)
before_weekend_sampled = before_weekend.sample(n=min_sample_size_weekend, random_state=42)
after_weekend_sampled = after_weekend.sample(n=min_sample_size_weekend, random_state=42)



f_statistic_weekday, p_value_weekday = stats.f_oneway(before_weekday_sampled['avg_available'], after_weekday_sampled['avg_available'])


f_statistic_weekend, p_value_weekend = stats.f_oneway(before_weekend_sampled['avg_available'], after_weekend_sampled['avg_available'])


print(f"\nANOVA Results for Weekdays: F-Statistic = {f_statistic_weekday}, P-Value = {p_value_weekday}")
print(f"ANOVA Results for Weekends: F-Statistic = {f_statistic_weekend}, P-Value = {p_value_weekend}")

# print(before_weekday_sampled)
# print(after_weekday_sampled)

if p_value_weekday < 0.05:
    print("\nWeekdays: Statistically significant difference detected.")
else:
    print("\nWeekdays: No significant difference detected.")

if p_value_weekend < 0.05:
    print("\nWeekends: Statistically significant difference detected.")
else:
    print("\nWeekends: No significant difference detected.")


plt.figure(figsize=(8, 6))
sns.boxplot(x='period', y='avg_available', data=df, hue='week_type', order=['Before Policy', 'After Policy'])
plt.title('Parking Availability Distribution Before and After the Policy (Weekday vs Weekend)')
plt.xlabel('Parking Policy Period')
plt.ylabel('Average Parking Availability')
plt.show()

