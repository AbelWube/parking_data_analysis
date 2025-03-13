import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import f_oneway
from datetime import datetime
import numpy as np  

# Database connection 
hostname = 'localhost'
database = 'parkingOccup_2025jan22'
username = 'postgres'
pwd = 'Zenani@Abel19'
port_id = '5432'

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host=hostname,
    dbname=database,
    user=username,
    password=pwd,
    port=port_id
)
cur = conn.cursor()

# User inputs
parking_id = input("Enter the parking ID: ")
day_of_week = input("Enter the day of the week (e.g., Monday): ")
time1 = input("Enter the first time (HH:MM): ")
time2 = input("Enter the second time (HH:MM): ")

# Convert times to 24-hour format 
time1 = datetime.strptime(time1, "%H:%M").strftime("%H:%M")
time2 = datetime.strptime(time2, "%H:%M").strftime("%H:%M")

# Query to fetch relevant data, ensuring capacity > 0
query = """
SELECT datetime, available 
FROM occupancy
WHERE id = %s AND available > -1 AND capacity > 0
"""
cur.execute(query, (parking_id,))
data = cur.fetchall()
cur.close()
conn.close()

# Convert to DataFrame
df = pd.DataFrame(data, columns=["Datetime", "Available"])
df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
df["Day"] = df["Datetime"].dt.day_name()
df["Time"] = df["Datetime"].dt.strftime('%H:%M')

# Filter by chosen day of the week
df = df[df["Day"] == day_of_week].sort_values(by="Datetime")

# Extract unique dates where each time appears
dates_time1 = df[df["Time"] == time1]["Datetime"].dt.date.unique()
dates_time2 = df[df["Time"] == time2]["Datetime"].dt.date.unique()

# Identify missing dates
dates_with_both_times = np.intersect1d(dates_time1, dates_time2)
missing_dates = set(dates_time1) ^ set(dates_time2)  # Dates in one list but not the other

# Display available and missing dates
print("\nAvailable Dates (Before Alignment):")
print(pd.DataFrame({time1: pd.Series(dates_time1), time2: pd.Series(dates_time2)}))

if missing_dates:
    print(f"\nWarning: The following dates have data for only one time: {missing_dates}")

# Filter the dataframe to only keep common dates
df = df[df["Datetime"].dt.date.isin(dates_with_both_times)]

# Create two groups based on selected times
group1 = df[df["Time"] == time1][["Datetime", "Available"]]
group2 = df[df["Time"] == time2][["Datetime", "Available"]]

# Align datasets by date to handle missing values
group1.set_index(group1["Datetime"].dt.date, inplace=True)
group2.set_index(group2["Datetime"].dt.date, inplace=True)
merged_df = pd.concat([group1["Available"], group2["Available"]], axis=1, keys=[time1, time2])

# Display aligned dataset
print("\nDataset with corresponding dates and availability for both times (after alignment):")
print(merged_df)

# Remove rows with NaN values (i.e., days where one time was missing)
merged_df.dropna(inplace=True)

# Perform ANOVA only if enough valid data exists
group1_values = merged_df[time1].values
group2_values = merged_df[time2].values

if len(group1_values) > 1 and len(group2_values) > 1:
    f_stat, p_value = f_oneway(group1_values, group2_values)
    print(f"\nANOVA Results - F-statistic: {f_stat:.4f}, P-value: {p_value:.4f}")

    if p_value < 0.05:
        print(f"Significant difference in parking availability between {time1} and {time2} on {day_of_week}.")
    else:
        print(f"No significant difference in parking availability between {time1} and {time2} on {day_of_week}.")

    # Visualization
    df_filtered = df[df["Time"].isin([time1, time2])]

    # Scatter Plot
    plt.figure(figsize=(12, 6))
    sns.scatterplot(x=df_filtered["Datetime"], y=df_filtered["Available"], hue=df_filtered["Time"], palette=["blue", "red"], alpha=0.7)
    plt.title(f"Scatter Plot of Parking Availability at {time1} and {time2} on {day_of_week}")
    plt.xlabel("Date")
    plt.ylabel("Available Parking Spaces")
    plt.legend(title="Time")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.show()

    # Box Plot
    plt.figure(figsize=(8, 6))
    sns.boxplot(x=df_filtered["Time"], y=df_filtered["Available"], palette="Set2")
    plt.title(f"Boxplot Comparison Between {time1} and {time2} on {day_of_week}")
    plt.xlabel("Time")
    plt.ylabel("Available Parking Spaces")
    plt.show()

else:
    print("Not enough data to perform ANOVA.")


