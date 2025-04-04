import psycopg2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import f_oneway, ttest_1samp
from datetime import datetime
import numpy as np  

# Ensure required library is installed
try:
    import xlsxwriter
except ImportError:
    import subprocess
    subprocess.run(["pip", "install", "xlsxwriter"])
    import xlsxwriter

# Database connection 
hostname = 'localhost'
database = 'parkingOccupHasselt_2025mar13'
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
missing_dates = set(dates_time1) ^ set(dates_time2)

# Filter the dataframe to only keep common dates
df = df[df["Datetime"].dt.date.isin(dates_with_both_times)]

# Create two groups based on selected times
group1 = df[df["Time"] == time1][["Datetime", "Available"]]
group2 = df[df["Time"] == time2][["Datetime", "Available"]]

# Align datasets by date
group1.set_index(group1["Datetime"].dt.date, inplace=True)
group2.set_index(group2["Datetime"].dt.date, inplace=True)
merged_df = pd.concat([group1["Available"], group2["Available"]], axis=1, keys=[time1, time2])

# Compute availability differences
merged_df["Difference"] = merged_df[time2] - merged_df[time1]

# Perform t-test to check if mean difference is significantly different from zero
if len(merged_df["Difference"].dropna()) > 1:
    t_stat, p_value = ttest_1samp(merged_df["Difference"].dropna(), 0)
    print(f"\nT-test Results - T-statistic: {t_stat:.4f}, P-value: {p_value:.4f}")
    
    if p_value < 0.05:
        print("Significant difference in availability changes between the two times.")
    else:
        print("No significant difference in availability changes between the two times.")
else:
    print("Not enough data to perform T-test.")

# Visualization
plt.figure(figsize=(8, 6))
sns.histplot(merged_df["Difference"].dropna(), kde=True, bins=15, color="purple")
plt.title("Distribution of Availability Differences")
plt.xlabel("Availability Difference (t2 - t1)")
plt.ylabel("Frequency")
plt.grid(True, linestyle="--", alpha=0.5)
plt.show()

# Export to Excel
excel_filename = "C:\\TS\\2\\Research portfolio\\after_comment\\final\\Parking_Availability.xlsx"
with pd.ExcelWriter(excel_filename, engine="xlsxwriter") as writer:
    merged_df.to_excel(writer, sheet_name="Parking Data")
    workbook = writer.book
    worksheet = writer.sheets["Parking Data"]
    worksheet.set_column("A:A", 12)
    worksheet.set_column("B:D", 18)
    header_format = workbook.add_format({"bold": True, "align": "center", "bg_color": "#D3D3D3", "border": 1})
    for col_num, value in enumerate(merged_df.columns.insert(0, "Date")):
        worksheet.write(0, col_num, value, header_format)
print(f"\nDataset exported successfully to {excel_filename}")
