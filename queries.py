import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect('formula12.db')

# Query to calculate average pit stop time for each driver and year
query = '''
    SELECT
        drivers.driverId,
        AVG(pit_stops.milliseconds) AS avg_pit_stop_time
    FROM
        pit_stops
    JOIN
        races ON pit_stops.raceId = races.raceId
    JOIN
        drivers ON pit_stops.driverId = drivers.driverId
    GROUP BY
        drivers.driverId
'''

# Execute the query and fetch the results into a DataFrame
df_avg_pit_stop = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Display the result
print(df_avg_pit_stop)
