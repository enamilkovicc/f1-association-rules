import sqlite3
import pandas as pd

csv_files = [
    'circuits.csv', 'constructor_results.csv', 'constructor_standings.csv',
    'constructors.csv', 'driver_standings.csv', 'drivers.csv', 'lap_times.csv',
    'pit_stops.csv', 'qualifying.csv', 'races.csv', 'results.csv', 'seasons.csv',
    'sprint_results.csv', 'status.csv'
]

relationships = {
    'circuits' : {'pk':'circuitId'},
    'constructor_results': {'pk': 'constructorResultsId', 'fk': {'constructorId': 'constructors(constructorId)', 'raceId': 'races(raceId)'}},
    'constructor_standings': {'pk': 'constructorStandingsId', 'fk': {'constructorId': 'constructors(constructorId)', 'raceId': 'races(raceId)'}},
    'constructors': {'pk': 'constructorId'},
    'driver_standings': {'pk': 'driverStandingsid', 'fk': {'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)'}},
    'drivers' : {'pk':'driverId'},    
    'lap_times': {'fk': {'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)'}},
    'pit_stops': {'fk': {'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)'}},
    'qualifying': {'pk': 'qualifyId', 'fk': {'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)', 'constructorId': 'constructors(constructorId)'}},
    'races' : {'pk':'raceId', 'fk':{'circuitId':'circuits(circuits)'}},
    'results': {'pk': 'resultId', 'fk': {'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)', 'constructorId': 'constructors(constructorId)'}},
    'sprint_results':{'fk':{'resultId':'results(resultId)', 'raceId': 'races(raceId)', 'driverId': 'drivers(driverId)', 'constructorId': 'constructors(constructorId)', 'statusId':'status(statusId)'}},
    'status' : {'pk':'statusId'}
}

conn = sqlite3.connect('formula1-originalDB.db')

for file in csv_files:
    df = pd.read_csv(f'data/{file}')
    
    table_name = file.split('.')[0]
    
    if table_name in relationships:
        df.to_sql(table_name, conn, index=False, if_exists='replace')
        
        if 'pk' in relationships[table_name]:
            conn.execute(f'CREATE UNIQUE INDEX idx_{table_name}_pk ON {table_name} ({relationships[table_name]["pk"]});')
            
        if 'fk' in relationships[table_name]:
            for fk, reference in relationships[table_name]['fk'].items():
                conn.execute(f'CREATE INDEX idx_{table_name}_{fk} ON {table_name} ({fk});')
                conn.execute(f'CREATE TRIGGER fk_{table_name}_{fk} '
                             f'BEFORE INSERT ON {table_name} '
                             f'FOR EACH ROW BEGIN '
                             f'SELECT CASE WHEN ((SELECT {reference} FROM {table_name} WHERE {reference} = NEW.{fk}) IS NULL) '
                             f'THEN RAISE(ABORT, "Foreign key constraint failed") END; '
                             f'END;')
    else:
        df.to_sql(table_name, conn, index=False, if_exists='replace')

conn.commit()
conn.close()

print("SQLite database created and data imported successfully.")
