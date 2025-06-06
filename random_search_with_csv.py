### Scripts that writes a table to csv file, it does the random search on the 'mode' best threshold in provided ranges (hardcoded here),
### Saves the obtained best values for the column inferred_modes_sped_const to the csv
### Updates the table values in postgres from the csv
### Necessary: a database named 'berlin' and user with access to it

import psycopg2
import numpy as np
import sys
import time
import pandas as pd
import os


error_threshold = .1
max_time = 20

if len(sys.argv) < 4:
    print('Necessary arguments: db username, db password, db tablename')
    print('Optional arguments: maximum error threshold (ex: .1), maximum time to wait in seconds (ex: 120)')
    sys.exit(1)
else:
    db_user = sys.argv[1]
    db_pass = sys.argv[2]
    tablename = sys.argv[3]
    if sys.argv[4] is not None:
        error_threshold = float(sys.argv[4])
    if sys.argv[5] is not None:
        max_time = int(sys.argv[5])

conn = psycopg2.connect(
    dbname='berlin',
    user=db_user,
    password=db_pass,
    host='localhost',
    port='5432'
)
cur = conn.cursor()

csv_filename = f'{tablename}.csv'
csv_results_filename = f'{tablename}_results.csv'

thresholds = {
    'still-walk': np.arange(0, 5, .01),
    'walk-bike': np.arange(5, 10, .01),
    'bike-car': np.arange(10, 20, .01),
}

def save_table_to_csv(tablename, csv_filename):
    query = f'SELECT id, speed_km_h, mode, inferred_mode_speed_const FROM {tablename} WHERE mode IS NOT NULL;'
    cur.execute(query)
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=columns)

    df.to_csv(csv_filename, index=False)
    print(f"Table '{tablename}' saved to {csv_filename}")

def update_postgres_from_csv(csv_filename, tablename, conn):
    df = pd.read_csv(csv_filename)

    for index, row in df.iterrows():
        query = f'''
            UPDATE {tablename}
            SET inferred_mode_speed_const = %s
            WHERE id = %s;
        '''
        cur.execute(query, (row['inferred_mode_speed_const'], row['id']))

    conn.commit()
    print(f"CSV file '{csv_filename}' updated PostgreSQL table '{tablename}'.")

def update_thresholds(df, still_walk, walk_bike, bike_car):
    df['inferred_mode_speed_const'] = np.select(
        [
            df['speed_km_h'] <= still_walk,
            (df['speed_km_h'] > still_walk) & (df['speed_km_h'] <= walk_bike),
            (df['speed_km_h'] > walk_bike) & (df['speed_km_h'] <= bike_car),
            df['speed_km_h'] > bike_car,
        ],
        ['still', 'walk', 'bike', 'car'],
        default=None
    )
    return df

def get_error_rate(df):
    errors = (df['inferred_mode_speed_const'] != df['mode']).sum()
    total = len(df)
    error_rate = errors / total
    return error_rate

def random_parameters_search():
    min_error = float('inf')
    best_combination = None
    start = time.time()

    df = pd.read_csv(csv_filename)

    try:
        while True:
            if (time.time() - start) > max_time:
                break

            t1 = np.random.choice(thresholds['still-walk'])
            t2 = np.random.choice(thresholds['walk-bike'])
            t3 = np.random.choice(thresholds['bike-car'])
            df_updated = update_thresholds(df, t1, t2, t3)

            curr_error = get_error_rate(df_updated)

            if curr_error < min_error:
                min_error = curr_error
                print(f'--- found new best: {t1:.2f}, {t2:.2f}, {t3:.2f}, error:{min_error:.4f}')
                best_combination = (t1, t2, t3)
                df_updated.to_csv(f'{csv_results_filename}', index=False)
                if min_error < error_threshold:
                    print('Reached error threshold!')
                    return best_combination, min_error

        print(f'Reached max time: {max_time} seconds')

    except KeyboardInterrupt:
        print('Stopping the search')

    return best_combination, min_error

def main():
    save_table_to_csv(tablename, csv_filename)

    best_thresholds, best_error = random_parameters_search()
    print()
    print(f'Best thresholds: {best_thresholds} with error rate: {best_error}')
    update_postgres_from_csv(csv_results_filename, tablename, conn)

    os.remove(csv_filename)

    cur.close()
    conn.close()

main()
