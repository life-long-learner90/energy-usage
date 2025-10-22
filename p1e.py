import gzip
import click
import pandas as pd
from pathlib import Path
from home_messages_db import HomeMessagesDB  
import re


def normalize(col):
    """
    Helper function to get rid of unusual characters in the column names,
    and just keep a~z and 0~9  
    """
    return re.sub(r'[^a-z0-9]', '', col.lower())

# The column names in the electricity data can be different across files (similar to the gas data). 
# To deal with it, an alias map is defined, that maps the expected column names across files to one name. 
ALIAS_MAP = {
    'importt1kwh': 'Import T1 kWh',   
    'importt1': 'Import T1 kWh',
    'electricityimportedt1': 'Import T1 kWh',
    't1': 'Import T1 kWh',

    'importt2kwh': 'Import T2 kWh',
    'importt2': 'Import T2 kWh',
    'electricityimportedt2': 'Import T2 kWh',
    't2': 'Import T2 kWh',

    'time': 'time'
}

"""
In the command line interface, supply the address of the database
and the paths (or a single path) of the electricity data to be inserted in the electricity table of the database.
"""

@click.command()
@click.argument('files', nargs=-1, type=click.Path(exists=True))  # Accepting multiple file paths
@click.option('-d', required=True, help='SQLAlchemy database URL, e.g. sqlite:///your_database.db') 

def p1e(files, d):
    """
    The function aggregates and cleans the electricity usage data, and 
    calls a method of the HomeMessagesDB class to handle insertion into the database.
    """

    frames = []  # Collecting cleaned dataframes here

    for file in files:
        try:
            # Opening gzipped or plain text files accordingly
            opener = gzip.open if file.endswith('.gz') else open
            with opener(file, 'rt', encoding='utf-8', errors='replace') as f:
                df = pd.read_csv(f)

            # Detecting and mapping normalized headers to expected ones
            norm_cols = {normalize(col): col for col in df.columns}
            reverse_map = {}
            for alias, target in ALIAS_MAP.items():
                if alias in norm_cols:
                    reverse_map[target] = norm_cols[alias]

            # Ensuring all required columns are present
            if not {'Import T1 kWh', 'Import T2 kWh', 'time'}.issubset(reverse_map):
                click.echo(f"Skipping file: {file} — Missing required columns.")
                continue

            # Selecting required columns and renaming them 
            df = df[[reverse_map['time'], reverse_map['Import T1 kWh'], reverse_map['Import T2 kWh']]].copy()
            df.columns = ['time', 'T1', 'T2']
            frames.append(df)

        except Exception as e:
            click.echo(f"Error reading file: {file} — {e}")

    # If nothing was loaded, we quit
    if not frames:
        click.echo("No valid data loaded.")
        return

    # Merging all dataframes
    data = pd.concat(frames)

    # Converting 'time' column to datetime safely
    data['time'] = pd.to_datetime(data['time'], errors='coerce')
    data = data.dropna(subset=['time'])  # Dropping invalid timestamps

    # Localize and perform the steps to convert the date to Unix time values: 
    data['epoch'] = data['time'].dt.tz_localize('Europe/Amsterdam',ambiguous='NaT')
    # Drop invalid timestamps, that emerged during localization (we remove a lot of rows by doing that, however, 
    # it helps prevent invalid Unix time values)
    data = data.dropna(subset=['epoch']) 
    data['epoch'] = data['epoch'].dt.tz_convert('UTC')
    # Convert to Unix Time
    data['epoch'] = data['epoch'].astype('int64') // 1_000_000_000
    # Remove the time column
    data = data.drop('time', axis=1) 

    # We have to drop duplicates in the epoch column, since they result in integrity errors when inserting into the database.
    # It is safe to do in this case, since we assume that per time stamp, there can only be one consumption record. 
    data = data.drop_duplicates(subset=['epoch'], keep='last') 
    data = data.sort_values('epoch').reset_index(drop=True)

    # Finally, we drop nan values from the electricity consumption columns, since we need complete records for analysis:
    data = data.dropna()

    # Create an object of the main database handling class, and call a method to insert the cleaned data
    # into the 'electricity' table:
    db_instance = HomeMessagesDB(d)
    db_instance.insert_p1e_data(data,'electricity')

# For calling from the command line interface
if __name__ == '__main__':
    p1e()
