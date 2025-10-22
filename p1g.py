import os
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

# The column names in the gas data can be different across files (similar to the electricity data). 
# To deal with it, an alias map is defined, that maps the expected column names across files to one name. 
ALIAS_MAP = {
    'time': 'time',
    'totalgasused': 'usage',  # This means 'totalgasused' and 'usage' are considered equivalent column names
    'gasusage': 'usage',
    't1gas': 'usage',
    'gas': 'usage',
    'usage': 'usage'
}

"""
In the command line interface, supply the address of the database
and the paths (or a single path) of the gas data to be inserted in the gas table of the database.
"""
@click.command()
@click.argument('files', nargs=-1, type=click.Path(exists=True))  # Accepting multiple files
@click.option('-d', required=True, help='SQLAlchemy database URL, e.g. sqlite:///your_database.db')

def p1g(files, d):
    """
    The function aggregates and cleans the gas usage data, and 
    calls a method of the HomeMessagesDB class to handle insertion into the database.
    """
    frames = []  # Collected DataFrames

    for file in files:
        try:
            # Reading .csv or .csv.gz
            opener = gzip.open if file.endswith('.gz') else open
            with opener(file, 'rt', encoding='utf-8', errors='replace') as f:
                df = pd.read_csv(f)

            # Normalizing column names
            norm_cols = {normalize(col): col for col in df.columns}
            reverse_map = {}

            # Map the column names to the aliases defined above
            for alias, target in ALIAS_MAP.items():
                if alias in norm_cols:
                    reverse_map[target] = norm_cols[alias]

            if not {'time', 'usage'}.issubset(reverse_map):
                click.echo(f"Skipping file: {file} — Missing required columns.")
                continue

            # Selecting and renaming
            df = df[[reverse_map['time'], reverse_map['usage']]].copy()
            df.columns = ['time', 'usage']

            frames.append(df)

        except Exception as e:
            click.echo(f"Error reading file: {file} — {e}")

    if not frames:
        click.echo("No valid gas data loaded.")
        return

    # Concatenating data
    data = pd.concat(frames)
    data['time'] = pd.to_datetime(data['time'], errors='coerce')

    # Converting to epoch (UTC)
    data['epoch'] = data['time'].dt.tz_localize('Europe/Amsterdam',ambiguous='NaT')
    # Drop invalid timestamps
    data = data.dropna(subset=['epoch'])  
    data['epoch'] = data['epoch'].dt.tz_convert('UTC')
    # Convert to Unix Time
    data['epoch'] = data['epoch'].astype('int64') // 1_000_000_000

    # Remove the time column
    data = data.drop('time', axis=1) 

    # Deduplicating
    data = data.drop_duplicates(subset=['epoch'], keep='last')
    data = data.sort_values('epoch').reset_index(drop=True)

    # We drop all nan values, since we need all values to be present for analysis
    data = data.dropna()

    # Create an object of the main database handling class, and call a method to insert the cleaned data
    # into the 'gas' table:
    db_instance = HomeMessagesDB(d)
    db_instance.insert_p1g_data(data, 'gas')

# For calling from the command line interface
if __name__ == '__main__':
    p1g()
