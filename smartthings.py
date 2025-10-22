import pandas as pd
import click 
import home_messages_db 
import numpy as np

"""
In the command line interface, supply the address of the database
and the paths (or a single path) of the smartthings data to be inserted in the smarthtings table of the database.
"""
@click.command()
@click.argument('files', nargs=-1)  
@click.option('-d', required=True, help='SQLAlchemy database URL, e.g. sqlite:///your_database.db') 


def smartthings(files,d): 

    """
    The function reads the data from the smartthings source, performs cleaning, and passes the cleaned dataframe to a 
    method of the HomeMessagesDB class for insertion.  
    """

    # If the user didn't supply one of the arguments:
    if (len(files) or len(d))==0:
        click.UsageError("Plase provide the database address and the file path(s)")

    # Read compressed data from all files in the directory (or from one file) into a dataframe
    if len(files)==1:
        smartthings=pd.read_csv(files[0], sep = '\t', compression = 'gzip') # Compressed files will be automatically opened
    else: 
        # Filter out files with the right extension (to remove unnecessary system files),
        # and concatenate all records in one table
        files=[i for i in files if i.endswith('.gz')]
        smartthings = pd.concat((pd.read_csv(file, sep = '\t', compression = 'gzip') for file in files),
                                 ignore_index=True) 

    ####### Cleaning  #######

    # Remove duplicates 
    smartthings=smartthings.drop_duplicates()
    # Remove rows with missing values in the epoch column
    smartthings = smartthings.dropna(subset=['epoch'])
    # Convert to datetime first, to make it possible to convert to UTC
    smartthings['epoch'] = pd.to_datetime(smartthings['epoch'], errors='coerce')

    # Take steps to make it possible to remove the implausible temperature values 
    smarthtings_temp_check=smartthings.loc[smartthings['attribute']=='temperature'].copy()
    smarthtings_temp_check["value"] = pd.to_numeric(smarthtings_temp_check["value"], errors="coerce")
    smarthtings_temp_check["value"] = smarthtings_temp_check["value"].astype(int)

    general_susp_temp = smarthtings_temp_check[
        (smarthtings_temp_check["attribute"] == "temperature") &
        (smarthtings_temp_check["value"] > 50) &
        (smarthtings_temp_check["value"] < 20)
    ]
    # Check suspect low temperatures in months 7–10 (below 2°C)
    low_temp_summer = smarthtings_temp_check[
        (smarthtings_temp_check["attribute"] == "temperature") &
        (smarthtings_temp_check["value"] < 2) &
        (smarthtings_temp_check["epoch"].dt.month.isin([6, 7, 8, 9]))
    ]

    # Check suspect high temperatures in cold months (bigger than 25)
    high_temp_winter = smarthtings_temp_check[
        (smarthtings_temp_check["attribute"] == "temperature") &
        (smarthtings_temp_check["value"] > 25) &  
        (smarthtings_temp_check["epoch"].dt.month.isin([11, 12, 1, 2]))
    ]
    num_invalid_temp_records=len(general_susp_temp)+len(high_temp_winter)+len(low_temp_summer)

    # Drop the invalid observations
    smartthings = smartthings.drop(general_susp_temp.index)
    smartthings = smartthings.drop(high_temp_winter.index)
    smartthings = smartthings.drop(low_temp_summer.index)

    print(f'Dropped {num_invalid_temp_records} records with invalid temperature values')

    # Take steps to make it possible to remove the implausible humidity values 
    smarthtings_hum_check=smartthings.loc[smartthings['attribute']=='humidity'].copy()
    smarthtings_hum_check["value"] = pd.to_numeric(smarthtings_hum_check["value"], errors="coerce")
    smarthtings_hum_check["value"] = smarthtings_hum_check["value"].astype(int)

    general_susp_humid = smarthtings_hum_check[
        (smarthtings_hum_check["value"] > 100) &
        (smarthtings_hum_check["value"] < 0)
    ]
    num_invalid_hum_records=len(general_susp_humid)
    # Drop the invalid humidity observations
    smartthings = smartthings.drop(general_susp_humid.index)
    print(f'Dropped {num_invalid_hum_records} records with invalid humidity values')

    #smartthings["value"] = smartthings["value"].astype(str)

    # Convert to Unix Time  
    smartthings['epoch'] = smartthings['epoch'].dt.tz_convert('UTC')
    smartthings['epoch'] = smartthings['epoch'].astype('int64') // 1_000_000_000

    # In case of smartthings, the epoch can't be used as a unique id, since multiple devices can produce measurements
    # at the same time. Thus, we create an id column, that is a composite of the existing columns:
    loc_part=smartthings['loc'].str[0:2] + '_' # Take the first two letters of the 'location' column; similar for rest. 
    lvl_part=smartthings['level'].str[0:2] + '_'
    name_part=smartthings['name'].str[:]+ '_'
    capability_part=smartthings['capability'].str[0:2]+ '_'
    att_part=smartthings['attribute'].str[0:2]+ '_'
    val_part=smartthings['value'].str[:]+ '_'
    epoch_part=np.char.mod('%d', np.array(smartthings['epoch']))
    smartthings['id']=loc_part+lvl_part+name_part+capability_part+att_part+val_part+epoch_part

    # Initialize an instance of the class handling the insertion
    db = home_messages_db.HomeMessagesDB(d)   
    # Use the method of the class to insert the data into the database 
    db.insert_smartthings(smartthings)

# For running the file through the command line interface
if __name__ == '__main__':
    smartthings()
