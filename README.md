# energy-usage
This repository contains the infrastructure to enable the storage and analysis of the data from smart devices. It is implemented based on the data that was collected over 32 months, by various smart devices, in a house in Nordwijk.
Project files
The repository contains the following files:

home_messages_db.py
The file contains the classes responsible for defining the structure of the database, as well as the HomeMessagesDB class, that is meant to facilitate all interactions with the database. The methods of this class are called in the tools p1e.py, p1g.py and smartthings.py in order to insert the data in the database. The reports make use of the querying methods of the class.

openweathermap.py
The script retrieves the humidity and temperature values for Nordwijk, in the range specified by the data. It is called directly in the report_weather_vs_gas_usage.ipynb.

p1e.py and p1g.py
The two scripts contain functions to read the file(s) from the 'P1e' and 'P1g' sources respectively, and prepare the data for insertion in the corresponding database tables. The scripts include the following cleaning steps:

Map the diverse column names present in the files to one format across all;
Remove duplicates and NaN's;
Convert the date to Unix format.
The respective dataframes are then passed to the insert_p1e_data() or the insert_p1g_data() methods of the database handling class for ingestion.

smartthings.py
The script contains a function to read the file(s) from the 'Smartthings' source and prepare the data for insertion in the database. It involves the following cleaning steps:

Remove duplicates and NaN's;
Convert the date to Unix format;
Remove implausible temperature and humidity values.
Finally, a unique id is created as a combination of column values, and the dataframe is passed to the insert_smartthings() method of the database handling class for ingestion.

report gas_usage.ipynb
The report explores two research questions:

Is the temperature measured by the sensor significantly different from the temperature as reported by open-meteo?

Do temperature and humidity have a significant impact on the gas consumption?

report_gas_elect_patterns.ipynb

The report addresses the research questions:

Can daily electricity usage be predicted from daily gas usage?
What are the gas and electricity usage patterns?
