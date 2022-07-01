#################################################################################
# Post-Merge Script for SQLite-MegaMerge-for-CellProfiler                       #
#                                                                               #
# @author         Mackenzie A Michell-Robinson                                  #
# @description    Sets the schema and constraints of merged databases           # 
#                 use with for CP-Analyst.                                      #
# @date           July 1, 2022                                                  #
#                                                                               #
#################################################################################

#################################################################################
############################## Import Libraries #################################

import sqlite3
import time
import sys
import traceback
from time import gmtime, strftime, sleep

#################################################################################
############################## Define Functions #################################

# 1. Attach a database to the currently connected database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return none

def attach_database(db_name):
    try:
        curs.execute(f"ATTACH DATABASE '{db_name}' as 'donor';")
    except Exception():
        traceback.exc()

# 2. Close the current database connection
#
# @return none

def close_connection():
    curs.close()
    conn.close()

# 3. Get the column information for a table
#
# @param table_name the name of the table (i.e. "example.db")
# unlike get_column_names returns a formatted string
# format: "colname1 coltype1, colname2 coltype2, ...)
# still strips primary ids column

def get_column_types(table_name):
    curs.execute(f"PRAGMA table_info({table_name});")
    temp = curs.fetchall()
    column_types = []
    for i in range(0, len(temp)):
        column_types.append(temp[i][2])
    return column_types


# 4. Convert a list of string objects to a string of comma separated items.
#
# @param listObj the list to convert
# @return a string containing the list items - separated by commas.

def list_to_string(list_obj, dim):
    list_string = ""
    if dim == 2:
        for i in range(0, len(list_obj)):
            if i == (len(list_obj) - 1):
                list_string = list_string + list_obj[i][0] + " " + list_obj[i][1]
            else:
                list_string = list_string + list_obj[i][0] + " " + list_obj[i][1] + ", "
    elif dim == 1:
        for i in range(0, len(list_obj)):
            if i == (len(list_obj) - 1):
                list_string = list_string + list_obj[i]
            else:
                list_string = list_string + list_obj[i] + ", "
    else:
        print("List_to_string is not equipped for list dimensions > 2")
        exit()
    return list_string

# 5. Get the column names of a table
#
# @param table_name the name of the database file (i.e. "example.db")
# @return a string array of the column names - strips primary ids column

def get_column_names(table_name):
    curs.execute(f"PRAGMA table_info({table_name});")
    temp = curs.fetchall()
    columns = []
    for i in range(0, len(temp)):
        columns.append(temp[i][1])
    return columns

# 6. Get the table names of a database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return a string array of the table names

def get_table_names():
    temp = []
    tables = []
    curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
    temp = curs.fetchall()
    for i in range(0, len(temp)):
        if ("sqlite_sequence" in temp[i][0]):
            continue
        else:
            tables.append(temp[i][0])
    return tables

#################################################################################
############################## Input Parameters #################################

# Set the complete path to the database file to be processed.

db = '/path/to/database.db'

#################################################################################
################################## Script #######################################

# 1. Initialize Connection and get list of tables with types/constraints from OriginalDB
##################################
conn = sqlite3.connect(db)  # Connect to an original database version
curs = conn.cursor()  # Connect a cursor
listTable = ['Per_Image', 'Per_Object']

print("Processing Tables... Please wait, this may take some time.")
for g in range(0, len(listTable)):
    print(f"Fetching table information for {db}.")
    coltyp   = get_column_types(listTable[g])
    colnam   = get_column_names(listTable[g])
# Column Constraint Definitions for Specific Tables
    if (listTable[g] == 'Per_Image'):
        print(f"Processing table constraints for {listTable[g]}.")
        temp_idx = colnam.index("ImageNumber")
        coltyp[temp_idx] = 'INTEGER UNIQUE'
        colnamtyp = list(map(list, zip(colnam, coltyp)))
        colnamtyp = list_to_string(colnamtyp, 2)
        colnamtyp = colnamtyp + ', PRIMARY KEY (ImageNumber)'
    if (listTable[g] == 'Per_Object'):
        print(f"Processing table constraints for {listTable[g]}.")
        temp_idx = colnam.index("ImageNumber")
        coltyp[temp_idx] = 'INTEGER'
        temp_idx = colnam.index("ObjectNumber")
        coltyp[temp_idx] = 'INTEGER UNIQUE'
        colnamtyp = list(map(list, zip(colnam, coltyp)))
        colnamtyp = list_to_string(colnamtyp, 2)
        colnamtyp = colnamtyp + ', FOREIGN KEY (ImageNumber) REFERENCES Per_Image (ImageNumber)'
        colnamtyp = colnamtyp + ', PRIMARY KEY (ObjectNumber)'
    colnam = list_to_string(colnam, 1)
# Table Modifications
    print(f"Finalizing alterations to {listTable[g]}...")
    if (listTable[g] == 'Per_Image' or listTable[g] == 'Per_Object'):
        curs.execute(f"CREATE TABLE _{listTable[g]}({colnamtyp});")
        curs.execute(f"INSERT INTO _{listTable[g]}({colnam}) SELECT {colnam} FROM {listTable[g]};")
        curs.execute(f"DROP TABLE {listTable[g]};")
        curs.execute(f"ALTER TABLE _{listTable[g]} RENAME TO {listTable[g]};")
    else:
        try:
            curs.execute(f"DROP TABLE {listTable[g]};")
        except:
            print(f"Can not drop {listTable[g]}. Continuing...")
            curs.execute(f"CREATE TABLE _{listTable[g]}({colnamtyp});")
            curs.execute(f"INSERT INTO _{listTable[g]}({colnam}) SELECT {colnam} FROM {listTable[g]};")
            curs.execute(f"DROP TABLE {listTable[g]};")
            curs.execute(f"ALTER TABLE _{listTable[g]} RENAME TO {listTable[g]};")
    print(f"Processing of {listTable[g]} completed.")
    conn.commit()

try:
    print("Cleaning up the database. Please wait...")
    curs.execute(f"VACUUM;")
except Exception():
    traceback.exc()

close_connection()
print(f"Post-processing of {db} complete.")
