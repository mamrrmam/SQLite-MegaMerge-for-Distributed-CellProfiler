#################################################################################
# SQLite Mega Merge Script                                                      #
#                                                                               #
# @author         Mackenzie A Michell-Robinson                                  #
# @contributor    Charles Duso "SQLite Merge Script"                            #
# @description    Merges databases that have the same tables and schema.        #
#                 Supports merging thousands of databases.                      #
#                 Includes relevant object number and image number handling     #
#                 for use with Distributed CellProfiler output and should       #
#                 itself output a single database that can be used in           #
#                 CellProfiler Analyst.                                         #
# @date           July 1st, 2022                                                #                                               #
#                                                                               #
#################################################################################

#################################################################################
############################## Import Libraries #################################

import sqlite3
import time
import sys
import traceback
import os
import pandas as pd
from time import gmtime, strftime

#################################################################################
############################## Global Variables #################################

dbCount = 0  # Variable to count the number of databases
listDB = []  # Variable to store the names of the databases
listTable = []  # Variable to store table names

#################################################################################
############################## Define Functions #################################

# 1. Attach a database to the currently connected database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return none

def attach_database(db_name, u, n):
    global dbCount
    global listDB
    db_add = f"db_{u}_{n}"
    print(f"Attaching database: '{db_name}' at block: {u}, position: {n} as '{db_add}'.")
    try:
        curs.execute(f"ATTACH DATABASE '{db_name}' as '{db_add}'")
        listDB[u].append(db_add) 
        ##appends the most recently attached database to the current block in listDB
    except Exception():
        traceback.exc()

# 2. Close the current database connection
#
# @return none

def close_connection():
    curs.close()
    conn.close()

# 3. Get the table names of a database
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
        if ("Experiment" in temp[i][0]):
            continue
        if ("Experiment_Properties" in temp[i][0]):
            continue
        if ("Per_Experiment" in temp[i][0]):
            continue
        else:
            tables.append(temp[i][0])
    return tables

# 4. Get the column names of a table
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

# 5. Compare two lists to see if they have identical data
#
# @param list1 the first list parameter for comparison
# @param list2 the second list parameter for comparison
# @return will return a boolean (0 lists !=, 1 lists ==)

def compare_lists(list1, list2):
    if len(list1) != len(list2):
        return 0
    else:
        for i in range(0, len(list1)):
            if list1[i] != list2[i]:
                return 0
    return 1

# 6. Convert a list of string objects to a string of comma separated items.
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


# 7. Merge a table from an attached database to the source table
#
# @param table_name the name of the table to merge
# @param column_names the names of the columns to include in the merge
# @param db_name_table_name the name of the attached database and the table i.e. "db_name.table_name"
# @return none

def merge_table(table_name, column_names, db_name):
    db_name_table_name = db_name + "." + table_name
    try:
        curs.execute(f"INSERT INTO {table_name}({column_names}) SELECT {column_names} FROM {db_name_table_name};")
        conn.commit()
    except Exception:
        traceback.print_exc()


# 8. Divide otherDBs into blocks of ten or less because sqlite can't attach more than ten at a time
#
# @param list (ie. otherDBs)
# @param n (the size of the block, in our case 10)

def divide_list(list, n):
    for i in range(0, len(list), n):
        yield list[i:i +n]

# 9. Get the column types of a table
#
# @param table_name the name of the table (i.e. "example.db") 
# format: [colname1 coltype1, colname2 coltype2, ...]
# still strips primary ids column

def get_column_names_types(table_name):
    curs.execute(f"PRAGMA table_info({table_name});")
    temp = curs.fetchall()
    column_names_types = []
    for i in range(0, len(temp)):
        column_names_types.append([temp[i][1], temp[i][2]])
    return column_names_types

# 10. Iteratively search thru a nested list to replace a value, for lists with up to three levels 
#
# @param list is the list to search
# @param search_key is the value to replace in the list
# @param replacement is the replacement value
# function returns a replacement list and the primary index for the replaced value
# as a string [list, "idx"] so that you can "pop" the whole entry from the list
# by .pop(var[1])

def list_find_replace(list, search_key, replacement):
    for i in range(0, len(list)):
        if list[i] == search_key:
            list[i] = replacement
            idx = i
            success = True
            package = [list, idx]
            break
        else:
            continue
    for i in range(0, len(list)):
        for k in range(0, len(list[i])):
            if list[i][k] == search_key:
                list[i][k] = replacement
                idx = i
                success = True
                package = [list, idx]
                break
            else:
                continue
    for i in range(0, len(list)):
        for k in range(0, len(list[i])):
            for y in range(0, len(list[i][k])):
                if list[i][k][y] == search_key:
                    list[i][k][y] = replacement
                    idx = i
                    success = True
                    package = [list, idx]
                    break
                else:
                    continue
    if success == True:
        print('The requested replacement has been made')
        return package
    else:
        print('No replacement could be made with this combination of list and key.')
        return list


# 11. Return a list of tuples (output from fetch methods when using SELECT statements in curs.execute
#         and fetchall, fetchmany, etc.
#         @fetchall is the named variable that the fetchall() is stored in
#         @col_index is the column index in the database - because fetchall() and fetchmany() return
#         lists of tuples, each primary list index represents a tuple (containing a row of values going
#         across the columns - ie. @row_index), whereas the secondary index (index of a specific tuple
#         value) represents the column. Therefore col_index is the secondary index of the fetch output.
#         @col_index has a default value of zero, it will return the first column automatically unless
#         it is given a column index.

def fetch_to_list(fetch, col_index=0):
    newlist = []
    for row_index in range(0, len(fetch)):
        temp = int(fetch[row_index][col_index])
        newlist.append(temp)
    return newlist

# 12. Rename a column in a table

def rename_column(table, old_name, new_name):
    curs.execute(f"ALTER TABLE {table} RENAME COLUMN {old_name} TO {new_name};")

# 13. Swap the names of two columns in the same table

def swap_column_names(table, col1, col2):
    col1_ = col1 + "_"
    col2_ = col2 + "_"
    rename_column(table, col1, col1_)
    rename_column(table, col2, col2_)
    rename_column(table, col1_, col2)
    rename_column(table, col2_, col1)


#################################################################################
############################## Input Parameters #################################

# 1. DEFINE A LIST OF DBs TO MERGE
##################################
# define a list of DBs using filenames from the folder containing databases by running ls /home/ubuntu/databases/*.db > filenames.txt
# ...needs to be full path for sqlite3 to interpret correctly
# otherDBs can also be defined by a list e.g. otherDBs = [] for example,
# otherDBs = ['/home/ubuntu/databases/5-9856-4928.db', '/home/ubuntu/databases/5-9856-5376.db']

with open('filenames.txt', 'r') as fileNames:
    otherDBs = [line.strip() for line in fileNames]
#otherDBs = ['1-0-0.db', '1-9408-3584.db']

# 2. DEFINE A MAIN DB AS A TEMPLATE FOR THE MERGE
###################################
# This is where the main database is ie. the first if a list of identical DBs but can be defined otherwise

mainDB = otherDBs[0]

# 3. QUALITY CONTROL
###################################

if len(otherDBs) == 0:
    print("ERROR: No databases have been added for merging.")
    sys.exit()

#4. WHAT KIND OF DATABASE WAS OUTPUT BY CELLPROFILER
###################################
# 'SingleObjectTable' or 'SingleObjectView' are currently supported

db_type = "SingleObjectTable"

#5. WHAT ARE THE NAMES OF THE OBJECTS YOU MEASURED
###################################
# ie. what are the names of the objects that will be in the columns/table names of your database

object1 = 'FilterNuclei1' # primary nuclear object
object2 = 'IdentifyNG2'   # secondary object based on nucleus
object3 = 'IdentifyYFP'   # secondary object based on nucleus

#################################################################################
############################# Quality Control ###################################

# 1. Initialize Connection and get main list of tables
################################## 

conn = sqlite3.connect(mainDB)  # Connect to the main database
curs = conn.cursor()  # Connect a cursor
listTable = get_table_names()  # Get the table names
listTable.sort()
close_connection()

# 2. Compare databases for quality control
##################################

startTime = time.time()
print("Comparing databases. Started at: " + strftime("%H:%M", gmtime()))
exc_DBs = [] # create a array of DBs with tables that do not match mainDB

i=0 #iterator
while i < len(otherDBs):
    conn = sqlite3.connect(otherDBs[i])
    curs = conn.cursor()
    temp = get_table_names()  # Get the current list of tables
    temp.sort()
    if len(listTable) > len(temp):
        exc_DBs.append([otherDBs[i], "Reason: Missing Table(s), database excluded from merge."])
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue
    if len(listTable) < len(temp):
        exc_DBs.append([otherDBs[i], "Reason: Extra Table(s) can not be merged, database included in merge."])
        continue
    if listTable != temp:
        exc_DBs.append([otherDBs[i], "Reason: Table(s) did not match, database excluded from merge."])
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue
    if listTable == temp:
        i += 1
        continue
    close_connection()
    num = len(otherDBs)
    print(f"There are {num} databases whose tables matched the main database.")

# 3. Log exceptions that were removed or otherwise not a good fit for merge
################################## 

for y in range(0, len(exc_DBs)):
   print(f"{exc_DBs[y][0]} was logged as an exception. {exc_DBs[y][1]}")

# 4. Log errors when no otherDBs were found
################################## 

if len(otherDBs) == 0:
    print("ERROR: No databases to merge. Databases were either removed due to \
          inconsistencies, or databases were not added properly.")
    sys.exit()

# 5. Print notification that quality control is complete
##################################

print("Finished comparing databases. Time elapsed: %.3f" % (time.time() -
                                                            startTime))

#################################################################################
############################ Merging Databases ##################################

# 1. Initialization Parameters
##################################

startTime = time.time()
print("Initializing merging of databases. Started at: " + strftime("%H:%M", gmtime()))

# 2. Pre-processing module produces a donor table based on the template of the original
##################################
## (1): A loop that deals with SingleObjectView CP Output
## (2): A loop that checks the number of objects for an image to determine if there
#            are >10000 objects per image. Another loop that adds GroupNumber to the table 
#            to allow image sub-grouping for tables with more than 1000 objects per image
## (3): A loop that renumbers ObjectNumbers Per_Object Tables
## (4): A loop that removes column constraints from tables to facilitate the merge

## time check
print("Pre-processing databases initiated at: " + strftime("%H:%M", gmtime()))

# (1) Per_Object Table Creation Loop
#### Scheme - make per object tables in each of the otherDBs using left join
#### then insert/merge all DBs

# Define objects
objects = (object1, object2, object3)

if (db_type == 'SingleObjectView'):
    for h in range(0, len(otherDBs)):                                                                                                               # databases loop (each database from one image)
        conn = sqlite3.connect(otherDBs[h], timeout = 10)
        curs = conn.cursor()
        lngt = len(otherDBs)
        now = h + 1
        print(f"Processing SingleObjectView Tables. Now processing {now} of {lngt}")
        curs.execute("PRAGMA legacy_alter_table = TRUE;")
    ### part 1 - rename ImangeNumber in the original per_object(n) tables
        for f in range(0, len(objects)):
            # get column names/types for object(n) table and rename ImageNumber column
            ob = objects[f]
            curs.execute(f"ALTER TABLE Per_{ob} RENAME COLUMN ImageNumber TO {ob}_ImageNumber")
            # add img_no and obj_no cols to be renumbered later (CP requires UNIQUE img and obj number columns in the final aggregated tables)
            curs.execute(f"ALTER TABLE Per_{ob} ADD {ob}_img_no integer;")
            curs.execute(f"ALTER TABLE Per_{ob} ADD {ob}_obj_no integer;")
            curs.execute(f"UPDATE Per_{ob} SET {ob}_obj_no = {ob}_Number_Object_Number")
            conn.commit()
    ### Add corresponding img_no column in Per_Image
        curs.execute("ALTER TABLE Per_Image ADD img_no integer;")
    ### part 2 - make the per-object table
        # Drop per_object views if exists
        curs.execute("SELECT name FROM sqlite_master WHERE type = 'view';")
        views = curs.fetchall()
        for x in range(0, len(views)):
            curs.execute(f"DROP VIEW IF EXISTS {views[x][0]}")
        conn.commit()
        # produce list of column names and types from each per_object(n) table to make the Per_Object table (and adds objectnumber column)
        colnamtyps = []
        for f in range(0, len(objects)):
            ob = objects[f]
            c = get_column_names_types(f"Per_{ob}")
            colnamtyps = colnamtyps + c
        colnamtyps.insert(1, ['obj_no', "INTEGER"]) #insert ObjectNumber column at 2nd positon
        colnamtyps_PO = list_to_string(colnamtyps, 2)
        # Creates Per_Object Table
        curs.execute(f"CREATE TABLE Per_Object({colnamtyps_PO})")
        conn.commit()
    ### Part 3 - put data in the Per_Object Table
        # get column names for Per_Obj statement
        colnams_po = list_to_string(get_column_names('Per_Object'), 1)
        # get column names for select statment
        colnams_sel = []
        for e in range(0, len(objects)):
            ob = objects[e]
            c = get_column_names(f"Per_{ob}")
            colnams_sel = colnams_sel + c
        colnams_sel.insert(1, f"{object1}_Number_Object_Number") #to insert data from primaryobj_Number_Object_Number into the column ObjectNumber
        colnams_sel = list_to_string(colnams_sel, 1)
        # insert data (comment in/out left or inner join)
        #LeftJoin = f"INSERT INTO Per_Object({colnams_po}) SELECT {colnams_sel} FROM Per_{object1} LEFT JOIN Per_{object2} ON Per_{object1}.{object1}_ImageNumber = Per_{object2}.{object2}_ImageNumber AND Per_{object1}.{object1}_Number_Object_Number = Per_{object2}.{object2}_Parent_{object1} LEFT JOIN Per_{object3} ON Per_{object1}.{object1}_ImageNumber = Per_{object3}.{object3}_ImageNumber AND Per_{object1}.{object1}_Number_Object_Number = Per_{object3}.{object3}_Parent_{object1};"
        InnerJoin = f"INSERT INTO Per_Object({colnams_po}) SELECT {colnams_sel} FROM Per_{object1} INNER JOIN Per_{object2} ON Per_{object1}.{object1}_ImageNumber = Per_{object2}.{object2}_ImageNumber AND Per_{object1}.{object1}_Number_Object_Number = Per_{object2}.{object2}_Parent_{object1} INNER JOIN Per_{object3} ON Per_{object1}.{object1}_ImageNumber = Per_{object3}.{object3}_ImageNumber AND Per_{object1}.{object1}_Number_Object_Number = Per_{object3}.{object3}_Parent_{object1};"
        curs.execute(InnerJoin)
        conn.commit()
        # remove excess image number columns
        for d in range(1, len(objects)):
            ob = objects[d]
            curs.execute(f"ALTER TABLE Per_Object DROP COLUMN {ob}_ImageNumber;")
            conn.commit()
        curs.execute(f"ALTER TABLE Per_Object RENAME COLUMN {object1}_ImageNumber TO img_no;")
        # remove excess columns
        for v in range(0, len(objects)):
            ob = objects[v]
            curs.execute(f"ALTER TABLE Per_Object DROP COLUMN {ob}_img_no;")
        close_connection()
    ## time check
    print("Object Tables Created. Time elapsed: %.3f" % (time.time() -
                                                        startTime))


# variable defintions - if using the pre-processing part (1) for per_object views,
# set img_no and obj_no = to themselves, otherwise use the column names 'ImageNumber'
# 'ObjectNUmber' and 'Number_ObjectNumber'

if (db_type == 'SingleObjectView'):
    img_no = 'img_no'
    obj_no = 'obj_no'
    no_ob_no = 'obj_no'
elif (db_type == 'SingleObjectTable'):
    img_no = 'ImageNumber'
    obj_no = 'ObjectNumber'
    no_obj_no = 'Number_Object_Number'


# (2) Check Object Numbers
#### A loop that checks the number of objects for an image to determine if there
#### are >200 objects per image

checklength = []

for h in range(0, len(otherDBs)):
    conn = sqlite3.connect(otherDBs[h], timeout = 10)
    curs = conn.cursor()
    curs.execute(f"SELECT COUNT ({obj_no}) FROM Per_Object;")
    chk = curs.fetchone()
    chk = int(chk[0])
    checklength.append(chk)

do_grouping = any(x > 200 for x in checklength)

## Adds GroupNumber column to Per_Object Table if there are any images with more than 200 objects per image.
## The GroupNumber column and the ImageNumber column will be swapped so that the "Group" is actually the image
## and images will be processed as subsets of 200 objects. The Per_Image table will be updated to have a record
## for each GroupNumber. Finally, the ImageNumber Column and GroupNumber column will be swapped.

#set the group iterator
grpit = 1

if (do_grouping):
    print("One or more of your databases has more than 1k objects per image, a GroupNumber column will be added to all databases.")
    group = 0
    for h in range(0, len(otherDBs)):
    ## Connect to DB
        conn = sqlite3.connect(otherDBs[h], timeout = 10)
        curs = conn.cursor()
        lngt = len(otherDBs)
    ## Get and sort tables from DB
        listTable = get_table_names()
        listTable.sort()
    ## Communicate step
        now = h + 1
        print(f"Adding GroupNumber to {otherDBs[h]}: Per_Object Table. Processing {now} of {lngt}")
###### ObjectNumber table #######
    ## Get ObjectNumber count from Per_Object table and initialize variables
      ## Get the ObjectNumber column from Per_Object
        curs.execute(f"SELECT {obj_no} FROM Per_Object;")
        objno = curs.fetchall()
      ## Set variables for lists
        grp_id = []
        obj_id = fetch_to_list(objno)
    ## Create two lists, one that corresponds to entries in the ObjectNumber column
    ## and one that produces a group_id for each 1000 entries in the ObjectNumber column
        for x in obj_id:
            grp_id.append(grpit) #create a list of group_ids that increments per 200 ObjectNumbers
            if (x % 200 == 0):
                grpit += 1
            else:
                continue
        grpit += 1 #augment group iterator by one for next image
    ## Create a dataframe from the lists
        data = {f'Object_no':obj_id, 'GroupNumber':grp_id}
        df = pd.DataFrame(data)
    ## Send the dataframe to sqlite3 grpnum table and remove objects from env
        df.to_sql('grpnum', conn, schema='main', index = False, chunksize = 1000, method = 'multi')
        del data
        del df
        conn.commit()
    ## Initialize column designations for grouping statement
        colnamtyp = list_to_string(get_column_names_types('Per_Object'), 2)
        colnamtyp = "GroupNumber INTEGER, " + colnamtyp
        colnam = list_to_string(get_column_names('Per_Object'), 1)
        colnam = "GroupNumber, " + colnam
    ## Create a join statement and create a joined table
        SelectJoin = f"SELECT {colnam} FROM Per_Object INNER JOIN grpnum ON grpnum.Object_no = Per_Object.{obj_no}"
        curs.execute(f"CREATE TABLE Joiner({colnamtyp});")
        curs.execute(f"INSERT INTO Joiner({colnam}) {SelectJoin};")
        conn.commit()
    ## Remove the old Grouping and Per_Object tables and rename Joiner to Per_Object
        curs.execute("DROP TABLE IF EXISTS grpnum;")
        curs.execute("DROP TABLE IF EXISTS Per_Object;")
        curs.execute("ALTER TABLE Joiner RENAME TO Per_Object;")
        print(f"Objects are now grouped into blocks of 200 by ImageNumber in {otherDBs[h]}.Per_Object... Use GroupNumber to filter by Image")
        conn.commit()
###### ImageNumber table #######
     ## Get table info
     ## In this case, don't use column types for joiner to avoid ImageNumber Unique constraint
        colnam = list_to_string(get_column_names('Per_Image'), 1)
        colnam = "GroupNumber, " + colnam
     ## Create a grouping table
        curs.execute(f"CREATE TABLE grpnum(GroupNumber INTEGER, {img_no} INTEGER);")
        curs.execute(f"INSERT INTO grpnum(GroupNumber, {img_no}) SELECT DISTINCT GroupNumber, {img_no} FROM Per_Object ORDER BY GroupNumber;")
     ## Create a pandas dataframe to re-create the Per_Image table including the revised group numbering
        grpnum = pd.read_sql_query("SELECT * FROM grpnum;", conn)
        perimg = pd.read_sql_query("SELECT * FROM Per_Image;", conn)
        for (columnName, columnData) in perimg.iteritems():
            if (f"{columnName}" == f'{img_no}' or f"{columnName}" == 'GroupNumber'):
                continue
            else:
                grpnum[f'{columnName}'] = columnData[0]
    ## Send the new perimg dataframe to sqlite
        grpnum.to_sql('perimg', conn, schema='main', index = False, chunksize = 1000, method = 'multi')
        del perimg
        del grpnum
        conn.commit()
     ## Create a the new Per_Image_ table and insert the dataframe records
        curs.execute(f"CREATE TABLE Per_Image_({colnam});")
        curs.execute(f"INSERT INTO Per_Image_({colnam}) SELECT {colnam} FROM perimg;")
        conn.commit()
     ## Renaming Columns for Both Tables
        swap_column_names("Per_Image_", "GroupNumber", f"{img_no}")
        swap_column_names("Per_Object", "GroupNumber", f"{img_no}")
        conn.commit()
     ## Get rid of old tables
        curs.execute("DROP TABLE IF EXISTS grpnum;")
        curs.execute("DROP TABLE IF EXISTS perimg;")
        curs.execute("DROP TABLE IF EXISTS Per_Image;")
        curs.execute("ALTER TABLE Per_Image_ RENAME TO Per_Image;")
        print(f"{otherDBs[h]}.Per_Image ImageNumbers have now been updated to reflect the ImageNumber grouping in Per_Object... Use GroupNumber to filter by Image")
        conn.commit()
close_connection()

## time check
print("Object Grouping Completed. Time elapsed: %.3f" % (time.time() -
                                                         startTime))

# (3) Renumbering Loop
#### For any columns which require unique values in CP (ImageNumber, ObjectNumber, etc), we can renumber the objects
#### in the same sequence they will be merged so that the numbering will be continuous after the merge.

# counters
img = 0
obj_1 = 0
obj_2 = 0
obj_3 = 0


### databases loop
for h in range(0, len(otherDBs)):
### make connection to db
    conn = sqlite3.connect(otherDBs[h], timeout = 10)
    curs = conn.cursor()
### get db info
    lngt = len(otherDBs)
    listTable = get_table_names()
    listTable.sort()
    now = h + 1
    print(f"Renumbering objects. Processing {now} of {lngt}")
### sorts listTable so that Per_Object table is last and renumbering happens correctly
    listTable.append(listTable.pop(listTable.index('Per_Object')))
### re/set counters for looping
    img += 1
    is_run1 = 0
    is_run2 = 0
    is_run3 = 0
###
### remove column constraints (each table from each database)
    for g in range(0, len(listTable)):
        colnamtyp = list_to_string(get_column_names_types(listTable[g]), 2)
        colnam = list_to_string(get_column_names(listTable[g]), 1)
        colset = set(colnam)
        curs.execute("PRAGMA legacy_alter_table = TRUE;")
        curs.execute(f"CREATE TABLE _{listTable[g]}({colnamtyp});")
        curs.execute(f"INSERT INTO _{listTable[g]}({colnam}) SELECT {colnam} FROM {listTable[g]};")
        curs.execute(f"DROP TABLE {listTable[g]};")
        curs.execute(f"ALTER TABLE _{listTable[g]} RENAME TO {listTable[g]};")
        conn.commit()
#######
####### Per_Image ImageNumber renumbering statements
        if (f"{listTable[g]}" == "Per_Image"):
            if (do_grouping):
                curs.execute(f"UPDATE {listTable[g]} SET GroupNumber = {img};")
            else:
                curs.execute(f"UPDATE {listTable[g]} SET {img_no} = {img};")
            if (db_type == 'SingleObjectView'):
                curs.execute(f"ALTER TABLE {listTable[g]} DROP COLUMN IF EXISTS ImageNumber;")
            print(f"Runumbering {img_no} in {otherDBs[h]}: Per_Image table")
            conn.commit()
#######
####### Per_Object1 Table ImageNumber and ObjectNumber renumbering statements for SingleObjectView
        elif (f"{listTable[g]}" == f'Per_{object1}'):
            curs.execute(f"UPDATE {listTable[g]} SET {object1}_{img_no} = {img};")
            curs.execute(f"UPDATE {listTable[g]} SET {object1}_{no_obj_no} = {object1}_{no_obj_no} + {obj_1};")
            curs.execute(f"SELECT COUNT ({object1}_{no_obj_no}) FROM {listTable[g]};")
            temp = curs.fetchone()
            obj_1_temp = obj_1 #hold obj_1 value for per_object table
            obj_1 += temp[0]   #augment obj_1 for next loop
            is_run1 = 1
            print(f"Runumbering ImageNumber and ObjectNumber columns in {otherDBs[h]}: Per_{object1} table")
            conn.commit()
#######
####### Per_Object2 Table ImageNumber and ObjectNumber renumbering statements for SingleObjectView
        elif (f"{listTable[g]}" == f'Per_{object2}'):
            curs.execute(f"UPDATE {listTable[g]} SET {object2}_{img_no} = {img};")
            curs.execute(f"UPDATE {listTable[g]} SET {object2}_{no_obj_no} = {object2}_{no_obj_no} + {obj_2};")
            curs.execute(f"SELECT COUNT ({object2}_{no_obj_no}) FROM {listTable[g]};")
            temp = curs.fetchone()
            obj_2_temp = obj_2 #hold obj_2 value for per_object table
            obj_2 += temp[0]   #augment obj_2 for next loop
            is_run2 = 1
            print(f"Runumbering ImageNumber and ObjectNumber columns in {otherDBs[h]}: Per_{object2} table")
            conn.commit()
#######
####### Per_Object3 Table ImageNumber and ObjectNumber renumbering statements for SingleObjectView
        elif (f"{listTable[g]}" == f'Per_{object3}'):
            curs.execute(f"UPDATE {listTable[g]} SET '{object3}_{img_no}' = {img};")
            curs.execute(f"UPDATE {listTable[g]} SET {object3}_{no_obj_no} = {object3}_{no_obj_no} + {obj_3};")
            curs.execute(f"SELECT COUNT ({object3}_{no_obj_no}) FROM {listTable[g]};")
            temp = curs.fetchone()
            obj_3_temp = obj_3 #hold obj_3 value for per_object table
            obj_3 += temp[0]   #augment obj_3 for next loop
            is_run3 = 1
            print(f"Runumbering ImageNumber and ObjectNumber columns in {otherDBs[h]}: Per_{object3} table")
            conn.commit()
#######
####### Set SingleObjectTable counters
        elif (f"{listTable[g]}" == 'Per_Object'):
            if (db_type == 'SingleObjectTable'):
                if (is_run1 != 1):
                    curs.execute(f"SELECT COUNT ({object1}_{no_obj_no}) FROM {listTable[g]};")
                    temp = curs.fetchone()
                    obj_1_temp = obj_1
                    obj_1 += temp[0]
                if (is_run2 != 1):
                    curs.execute(f"SELECT COUNT ({object2}_{no_obj_no}) FROM {listTable[g]};")
                    temp = curs.fetchone()
                    obj_2_temp = obj_2
                    obj_2 += temp[0]
                if (is_run3 != 1):
                    curs.execute(f"SELECT COUNT ({object3}_{no_obj_no}) FROM {listTable[g]};")
                    temp = curs.fetchone()
                    obj_3_temp = obj_3
                    obj_3 += temp[0]
###########
########### Per_Object Table ImageNumber and ObjectNumber renumbering statements
            print(f"Runumbering ImageNumber and ObjectNumber columns in {otherDBs[h]}: {listTable[g]} table")
###########
########### Renumber ImageNumber Column
            if (do_grouping):
                curs.execute(f"UPDATE {listTable[g]} SET GroupNumber = {img};")
            else:
                curs.execute(f"UPDATE {listTable[g]} SET {img_no} = {img};")
###########
########### NB. CONDITIONAL UPDATE SYNTAX Table_Name SET Column = CASE WHEN (Column IS NULL) THEN (Column) ELSE (Column + MATH) END;
########### Renumber ObjectNumber the same way as object1, conditional for non-null values
            curs.execute(f"UPDATE {listTable[g]} SET {obj_no} = CASE WHEN ({obj_no} IS NULL) THEN ({obj_no}) ELSE ({obj_no} + {obj_1_temp}) END;")
###########
########### Renumber object1 if not null (left join may produce null values)
            curs.execute(f"UPDATE {listTable[g]} SET {object1}_{no_obj_no} = CASE WHEN ({object1}_{no_obj_no} IS NULL) THEN ({object1}_{no_obj_no}) ELSE ({object1}_{no_obj_no} + {obj_1_temp}) END;")
###########
########### Renumber object2 if not null (left join may produce null values)
            curs.execute(f"UPDATE {listTable[g]} SET {object2}_{no_obj_no} = CASE WHEN ({object2}_{no_obj_no} IS NULL) THEN ({object2}_{no_obj_no}) ELSE ({object2}_{no_obj_no} + {obj_2_temp}) END;")
###########
########### Renumber object3 if not null (left join may produce null values)
            curs.execute(f"UPDATE {listTable[g]} SET {object3}_{no_obj_no} = CASE WHEN ({object3}_{no_obj_no} IS NULL) THEN ({object3}_{no_obj_no}) ELSE ({object3}_{no_obj_no} + {obj_3_temp}) END;")
###########
########### Commit Changes and remove temporary stored variables
            conn.commit()
            del obj_1_temp
            del obj_2_temp
            del obj_3_temp
        else:
            continue
############
############ Rename img_no and obj_no columns in Per_Object table created from SingleObjectView output
    if (db_type == 'SingleObjectView'):
        curs.execute("ALTER TABLE Per_Image RENAME COLUMN {img_no} TO ImageNumber;")
        curs.execute("ALTER TABLE Per_Image RENAME COLUMN {obj_no} TO ObjectNumber;")
        curs.execute("ALTER TABLE Per_Object RENAME COLUMN {img_no} TO ImageNumber;")
        curs.execute("ALTER TABLE Per_Image RENAME COLUMN {obj_no} TO ObjectNumber;")
        conn.commit()
### Close Connection and communicate time check
    close_connection()
    print(f"Pre-processing of {otherDBs[h]} complete). Time elapsed: %.3f" % (time.time() -
                                                                              startTime))


# 3. Database Merge Module
##################################
## A nested for loop iterates through the blocks in this version
## to prevent an error from trying to attach more than ten DBs at a time

print("Merging databases initiated at: " + strftime("%H:%M", gmtime()))
otherDBs.pop(0) # removes the first database (mainDB) from filenames, turn this off if MainDB is not in filenames.txt
DBs_attacher = list(divide_list(otherDBs, 10))
nBlocks = len(DBs_attacher)
Total_DBs_attacher = int(sum([len(block) for block in DBs_attacher]))
print("Total: "+str(Total_DBs_attacher)+" Blocks: "+str(nBlocks))

# Merge Loop

for u in range(0, len(DBs_attacher)):                                         # Block level iterator
    conn = sqlite3.connect(mainDB, timeout = 15)                              # Attach main database
    curs = conn.cursor()                                                      # Attach cursor
    listDB.append([])                                                         # Add a new block to listDB
    now = u+1
    print("Now processing: "+str(now)+" of "+str(nBlocks))
    for n in range(0, len(DBs_attacher[u])):                                  # Sub-Block level iterator, n<=10 
        attach_database(DBs_attacher[u][n], u, n)                             # Attach databases within block
        for j in range(0, len(listTable)):                                    # for each table in each database
            columns = list_to_string(get_column_names(listTable[j]), 1)       # get each column for each table
            merge_table(listTable[j], columns, listDB[u][n])                  # and insert rows from these columns, in this database, in the equivalent table in main
            conn.commit()                                                     # Commit changes one last time after a database in the block is done
        os.remove(f"{DBs_attacher[u][n]}")                                    # Removes merged db after the merge to conserve space on disk
    close_connection()                                                        # Close connection at end of each block of ten databases
    print("Finished merging: "+str(u)+" of"+str(nBlocks)+". Time elapsed: %.3f" % (time.time() -
                                                                                   startTime))

try:
    conn = sqlite3.connect(mainDB, timeout = 15)
    curs = conn.cursor()
    print("Cleaning up the main database. Please wait...")
    curs.execute(f"VACUUM;")
except Exception():
    traceback.exc()

print("All databases finished merging. Time elapsed: %.3f" % (time.time() -
                                                          startTime))

# VACUUM to defragment db
conn = sqlite3.connect(mainDB, timeout = 15)
curs = conn.cursor()
curs.execute("VACUUM;")

# Run post_processing.py
os.system("python3 post_processing.py")
