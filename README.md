# SQLite-MegaMerge-for-Distributed-CellProfiler
## For merging sqlite database output from Distributed CellProfiler for use in CellProfiler Analyst

## How to use this repository

#### NB. I run this from an AWS EC2 instance. It requires at least python3, sqlite3, and pandas. Submit an issue if you have any problems and I'll see what I can do.

### Step 1
#### Move your databases to an empty directory

### Step 2
#### Pull a copy of MegaMergeScript.py and post-processing.py into the directory

### Step 3a
#### Create a list of filenames
````
For example, using:
ls /home/ubuntu/databases/*.db > filenames.txt
````
The filenames.txt file should contain a plain list of filenames with a complete filepath, such as that produced by the command above. There is code within the script that will handle this file. It should contain all databases (incl. mainDB at position 0, ideally). Leave it in the databases/ directory with the .db files.

### Step 3b (optional)
#### Modifying the input parameters
````
#################################################################################
############################## (2) Input Parameters #############################

# 2.1 DEFINE A LIST OF DBs TO MERGE
##################################
# define a list of DBs using filenames from the folder containing databases by running ls /home/ubuntu/databases/*.db > filenames.txt
# ...needs to be full path for sqlite3 to interpret correctly
# otherDBs can also be defined by a list e.g. otherDBs = [] for example,
# otherDBs = ['/home/ubuntu/databases/5-9856-4928.db', '/home/ubuntu/databases/5-9856-5376.db']

with open('filenames.txt', 'r') as fileNames:
    otherDBs = [line.strip() for line in fileNames]
#otherDBs = ['1-0-0.db', '1-9408-3584.db']

# 2.2 DEFINE A MAIN DB AS A TEMPLATE FOR THE MERGE
###################################
# This is where the main database is ie. the first if a list of identical DBs but can be defined otherwise

mainDB = otherDBs[0]
````

DEFINE A LIST OF DBs TO MERGE:
The program will merge the databases in otherDBs into mainDB. The code as-is is meant to set the first .db file in filenames.txt as mainDB and merge the others into this file. However, mainDB and otherDBs can be set normally, e.g. otherDBs = [obj1, obj2, ... objn]. If so, you need to comment out the pop statement in (5) Database Merge Module (*****see below).

DEFINE A MAIN DB AS A TEMPLATE FOR THE MERGE: 
Leave as-is unless changing how the list of DBs is defined. In that case, set it to whichever database you want all other databases merged into.

*If for some reason you're no longer working with mainDB as the first item in filenames.txt, make sure to comment out the pop statement at the top of (5) Database Merge Module further down in the script. This statement removes the first database in the list of databases acquired from filenames.txt so that the database will not be merged to itself when using the standard setup. If you've set another DB (not in filenames.txt) as the main DB, this pop statement will cause the script to skip the first database in the list so it will not be merged. See example below.*

````
#################################################################################
########################## (5) Merging Databases ################################

# 5.1 Initialization Parameters
####
####

print("Merging databases initiated at: " + strftime("%H:%M", gmtime()))
otherDBs.pop(0) # removes the first database (mainDB) from filenames, turn this off if MainDB is not in filenames.txt <<<==== HERE ======
DBs_attacher = list(divide_list(otherDBs, 10))
nBlocks = len(DBs_attacher)
Total_DBs_attacher = int(sum([len(block) for block in DBs_attacher]))
print("Total: "+str(Total_DBs_attacher)+" Blocks: "+str(nBlocks))

````

QUALITY CONTROL:
Leave as-is.

````
# 2.3 QUALITY CONTROL
###################################

if len(otherDBs) == 0:
    print("ERROR: No databases have been added for merging.")
    sys.exit()

````
WHAT KIND OF DATABASE WAS OUTPUT BY CELLPROFILER:
Set db_type to either "SingleObjectTable" or "SingleObjectView" depending on the type of output you set in CellProfiler's ExportToDatabase module.

````
# 2.4 WHAT KIND OF DATABASE WAS OUTPUT BY CELLPROFILER
###################################
# 'SingleObjectTable' or 'SingleObjectView' are currently supported

db_type = "SingleObjectTable"

````

WHAT ARE THE NAMES OF THE OBJECTS YOU MEASURED:
Leave as-is unless you're using "SingleObjectView" output. This output create a single per_object table for each type of object. Currently, this version of the script supports three objects (three channels). This will be required for the script to create the merged Per_Object table from the individual channel tables.

If you're using "SingleObjectView" output, set each object variable to the name you chose for your object in CellProfiler (e.g. "Nuclei").

````
# 2.5 WHAT ARE THE NAMES OF THE OBJECTS YOU MEASURED
###################################
# ie. what are the names of the objects that will be in the columns/table names of your database

object1 = 'FilterNuclei1' # primary nuclear object
object2 = 'IdentifyNG2'   # secondary object based on nucleus
object3 = 'IdentifyYFP'   # secondary object based on nucleus

````

### Step 4
#### If there are tables that don't need to be merged, set them in Define Functions (#3) get_table_names()

````

# 1.3 Get the table names of a database
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
    
````
Add or comment out the if/else statements to skip merging specific tables.

### Step 5
#### Run MegaMergeScript from /databases/ directory
````
python3 MegaMergeScript.py
````

### Basic Troubleshooting Insights
Constraint errors - these are tricky and are caused by Primary Key, NOT NULL, UNIQUE constraints on specific columns, etc. These should not occur because you're moving all data into new tables without constrained columns, but getting these errors usually require a look under the hood. Use DB-Browser or equivalent to visually inspect a db and double check the columns and tables you're trying to merge. Adding print statements to check table or column name output can be helpful to check against. The constraints required for CellProfiler Analyst will be added back in post-processing.py

Database size or disk space error - when working with large datasets, the DISK SPACE error is the more likely culprit (unless your data is >100 TB). Make sure to check that your storage is adequate (e.g. if using an AWS instance), and that there are no ongoing processes eating your memory.

SQLite error in view, no such table - if your db has views, it will require the PRAGMA legacy_alter_table=TRUE statement in the pre-processing loop. It comes set this way.

Database connection, or database closed - The conn = sqlite.connect(<db>, timeout = 10) setting can be set to a longer timeout if your processing is significant. This may occur if more than one process is trying to write to the same db or if you've introduced edits and the commmits are not happening frequently enough or in the right places.

### What the script is doing

#### Input Parameters - Section (2)
  Here you're telling the script what dbs you're working with. By default, the script will try to merge all the dbs in filenames.txt with the first db in filenames.txt (mainDB). 

#### Quality Control - Section (3)
  Here the scripts gets the names of the tables for merging from mainDB, then iterates through all the otherDBs and compares the table numbers. If there are less tables in main DB than in a 'otherDB', only the tables printed at the top of the QC step will be merged. If there are more tables in mainDB than in otherDB, otherDB is discarded from the merge process entirely. The QC section will print statements to the console that will record which tables are logged as matches or exceptions during the iteration phase as well as at the end. If there are a lot of dbs, this is helpful to avoid excess scrolling.
  
#### Pre-Processing - Section (4)
There are 3 main modules in the Pre-Processing Section:
1. The first is for "SingleObjectView" output to create a Per_Object table that can be used by CellProfiler Analyst (CPA).
2. The second checks the Per_Object table for the number of objects per image, and renumbers the databases to handle large numbers of objects. 
    - The reason is that I have found that with large databases CPA classifier does not handle the data well, probably because either the memory required is too much for my computer to handle, or because the SQL query for the database takes too long and something times out in classifier. 
    - What this module does is check the Per_Object table for any database containing more than 200 objects per image. If it finds this is the case, it groups the objects in each image into sets of 200 and renumbers the ImageNumber column with these group numbers (effectively setting each "image" at a maximum of 200 objects. It moves the original "ImageNumber" designation to a column called "GroupNumber" that can be used to aggregate object count data by image after classification (for instance, by using GROUP BY in your SQL query later on).
3. The third module removes column constraints from all tables in order to facilitate the merging process. After this is done, the ImageNumber and ObjectNumber columns are renumber to be continuous from database to database, so that the ImageNumbers in Per_Image are unqiue, and the ObjectNumbers in Per_Object are unique. This is required for the merged database to function correctly in CPA.

#### Merging Databases - Section (5)
  This section relies on the same scheme as in @gopherchuck's original code. However, SQLite3 can only attach ten databases to mainDB at a time, so the otherDBs list is used to create DBs_attacher, a nested list of lists, ten a piece. The code essentially goes through the same process, but requires the database attachment and merge process in a nested for-loop, instead of two separate loops. Elsewhere, counters have been adjusted to reflect the counting process for handling blocks and sub-blocks.
  
#### Finalizing Merge - Section (5)
  The code in this section will use sqlite3 VACUUM function to clean up the database. This will reduce the file size by removing deprecated references in the database.
  
  NB. This section will automatically try to run the post processing script (post_processing.py).
  
#### Other notes
  The code is not generalized and contains some parts that are vestiges of other modules I am not currently running. I apologize if there are some inefficiencies, as this was not my goal in developing this code. Please feel free to submit an issue if there are problems/solutions that need to be addressed.
