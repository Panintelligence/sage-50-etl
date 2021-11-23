import sqlite3
import six
import base64
import pyodbc
import datetime
import os.path
import time

start = time.time()
logfile = "Sage50DataErrorsLog.txt"
if os.path.exists(logfile):
    os.remove(logfile)

def encryption(key, string):
    encoded_chars = []
    for i in range(len(string)):
        key_c = key[i % len(key)]
        encoded_c = chr(ord(string[i]) + ord(key_c) % 256)
        encoded_chars.append(encoded_c)
    encoded_string = ''.join(encoded_chars)
    encoded_string = encoded_string.encode('latin') if six.PY3 else encoded_string
    return base64.urlsafe_b64encode(encoded_string).rstrip(b'=')


def decryption(key, string):
    string = base64.urlsafe_b64decode(string + '===')
    string = string.decode('latin') if six.PY3 else string
    encoded_chars = []
    for i in range(len(string)):
        key_c = key[i % len(key)]
        encoded_c = chr((ord(string[i]) - ord(key_c) + 256) % 256)
        encoded_chars.append(encoded_c)
    encoded_string = ''.join(encoded_chars)
    return encoded_string


## removing funny characters function ##
def _removeNonAscii(s):
    return "".join(i for i in s if ord(i)<128)

## Convert an empty string or a string set as none to null ##
def none_to_null(string, wrap=True):
    if string is None or str(string) == "None" or str(string) == '':
        return 'null'
    else:
        return (str(string), "'" + str(string) + "'")[wrap]

def executeScriptsFromFile(filename):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')

    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            #print(command)
            SQL_conn.execute(command)
            SQL_conn.commit()
        except Exception as ex:
            print("\rERROR: %s" % ex)
            current_time = datetime.datetime.now()
            f = open(logfile, "a+")
            f.write("%s \rERROR %s" % (current_time, ex) + " " + command)

def resolve_type(column_type, none_as_varchar):
    if column_type is None:
        if none_as_varchar:
            return "VARCHAR(MAX)"
        return "DATETIME"
        
    return str(column_type).upper().replace("SMALLINT", "INT")\
        .replace("TINYINT", "INT")\
        .replace("DOUBLE", "DECIMAL(18,2)")\
        .replace("VARCHAR", "VARCHAR(MAX)")\
        .replace("LONG VARCHAR(MAX)", "VARCHAR(MAX)")\
        .replace("TIMESTAMP", "VARCHAR(MAX)")

######
hardkey = "dashboard"
######

## catch any errors
try:
### SQL Connections as this is staying the same

    db = sqlite3.connect("PiConnections.db")
    cursor = db.cursor()
    cursor.execute("SELECT strSoftkey FROM tblConnStr")
    softkey = str(cursor.fetchone()[0])
    key = hardkey + softkey

    cursor.execute("SELECT strSQLServer FROM tblConnStr")
    sqlstring = str(cursor.fetchone()[0])

    sqlconn = decryption(key, sqlstring)
    SQL_conn = pyodbc.connect(sqlconn)
    SQLData = SQL_conn.cursor()

    ### go and get the number of companies required from file
    filename = "PIDataSources.txt"
    with open(filename) as f:
        source = f.readlines()
        source = [x.strip() for x in source]

    ### loop through each source and build the different DSNs
    comcount = 0
    for strDSN in source:
        strDSN = "DSN=" + strDSN + ";"
        db = sqlite3.connect("PiConnections.db")
        cursor = db.cursor()
        cursor.execute("SELECT strSoftkey FROM tblConnStr")
        softkey = str(cursor.fetchone()[0])
        key = hardkey + softkey

        cursor.execute("SELECT strSage FROM tblConnStr")
        s50string = str(cursor.fetchone()[0])
        s50conn = strDSN + decryption(key, s50string)
        Sage50_conn = pyodbc.connect(s50conn)

        ### create several cursors so they're not reused
        cursor0 = Sage50_conn.cursor()
        cursor1 = Sage50_conn.cursor()
        cursor2 = Sage50_conn.cursor()
        cursor3 = Sage50_conn.cursor()
        #SQLData = SQL_conn.cursor()

        ### don't need to get all the tables of data so have an EXCLUDE file
        filename = "Exclude_Tables.txt"
        with open(filename) as f:
                exclude = f.readlines()
                exclude = [x.strip() for x in exclude]

        #############################
        ## Create Tables and Login ##
        #############################
        c = "SELECT NAME FROM COMPANY"
        cursor0.execute(c)
        compname = str(cursor0.fetchone()[0])
        print("Setting up Tables for: " + compname)

        result = ''
        tables = cursor1.tables()
        for table in tables:
            tb = table.table_name
            if tb in exclude:
                continue
            print(f"Setting up table: {tb}")
            if comcount == 0:
                tcheck = ("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s" % (tb, tb))
                print(f"Dropping {tb} if it exists...")
                SQLData.execute(tcheck)
                SQLData.commit()
                result = "CREATE TABLE " + tb + "("
                columns = []
                found_datetime = False
                print(f"Building query to (re)create {tb}...")
                for col in cursor2.columns(tb):
                    datatype = resolve_type(col.type_name, found_datetime)
                    if not found_datetime:
                        found_datetime = datatype == "DATETIME"
                    colstr = str(" [" + col.column_name + "] " + datatype + ",")
                    columns.append(str(col.column_name))
                    result = result + colstr
                result = result + "[PI_ID] INT )"
                print(f"Building {tb} with query:")
                print(result)
                SQLData.execute(result)
                SQLData.commit()

            #######################
            # Now run the ETL bit #
            #######################

            # Only keep 90 days of audit data
            auditremove = "DELETE FROM SAGE50_ETL_AUDIT WHERE ISNULL(DATEDIFF(D,Started_Update,GETDATE()),91) > 90 "
            print("Removing old audits...")
            SQLData.execute(auditremove)
            SQLData.commit()

            # Insert start time into audit check table
            auditinsert = ("INSERT INTO SAGE50_ETL_AUDIT (Table_Name, Started_Update, Completed_Update, PI_ID) VALUES(?, GETDATE(), NULL, ?) ")
            print("Adding audit information...")
            SQLData.execute(auditinsert, tb, comcount)
            SQLData.commit()
            # Get the insert ID
            SQLData.execute("SELECT MAX(ID) FROM SAGE50_ETL_AUDIT")
            row = SQLData.fetchone()
            print("Fetching latest audit id...")
            auditid = row[0]


            # now go and get the data from within each table #
            getdata = ("SELECT * FROM %s " % tb)
            print(f"Selecting data from {tb}...")
            cursor2.execute(getdata)
            print("Grabbing columns...")
            columns = [column[0] for column in cursor2.description]
            print("Got columns:")
            print(columns)
            records = 0
            while True:
                print(f"Fetching batch of 5000 records for {tb}")
                result = cursor2.fetchmany(5000)
                if not result:
                    print(f"Got null. Skipping...")
                    break
                records = records + int(len(result))
                if len(result) == 0:
                    print("Got no results, moving on...")
                    auditinsert = "UPDATE SAGE50_ETL_AUDIT SET Completed_Update = GETDATE() WHERE ID = ? "
                    SQLData.execute(auditinsert, auditid)
                    SQLData.commit()
                    continue
                no_of_columns = len(result[0])
                print(f"Preparing query to insert {no_of_columns} values in one row...")
                cols = ['?'] * no_of_columns
                sql = ("INSERT INTO %s VALUES (%s, %d)") % (tb, ",".join(cols), comcount)
                print(f"Inserting data...")
                SQLData.fast_executemany = True
                SQLData.executemany(sql, result)
                SQLData.commit()

            print("Records Updated: %s\n" % str(records))
             # Insert end time into audit check table
            auditinsert = "UPDATE SAGE50_ETL_AUDIT SET Completed_Update = GETDATE() WHERE ID = ? "
            print("Updating audits...")
            SQLData.execute(auditinsert, auditid)
            SQLData.commit()
        comcount = comcount + 1
        Sage50_conn.close()

    print("Tables Updated\n")

    print("Adding SQL views...\n")

    executeScriptsFromFile('Sage50_Views.sql')

    print("Running SQL scripts...\n")

    executeScriptsFromFile('Sage50_Script.sql')
    SQL_conn.close()


except Exception as ex:
    print("\rERROR: %s" % ex)
    current_time = datetime.datetime.now()
    f = open(logfile, "a+")
    f.write("%s \rERROR: %s" % (current_time, ex) + " " + sql)
    ##input("Press any key to exit")

end = time.time()
print("##### COMPLETE ##### \nTime Taken: %s seconds" % str(round(end - start)))
time.sleep(30)
