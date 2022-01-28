import sqlite3
import sys
import six
import base64
import uuid
import pyodbc

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


######
hardkey = "dashboard"
######

## check if the user ACTUALLY wants to do this

check = input("### This will overwrite the current connection settings. Do you want to change them now? (Y/N): ")

if check not in ("Y", "y"):
    input("### Please run this exe file again if settings need to change")
    sys.exit()

## let's go and check the database and/or create it if required

db = sqlite3.connect("PiConnections.db")
cursor = db.cursor()
cursor.execute("DROP TABLE IF EXISTS tblConnStr")
cursor.execute("CREATE TABLE IF NOT EXISTS tblConnStr(strSQLServer TEXT, strSage TEXT, strSoftkey TEXT )")
db.commit()

## go and get a unique key

softkey = str(uuid.uuid4())

## stick it in the db

data = ("INSERT INTO tblConnStr(strSoftkey) VALUES('%s')" % (softkey))
cursor.execute(data)
db.commit()

## just make sure we're using the softkey that is stored to encrypt

cursor.execute("SELECT strSoftkey FROM tblConnStr")
softkey = cursor.fetchone()[0]

key = hardkey + softkey

## Now go and get the SQL SERVER details from the user and enter in our db

while True:
    print("\n### Please enter the SQL Server details ###")
    strServer = input("Enter SQL Server Host Name (e.g. localhost\SQLEXPRESS): ")
    strDBName = input("Enter SQL Database Name (e.g. PI_Sage50): ")
    strUser = input("Enter SQL UserName: ")
    strPass = input("Enter SQL Password: ")

    SQLstring = ("Driver={ODBC Driver 17 for SQL Server};Server=%s;Database=%s;UID=%s;PWD=%s;" % (strServer,strDBName,strUser,strPass))


    ## encrypt the string and store in the db

    en = encryption(key, SQLstring)
    en = en.decode('ASCII')

    data = ("UPDATE tblConnStr SET strSQLServer = '%s' WHERE strSoftkey = '%s'" % (en, softkey))

    cursor.execute(data)
    db.commit()

    ## Now go and get the Sage 50 details from the user and enter in our db

    print("\n### Please enter the Sage 50 details ###")
    strUser = input("Enter Sage 50 UserName: ")
    strPass = input("Enter Sage 50 Password: ")

    filename = "PIDataSources.txt"
    S50String = ("UID=%s;PWD=%s; " % (strUser, strPass))
    en = encryption(key, S50String)
    en = en.decode('ASCII')
    data = ("UPDATE tblConnStr SET strSage = '%s' WHERE strSoftkey = '%s' " % (en, softkey))
    cursor.execute(data)
    db.commit()

    # Test the connection details by inserting audit table
    try:

        with open(filename) as f:
            source = f.readlines()
            source = [x.strip() for x in source]

        comcount = 0
        for strDSN in source:
            print(strDSN)
            DSNcheck = strDSN
            strDSN = "DSN=" + strDSN +";"
            db = sqlite3.connect("PiConnections.db")
            cursor = db.cursor()
            cursor.execute("SELECT strSoftkey FROM tblConnStr")
            connsoftkey = str(cursor.fetchone()[0])
            key = hardkey + connsoftkey

            cursor.execute("SELECT strSQLServer FROM tblConnStr")
            sqlstring = str(cursor.fetchone()[0])

            cursor.execute("SELECT strSage FROM tblConnStr")
            s50string = str(cursor.fetchone()[0])

            sqlconn = decryption(key, sqlstring)
            s50conn = strDSN + decryption(key, s50string)
            Sage50_conn = pyodbc.connect(s50conn)
            SQL_conn = pyodbc.connect(sqlconn)

            cursor1 = Sage50_conn.cursor()
            SQLData = SQL_conn.cursor()
            if comcount == 0:
                auditcheck = ("IF OBJECT_ID('dbo.SAGE50_ETL_AUDIT', 'U') IS NOT NULL DROP TABLE dbo.SAGE50_ETL_AUDIT")

                SQLData.execute(auditcheck)
                SQLData.commit()

                audittable = ("CREATE TABLE [dbo].[SAGE50_ETL_AUDIT]([ID] [int] IDENTITY(1,1) NOT NULL,"
                              "[Table_Name] [varchar](200) NULL,"
                              "[Started_Update] [datetime] NULL,"
                              "[Completed_Update] [datetime] NULL,"
                              "[PI_ID] INT NULL,"
                              ")")

                SQLData.execute(audittable)
                SQLData.commit()

            for table in cursor1.tables():
                tb = table.table_name
                auditsetup = ("INSERT INTO [dbo].[SAGE50_ETL_AUDIT] (Table_Name,PI_ID) VALUES (?,?)")
                SQLData.execute(auditsetup, tb, comcount)
                SQLData.commit()
            comcount = comcount + 1
            SQLData.close()
            Sage50_conn.close()
        input("\n### Credentials Updated Successfully ### \n Press any key to continue")
        db.close()
        break

    except Exception as e:
        check = "driver"
        if check in str(e):
            print("\n### ERROR:\n" + str(e))
            print(DSNcheck + ": Is this the correct Data Source Name?")
            print("\nPlease check that the Microsoft ODBC Driver 17 for SQL Server "
                  "and Sage 50 ODBC drivers are both installed and configured correctly.")
            print("\n### Please check and re-enter details below ###")
        else:
            print("\n### ERROR:\n" + str(e))
            print("\n### DETAILS INCORRECT ### \n### Please check and re-enter details below ###")


