#!/usr/bin/python3
__author__ = 'Amit Tyagi, Sherwin-Williams Company'

# importing modules
import sys
import cx_Oracle
import os
import argparse

# Agruments parsing:
parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, \
description='This script generates the DDL for Snowflake from an Oracle Database for a given schema/single object.\n\
ex: dwuser_ro password obid_dbhost.abc.com 1521 obid [object_name/ALL] -d C:\Temp')
parser.add_argument("user_name", help="Database user/schema name.")
parser.add_argument("password", help="Database password.")
parser.add_argument("host_name", help="Database host name ex: obid_dbhost.sherwin.com")
parser.add_argument("port", help="Database port. ex: 1521", type=int)
parser.add_argument("service_name", help="Database service name.")
parser.add_argument("object_name", help="Provide object name if you want DDL for a single object else say ""ALL"" for all objects in the schema.",default='-1')
parser.add_argument("-d","--d",dest="directory_name",help="Directory to generate DDL Scripts. Default is current working directory.",default=os.getcwd())
args = parser.parse_args()

#Specify Database object type for the DDL
DDL_OBJECT_TYPE_LIST = ['TABLE', 'VIEW', 'PROCEDURE', 'PACKAGE', 'FUNCTION', 'SEQUENCE']
print ("\nThis script will create DDL for Snowflake from an Oracle Database for a given single object.")
print (str(DDL_OBJECT_TYPE_LIST).replace("'",""))

# Set the Directory where you want to generate the DDL file
os.chdir(args.directory_name)
print("\nCurrent working directory (where scripts will be generated):",os.getcwd(),"\n")

# Set the arguments provided on the command line
p_schema_name = args.user_name.upper()
p_object_name = args.object_name.upper()

# Set oracle connection details <username/password@//hostname:port/serviceName>
con = cx_Oracle.connect(args.user_name+'/'+args.password+'@//'+args.host_name+':'+str(args.port)+'/'+args.service_name)

# Create cursors from the connection
cursor_ddl = con.cursor()
cursor_objects = con.cursor()

# Define Starting query which will provide Owner, Object_type and Object_name for which DDL has to be generated. 
# It must order by following: ORDER  BY OWNER, OBJECT_TYPE, OBJECT_NAME
def DDL_OBJECTS_LIST_ALL (fp_schema_name, fp_object_type, fp_object_name):
    return """
    SELECT OWNER, OBJECT_TYPE, OBJECT_NAME
    FROM   DBA_OBJECTS DO
    INNER  JOIN DBA_USERS DU
    ON     DO.OWNER = DU.USERNAME
    WHERE  DU.ORACLE_MAINTAINED = 'N'
    AND    OBJECT_NAME NOT LIKE '%$%' /* Excluding system/ODI temporary objects */
    AND    OWNER = UPPER('""" + fp_schema_name + """')
    AND    OBJECT_TYPE = UPPER(DECODE('"""+ fp_object_type + """','ALL',OBJECT_TYPE,'"""+ fp_object_type + """'))
    AND    OBJECT_NAME = UPPER(DECODE('"""+ fp_object_name + """','ALL',OBJECT_NAME,'"""+ fp_object_name + """'))    
    ORDER  BY OWNER, OBJECT_TYPE, OBJECT_NAME
"""

# Use oracle utility to get DDL for objects other than tables.
def DDL_QUERY (fp_object_owner,fp_object_type,fp_object_name) :
    return """
SELECT DBMS_METADATA.GET_DDL(UPPER('"""+ fp_object_type +"""'), UPPER('"""+ fp_object_name +"""'), UPPER('"""+ fp_object_owner +"""'))
FROM DUAL 
"""

# Get DDL for the Primary Key and Unique Keys
def DDL_TABLE_PRIMARY_UNIQUE_KEYS(fp_object_owner,fp_object_name):
    return """
        SELECT 'ALTER TABLE ' || TABLE_NAME || ' ADD CONSTRAINT ' || CONSTRAINT_NAME || CONSTRAINT_TYPE || CONSTRAINT_COLUMNS || ');' AS CONSTRAINT_DDL
        FROM   (SELECT A.OWNER,
               A.TABLE_NAME,
               A.CONSTRAINT_NAME,
               DECODE(B.CONSTRAINT_TYPE, 'P', ' PRIMARY KEY ( ', 'U', ' UNIQUE (', CONSTRAINT_TYPE) AS CONSTRAINT_TYPE,
               LISTAGG(A.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY A.POSITION) AS CONSTRAINT_COLUMNS
        FROM   DBA_CONS_COLUMNS A, DBA_CONSTRAINTS B
        WHERE  A.OWNER = B.OWNER
        AND    A.TABLE_NAME = B.TABLE_NAME
        AND    A.CONSTRAINT_NAME = B.CONSTRAINT_NAME
        AND    CONSTRAINT_TYPE IN ('P', 'U')
        AND    A.OWNER = UPPER('""" + fp_object_owner + """')
        AND    A.TABLE_NAME = UPPER('""" + fp_object_name + """')
        GROUP  BY A.OWNER, A.TABLE_NAME, A.CONSTRAINT_NAME, B.CONSTRAINT_TYPE)    
    """
    
def DDL_TABLE_REFERENTIAL_INTIGRITY_CONS(fp_object_owner, fp_object_name):
    return """
    SELECT DBMS_METADATA.GET_DDL('REF_CONSTRAINT', CONSTRAINT_NAME, OWNER)
    FROM   DBA_CONSTRAINTS
    WHERE  OWNER = UPPER('""" + fp_object_owner + """')
    AND    TABLE_NAME = UPPER('""" + fp_object_name + """')
    AND    CONSTRAINT_TYPE = 'R'
    """

# Define function which will create DDL for the given OWNER and TABLE
def TABLE_DDL (fp_owner, fp_table_name):
    return """
WITH ORACLE_TABLE_COLUMNS_VW AS
(SELECT DBMS_XMLGEN.GETXMLTYPE('
SELECT A.OWNER, 
       A.TABLE_NAME, 
       A.COLUMN_NAME, 
       A.DATA_TYPE, 
       A.DATA_LENGTH, 
       A.CHAR_LENGTH, 
       A.DATA_PRECISION, 
       A.DATA_SCALE, 
       A.COLUMN_ID, 
       A.DATA_DEFAULT, 
       A.NULLABLE,
       A.VIRTUAL_COLUMN,
       B.COMMENTS AS TABLE_COMMENT,
       C.COMMENTS AS COLUMN_COMMENTS
FROM   DBA_TAB_COLS A LEFT   OUTER JOIN DBA_TAB_COMMENTS B ON A.OWNER = B.OWNER AND A.TABLE_NAME = B.TABLE_NAME AND TABLE_TYPE = ''TABLE''
LEFT   OUTER JOIN DBA_COL_COMMENTS C ON (A.OWNER = C.OWNER AND A.TABLE_NAME = C.TABLE_NAME AND A.COLUMN_NAME = C.COLUMN_NAME)
WHERE  HIDDEN_COLUMN = ''NO'' 
AND A.OWNER = UPPER(''""" + fp_owner + """'')
AND A.TABLE_NAME = UPPER(''""" + fp_table_name + """'')
') AS XML_COL
  FROM   DUAL)
SELECT /*COLUMN_ID, MAX_COLUMN_ID,*/
          -- Noformat Start
  /* Create table syntax */
    CASE WHEN COLUMN_ID=1 THEN 'CREATE OR REPLACE TABLE '||TABLE_NAME||'(' END||
  /* Enclose column name in double quotes */
    RPAD('"' || COLUMN_NAME || '"', 40, ' ') ||
  /* Decode and define data types*/
    RPAD(CASE WHEN DATA_TYPE IN ('VARCHAR2','CHAR','RAW','CLOB','LONG') THEN 'VARCHAR'
         WHEN DATA_TYPE = 'DATE' THEN NULL
           ELSE DATA_TYPE END ||
  /* Map Column precision to Snowflake */
    CASE WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN '(' || CHAR_LENGTH || ')'
         WHEN DATA_TYPE IN ('RAW') THEN '(' || 2*DATA_LENGTH || ')'
         WHEN DATA_TYPE = 'NUMBER' THEN '(' || NEW_DATA_PRECISION || ',' || NEW_DATA_SCALE || ')'
         WHEN DATA_TYPE IN ('DATE','TIMESTAMP(6)') THEN 'TIMESTAMP_LTZ' 
    END,20,' ')||           
  /* Apply default values */
    CASE WHEN VIRTUAL_COLUMN='NO' AND DATA_DEFAULT IS NOT NULL THEN ' DEFAULT '||DATA_DEFAULT END ||
  /* Create Not null constraints */
    CASE WHEN NULLABLE='N' THEN ' NOT NULL ' END||
  /* Add column comments if available */
    CASE WHEN column_comments IS NOT NULL THEN ' COMMENT '''||COLUMN_COMMENTS||'''' END||         
    CASE WHEN COLUMN_ID!= MAX_COLUMN_ID THEN ',' ELSE 
  /* create end of table with table comment if available */
    CASE WHEN table_comment IS NOT NULL THEN ') COMMENT='''||TABLE_COMMENT||''';' ELSE ');' END END AS DDL_SQL-- Table end syntax
        -- Noformat End
FROM   (SELECT OWNER,
               TABLE_NAME,
               COLUMN_NAME,
               DATA_TYPE,
               DATA_LENGTH,
               CHAR_LENGTH,
               DATA_PRECISION,
               DATA_SCALE,
               COLUMN_ID,
                 -- Noformat start
                           CASE 
                            WHEN DATA_TYPE = 'NUMBER' AND DATA_PRECISION IS NULL 
                              THEN CASE WHEN DATA_SCALE IS NOT NULL THEN (38-DATA_SCALE) ELSE 28 END 
                              ELSE DATA_PRECISION 
                              END AS NEW_DATA_PRECISION,  
                           CASE 
                            WHEN DATA_TYPE = 'NUMBER' AND DATA_PRECISION IS NULL AND DATA_SCALE IS NULL 
                              THEN 10 
                              ELSE DATA_SCALE 
                              END AS NEW_DATA_SCALE,
                  -- Noformat End
               MAX(COLUMN_ID) OVER(PARTITION BY OWNER, TABLE_NAME) AS MAX_COLUMN_ID,
               DATA_DEFAULT AS DATA_DEFAULT,
               NULLABLE,
               VIRTUAL_COLUMN,
               TABLE_COMMENT,
               COLUMN_COMMENTS
        FROM   ORACLE_TABLE_COLUMNS_VW,
               XMLTABLE('/ROWSET/ROW' PASSING ORACLE_TABLE_COLUMNS_VW.XML_COL COLUMNS OWNER VARCHAR2(40) PATH 'OWNER'
                       ,TABLE_NAME VARCHAR2(40) PATH 'TABLE_NAME'
                       ,COLUMN_NAME VARCHAR2(40) PATH 'COLUMN_NAME'
                       ,DATA_TYPE VARCHAR2(50) PATH 'DATA_TYPE'
                       ,DATA_LENGTH NUMBER PATH 'DATA_LENGTH'
                       ,CHAR_LENGTH NUMBER PATH 'CHAR_LENGTH'
                       ,DATA_PRECISION NUMBER PATH 'DATA_PRECISION'
                       ,DATA_SCALE NUMBER PATH 'DATA_SCALE'
                       ,COLUMN_ID NUMBER PATH 'COLUMN_ID'
                       ,DATA_DEFAULT VARCHAR2(1000) PATH 'DATA_DEFAULT'
                       ,NULLABLE VARCHAR2(1) PATH 'NULLABLE'
                       ,VIRTUAL_COLUMN VARCHAR2(3) PATH 'VIRTUAL_COLUMN'
                       ,TABLE_COMMENT VARCHAR2(4000) PATH 'TABLE_COMMENT'
                       ,COLUMN_COMMENTS VARCHAR2(4000) PATH 'COLUMN_COMMENTS') XMLTAB)
ORDER  BY OWNER, TABLE_NAME, COLUMN_ID	
"""

# Oracle specific session configuration to eliminate specific DDL
FORMAT_BLOCK = """
BEGIN
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'STORAGE',FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'TABLESPACE',FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES', FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS', FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'REF_CONSTRAINTS', FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'CONSTRAINTS_AS_ALTER', TRUE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'OID', FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'SIZE_BYTE_KEYWORD', FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR', TRUE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',TRUE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'PARTITIONING' ,FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'PHYSICAL_PROPERTIES' ,FALSE);
       DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM,'REUSE' ,FALSE);
END;
"""

try:
    
    # Set the session configuration
    cursor_ddl.execute(FORMAT_BLOCK)
    # DDL for single object
    if (p_object_name != 'ALL'):
        object_count = 0
        cursor_objects.execute(DDL_OBJECTS_LIST_ALL(p_schema_name,"ALL",p_object_name))
        for db_objects in cursor_objects:
            object_count = object_count + 1
            object_type_count = 0
            for cur_object_type in DDL_OBJECT_TYPE_LIST:
                if (db_objects[1] == cur_object_type):
                    object_type_count = object_type_count + 1
                    print("Creating DDL for",db_objects[1],":",db_objects[0] + "."+ db_objects[2])
                    with open (db_objects[0] + '_' + db_objects[1] + '_'+ db_objects[2] + '.sql', 'w') as f:
                        if(db_objects[1] == 'TABLE'):
                            # get table DDl
                            cursor_ddl.execute(TABLE_DDL(db_objects[0],db_objects[2]))
                            for ddl_table_result in cursor_ddl:
                                f.write ("%s\n" %ddl_table_result[0].replace("SYSDATE","current_timestamp").replace("UPPER(SYS_CONTEXT('USERENV', 'OS_USER'))","current_user"))
                            # get primary key and unique constarints
                            cursor_ddl.execute(DDL_TABLE_PRIMARY_UNIQUE_KEYS(db_objects[0],db_objects[2]))
                            for ddl_PK_UK in cursor_ddl:
                                f.write ("%s\n" %ddl_PK_UK[0])                                                         
                            # get referential integrity constraints
                            cursor_ddl.execute(DDL_TABLE_REFERENTIAL_INTIGRITY_CONS(db_objects[0],db_objects[2]))
                            results = cursor_ddl.fetchall()
                            for ddl_result in results:
                                f.write ("%s\n" % str(ddl_result[0].read()).replace("\"" + db_objects[0] + "\".",""))
                        else:
                            # get DDL for objects other than table
                            cursor_ddl.execute(DDL_QUERY(db_objects[0],db_objects[1],db_objects[2]))
                            results = cursor_ddl.fetchall()
                            for ddl_result in results:
                                f.write ("%s\n" % str(ddl_result[0].read()).replace("\"" + db_objects[0] + "\".","").replace("EDITIONABLE","").replace("FORCE",""))
            if (object_type_count == 0):
                print(p_object_name,"is a",db_objects[1],", which is not listed as valid object type for DDL generation.")
                print ("Please modify \"DDL_OBJECT_TYPE_LIST\" list if you want to generate DDL for",db_objects[1])
        if (object_count == 0):
            print ("Warning : No object found by the name",p_object_name," in schema",p_schema_name,". Either provide valid object name or Say ALL to generate DDL for all objects in schema.")
    # DDL for all objects
    else:
        print("Generating DDL for all objects in schema : ",p_schema_name)
        print("\n")
        for cur_object_type in DDL_OBJECT_TYPE_LIST:
            print("Creating DDL for : ",cur_object_type,"...")
            with open (p_schema_name + '_' + cur_object_type + '.sql', 'w') as f:
                if(cur_object_type == 'TABLE'):
                    # get DDL for table
                    cursor_objects.execute(DDL_OBJECTS_LIST_ALL(p_schema_name, cur_object_type,'ALL'))
                    object_count = 0
                    for db_objects in cursor_objects:
                        if (object_count == 0):
                            f.write ("\n/* ************************************************************************************ */\n")
                            f.write ("/*Object owner = {}.*/".format(db_objects[0]))
                            f.write ("\n/* ************************************************************************************ */\n")
                        object_count = object_count + 1
                        f.write ("\n")
                        cursor_ddl.execute(TABLE_DDL(db_objects[0],db_objects[2]))
                        for ddl_table_result in cursor_ddl:
                            f.write ("%s\n" %ddl_table_result[0].replace("SYSDATE","current_timestamp").replace("UPPER(SYS_CONTEXT('USERENV', 'OS_USER'))","current_user"))
                        cursor_ddl.execute(DDL_TABLE_PRIMARY_UNIQUE_KEYS(db_objects[0],db_objects[2]))
                        for ddl_PK_UK in cursor_ddl:
                            f.write ("%s\n" %ddl_PK_UK[0])                                                         
                        cursor_ddl.execute(DDL_TABLE_REFERENTIAL_INTIGRITY_CONS(db_objects[0],db_objects[2]))
                        results = cursor_ddl.fetchall()
                        for ddl_result in results:
                            f.write ("%s\n" % str(ddl_result[0].read()).replace("\"" + db_objects[0] + "\".","").replace("EDITIONABLE","").replace("FORCE",""))
                else:
                    # get DDL for objects other than table
                    cursor_objects.execute(DDL_OBJECTS_LIST_ALL(p_schema_name, cur_object_type,"ALL"))
                    object_count = 0
                    for db_objects in cursor_objects:
                        if (object_count == 0):
                            f.write ("\n/* ************************************************************************************ */\n")
                            f.write ("/*Object owner = {}.*/".format(db_objects[0]))
                            f.write ("\n/* ************************************************************************************ */\n")
                        object_count = object_count + 1
                        f.write ("\n")
                        cursor_ddl.execute(DDL_QUERY(db_objects[0],db_objects[1],db_objects[2]))
                        results = cursor_ddl.fetchall()
                        for ddl_result in results:
                            f.write ("%s\n" % str(ddl_result[0].read()).replace("\"" + db_objects[0] + "\".","").replace("EDITIONABLE","").replace("FORCE",""))
                if object_count >0:
                    f.write ("\n /* This file has {} {} in this file.*/\n".format(str(object_count),str(cur_object_type)))

except cx_Oracle.DatabaseError as e:
    print("There is a problem with Oracle", e)
# Write any errors that occur
# close the all database operations and files
finally:
    if con:
        con.close()