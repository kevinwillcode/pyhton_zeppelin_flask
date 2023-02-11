import json
import os
import requests
import pyodbc as pyodbc
from sys import stderr
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)

api_key = os.environ.get("API_KEY", "")
if api_key == "":
    print("api key is required", file=stderr)

api_base_url = "https://api.stagingv3.microgen.id/query/api/v1/" + api_key

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'
@app.route('/check')
def check():
    drivers = [item for item in pyodbc.drivers()]
    
    return jsonify(drivers)

def get_cookie(username, userpassword):
    try:
        url = 'https://ipqlftmgzk.function.microgen.id/api/login'
        response1 = requests.post(url,data={'username': username , 'password': userpassword})
        source = str(response1.json()["Set-Cookie"])
        print("add source")
        return source
    except:
        print("error get cookies")
        return jsonify({"msg": "Missing Authorization"}), 400 

def create_notebook(name,source,hostname,port,sqlDB,user,password,database,table,existingTable,query=None,sqlTable=None):
    print("add notebook")
    try:
        url1 = 'https://ipqlftmgzk.function.microgen.id/api/createnote?'+source+''
        response2 = requests.post(url1,json={"name":str(name)})
        data = response2.json()
        sdata = str(data["body"])
        print(sdata)
        create_note(source,sdata,hostname,port,sqlDB,user,password,database,table,existingTable,query,sqlTable)
        return sdata
    except:
        print("error add notebook")
        return jsonify({"msg": "error add notebook"}), 400 

def create_note(source,sdata,hostname,port,sqlDB,user,password,database,table,existingTable,query=None,sqlTable=None):
    
    if query != "":
        if ";" in query:
            query = query.replace(";","")
        query = " ".join(line.strip() for line in query.splitlines())
        print(query)
        sqlTable = "("+query+")tmp"
        
    if existingTable == False:
        try:
            print("add note")
            text = "%md Connector to SQL Server"
            text1 = "%spark2.pyspark\nfrom pyspark_llap import HiveWarehouseSession\n\
hive = HiveWarehouseSession.session(spark).build()\n\
df_load = spark.read.format('jdbc').option('url', 'jdbc:sqlserver://"+hostname+":"+port+";database="+sqlDB+"')\
.option('dbtable','"+sqlTable+"')\
.option('user','"+user+"')\
.option('password','"+password+"').load()\n\
df_load.show()"
            url3 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'?'+source+''
            responseget = requests.get(url3)
            data = responseget.json()
            paragraphid = str(data["body"]["paragraphs"][0]["id"])
            url1 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'/paragraph/'+paragraphid+'?'+source+''
            responseput = requests.put(url1,json={"text":str(text)})
            
            url2 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'/paragraph?'+source+'' 
            text2="%spark2.pyspark\n#df_load.write.format(HiveWarehouseSession().HIVE_WAREHOUSE_CONNECTOR).mode('overwrite')\
.option('table','"+database+"."+table+"').save()\n"
            requests.post(url2,json={"title": "Paragraph insert revised","text":text1 })
            requests.post(url2,json={"title": "Paragraph insert revised","text":text2 })
            print(responseget.json()["id"])
            return responseput.json()["id"]
        except Exception as e:
            print(e)
            return 'error note'
    elif existingTable == True:
        try:
            print("add note")
            text = "%md Connector to SQL Server"
            text1 = "%spark2.pyspark\nfrom pyspark_llap import HiveWarehouseSession\n\
hive = HiveWarehouseSession.session(spark).build()\n\
df_load = spark.read.format('jdbc').option('url', 'jdbc:sqlserver://"+hostname+":"+port+";database="+sqlDB+"')\
.option('dbtable','"+sqlTable+"')\
.option('user','"+user+"')\
.option('password','"+password+"').load()\n\
df_load.show()"
            url3 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'?'+source+''
            responseget = requests.get(url3)
            data = responseget.json()
            paragraphid = str(data["body"]["paragraphs"][0]["id"])
            url1 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'/paragraph/'+paragraphid+'?'+source+''
            responseput = requests.put(url1,json={"text":str(text)})
            
            url2 = 'https://ipqlftmgzk.function.microgen.id/api/notebook/'+sdata+'/paragraph?'+source+'' 
            text2="%spark2.pyspark\n#df_load.write.format(HiveWarehouseSession().HIVE_WAREHOUSE_CONNECTOR).mode('append')\
.option('table','"+database+"."+table+"').save()\n"
            requests.post(url2,json={"title": "Paragraph insert revised","text":text1 })
            requests.post(url2,json={"title": "Paragraph insert revised","text":text2 })
            print(responseget.json()["id"])
            return responseput.json()["id"]
        except Exception as e:
            print(e)
            return 'error note'   
    
@app.post("/api/import/sqlServer")
def sourcecode():
    try:
        request_data = request.get_json()
        username = request_data['username']
        userpassword = request_data['userpassword']
        hostname = request_data['hostname']
        port = request_data['port']
        sqlDB = request_data['sqlDB']
        sqlTable = request_data['sqlTable']
        user = request_data['user']
        password = request_data['password']
        database = request_data['database']
        table = request_data['table']
        name = request_data['name']
        existingTable = request_data['existingTable']
        query = request_data['query']
        source = get_cookie(username, userpassword)
        id = create_notebook(name,source,hostname,port,sqlDB,user,password,database,table,existingTable,query,sqlTable)
        return jsonify({"statusCode":200,"status":"success","id":id})

    except Exception as e:
        print(e)
        return 'error'
    

@app.post("/api/import/mssqlServer/showDB")
def show_db():
    try:
        request_data = request.get_json()
        hostname = request_data['hostname']
        port = request_data['port']
        username = request_data['username']
        password = request_data['password']
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+hostname+','+port+';UID='+username+';PWD='+password+';')
        cursor=connection.cursor()
        cursor.execute("SELECT NAME FROM sys.databases")
        databasesDetails = dict()
        databasesDetails['items']={}
        count = 0
        for row in cursor: 
            databasesDetails['items'][count] = row.NAME
            count+=1
        
        cursor.close()
        connection.close()
        return databasesDetails
    except pyodbc.Error as ex:
            errorJSON=[]
            errorJSON.append(ex.args[1])
            return jsonify({"Message":errorJSON})
@app.post("/api/import/mssqlServer/showTB")
def show_table():
    try:
        request_data = request.get_json()
        hostname = request_data['hostname']
        port = request_data['port']
        username = request_data['username']
        password = request_data['password']
        database = request_data['database']
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+hostname+','+port+';Database='+database+';UID='+username+';PWD='+password+';')
        cursor=connection.cursor()
        cursor.execute("SELECT TABLE_NAME from information_schema.tables")
        tablesDetails = dict()
        tablesDetails['items']={}
        count = 0
        for row in cursor:
            tablesDetails['items'][count] = row.TABLE_NAME
            count+=1
        cursor.close()
        connection.close()
        return tablesDetails 
    except pyodbc.Error as ex:
            errorJSON=[]
            errorJSON.append(ex.args[1])
            return jsonify({"Message":errorJSON})

@app.post("/api/import/mssqlServer/previewTable")
def preview_table():
    try:
        request_data = request.get_json()
        hostname = request_data['hostname']
        port = request_data['port']
        username = request_data['username']
        password = request_data['password']
        database = request_data['database']
        tablename = request_data['tablename']
        connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+hostname+','+port+';Database='+database+';UID='+username+';PWD='+password+';')
        cursor=connection.cursor()
        #cursor.execute('SELECT count(*) from '+tablename+'')
        cursor.execute('SELECT TOP 20 * from '+tablename+'')
        columns = [column[0] for column in cursor.description]

        tablesDetails=[]
        #tablesDetails.append(columns)
        #count = 0
        for row in cursor.fetchall():
            tablesDetails.append(dict(zip(columns, row)))
        #   count+=1
        #tablesDetails = [row for row in cursor]
        cursor.close()
        connection.close()
        return jsonify(tablesDetails)
    except pyodbc.Error as ex:
            errorJSON=[]
            errorJSON.append(ex.args[1])
            return jsonify({"Message":errorJSON})
        
@app.post("/api/import/mssqlServer/query")
def mssqlQuery():
    request_data = request.get_json()
    hostname = request_data['hostname']
    port = request_data['port']
    username = request_data['username']
    password = request_data['password']
    database = request_data['database']
    query = request_data['query']
    hiveQueries = [
        "USE",
        "SELECT",
        "INSERT",
        "CREATE",
        "DROP",
        "ALTER",
        "DESCRIBE",
        "SHOW",
        "LOAD",
        "WITH",
    ]
    #query = 'SELECT TOP (20) * from '+tablename+' where age>20'

    for i in range(len(hiveQueries)): 
        if (query.upper().startswith(hiveQueries[i])):
            tempQuery = hiveQueries[i]
            break
        
    if "LIMIT" in query.upper():
        return jsonify({"Message":"MSSQL does not support LIMIT"})
    if ";" in query:
        query = query.replace(";","")
    if "\n" in query:
        query = query.replace("\n","") 
        
    if (tempQuery=="USE"):
        return jsonify({"Message":"USE is not supported"})
    elif (tempQuery=="SELECT"):
        try:
            query = query.upper()
            query = query.replace("SELECT","SELECT TOP (20)")
            connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:'+hostname+','+port+';Database='+database+';UID='+username+';PWD='+password+';')
            cursor=connection.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            tablesDetails=[]
            for row in cursor.fetchall():
                tablesDetails.append(dict(zip(columns, row)))
            cursor.close()
            connection.close()    
            return jsonify(tablesDetails)
                
        except pyodbc.Error as ex:
            errorJSON=[]
            errorJSON.append(ex.args[1])
            return jsonify({"Message":errorJSON})
    elif (tempQuery=="INSERT"):
        message = "INSERT is not supported"
        return jsonify({"Message":message})
    elif (tempQuery=="CREATE"):
        message = "CREATE is not supported"
        return jsonify({"Message":message})
    elif (tempQuery=="DROP"):
        message = "DROP is not supported"
        return jsonify({"Message":message})
    elif (tempQuery=="ALTER"):
        message = "ALTER is not yet supported"
        return jsonify({"Message":message})
    elif (tempQuery=="DESCRIBE"):
        message = "MSSQL does not support DESCRIBE" 
        return jsonify({"Message":message})
    elif (tempQuery=="SHOW"):
        message ="MSSQL does not support SHOW" 
        return jsonify({"Message":message})  
    elif (tempQuery=="LOAD"):
        message ="MSSQL does not support LOAD"
        return jsonify({"Message":message})
    elif (tempQuery=="WITH"):
        message ="MSSQL does not support WITH"
        return jsonify({"Message":message})
    else :
        message = "ERROR"
        return jsonify({"Message":message})
            
if __name__ == "__main__":
    app.run(debug=True)
