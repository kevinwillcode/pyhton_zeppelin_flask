import pyodbc as pyodbc
from flask import Flask, jsonify
import os

from zeppelin_api import ZeppelinAPI

ZEPPELIN_URL = os.getenv("ZEPPELIN_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

app = Flask(__name__)

zeppelin = ZeppelinAPI(base_url=ZEPPELIN_URL, username=USERNAME, password=PASSWORD)

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.route('/healty')
def check():
    return jsonify("healty")

@app.route("/calculate")
def calculate_jip():
    script={
                    "paragraphs": [
                        {
                        "text": "%python\nimport time\n\ntime.sleep(10)"
                        },
                        {
                        "text": "%python\nimport time\n\ntime.sleep(10)"
                        },
                    ]
                }
     
    zeppelin.create_notebook(script=script, default_interpreter="python", uniq_name=True, run_all=True ,delete_after_run=True, check_status=True)
    
    return jsonify("Done")
          
if __name__ == "__main__":
    app.run(debug=True)
