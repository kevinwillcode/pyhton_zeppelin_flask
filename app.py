import json
import os
import requests
import pyodbc as pyodbc
from sys import stderr
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
zep_url = "https://10.206.26.42:9995"

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.route('/healty')
def check():
    
    return jsonify("healty")

@app.route("/get-data")
async def execute_simulation(machine_name=None):
    data = await print("test")
    return jsonify(data)
          
if __name__ == "__main__":
    app.run(debug=True)
