from dotenv import load_dotenv
from flask import Flask, jsonify, request
import logging
import os
from threading import Thread

from zeppelin_api import ZeppelinAPI
from utils.calculate import calculate_jip, combine_notebook, sample_notebook

ZEPPELIN_URL = os.getenv("ZEPPELIN_URL")
USERZEP = os.getenv("USERZEP")
PASSWORD = os.getenv("PASSWORD")

# Check if any of the required environment variables is not set
if ZEPPELIN_URL is None or USERZEP is None or PASSWORD is None:
    raise ValueError("One or more required environment variables are not set.")

app = Flask(__name__)

zeppelin = ZeppelinAPI(base_url=ZEPPELIN_URL, username=USERZEP, password=PASSWORD)

# Cache
status_calculate = {
            "running": False,
            "message": "",
            "as_of_week": "",
            "machine": ""
        }

@app.route('/')
def hello_geek():
    return '<h2>This API for calculate_jip</h2>'

@app.route("/test_notebook", methods=['GET'])
def test_notebook():
    script={
                "paragraphs": [
                        {
                        "text": "%python\nimport time\n\ntime.sleep(3)"
                        },
                    ]
                }
     
    response = zeppelin.create_notebook(script=script, default_interpreter="python", uniq_name=True, run_all=True, check_status=True)
    
    return jsonify(response)

@app.route("/notebook", methods=['POST'])
def notebook():

    try:
        data = request.json
        
        logging.debug(data)
        # Assuming you have initialized the 'zeppelin' object properly
        response = zeppelin.create_notebook(
            script=data,
            uniq_name=bool(request.args.get('uniqName')),
            run_all=bool(request.args.get('runAll')),
            check_status=bool(request.args.get('checkStatus')),
            delete_after_run=bool(request.args.get('deleteAfterRun'))
        )
        # Assuming you want to return the response from Zeppelin API
        return jsonify(response)
    
    except Exception as e:
        # Log the exception for debugging purposes
        logging.exception("An error occurred: %s", str(e))
        # You may want to return an appropriate error response to the client
        return jsonify(error=f"An error occurred {str(e)}"), 500
    
def _combine_task(script):
# Combine result Calculate
    note_name = "calculate"
    check_note = zeppelin.search_notebook(search_text=note_name) # {'id': '2JJEGXX4B', 'path': '/calculate'} or None
    
    if check_note is None:             
        logging.debug("[CALCULATE JIP] - create notebook")
        zeppelin.create_notebook(script=script, uniq_name=False, run_all=True, check_status=False, notebook_name=note_name) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
        
    else: 
        # Running Notebook for combine data machine
        logging.debug(f"[CALCULATE JIP] - Notebook '{note_name}' found !")
        zeppelin.run_all_paragraft(note_id=str(check_note['id']))
        logging.debug(f"[CALCULATE JIP] - Running notebook '{note_name}' Success")

@app.route("/calculate_jip", methods=['POST','GET'])
def calculate_jip_execute():
    machine_name = request.args.get("machineName") # OPJ
    as_of_week = request.args.get("asOfWeek") # 2367
    uniq_name = request.args.get("uniqName") # "sample_notebook"
    
    script_calculate = calculate_jip(machine_name=machine_name, as_of_week=int(as_of_week))
    script_combine = combine_notebook()
    
    # Sample Notebook
    # script_sample = sample_notebook()
    
    try:
        
        if status_calculate["running"] == False:
            status_calculate["running"] = True
            status_calculate["machine"] = machine_name
            status_calculate["as_of_week"] = as_of_week
            status_calculate["message"] = "Calculation still running please wait..."
            logging.debug(f"Calculation {str(status_calculate['machine'])} still running please wait...")
        elif status_calculate["running"] == True:
            return status_calculate
            
        # Create Notebook calculate
        zeppelin.create_notebook(
            script = script_calculate,
            run_all=bool(request.args.get('runAll')),
            check_status=bool(request.args.get('checkStatus')),
            uniq_name=bool(True)
        ) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
        
        thread = Thread(target=_combine_task(script_combine))
        thread.start()
        thread.join()
        
        # Change calculation 
        status_calculate = {
                    "running": False,
                    "message": "",
                    "as_of_week": "",
                    "machine": ""
                }
        
        return {"status" : "Done", 
                "calculate" : {
                    "as_of_week" : as_of_week,
                    "machine" : machine_name
                    },
                "message": f"Calculate has been execute"}

    except Exception as e:
        # Log the exception for debugging purposes
        logging.exception("An error occurred: %s", str(e))
        # You may want to return an appropriate error response to the client
        return jsonify(error=f"An error occurred {str(e)}"), 500
    
@app.route("/delete-all-notebook", method=["GET"])
def delete_all_notebook():
    zeppelin.delete_note(delete_all=True)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
