from dotenv import load_dotenv
from flask import Flask, jsonify, request
import logging
import os


from zeppelin_api import ZeppelinAPI
from utils.calculate import calculate_jip, combine_notebook

ZEPPELIN_URL = os.getenv("ZEPPELIN_URL")
USERZEP = os.getenv("USERZEP")
PASSWORD = os.getenv("PASSWORD")

# Check if any of the required environment variables is not set
if ZEPPELIN_URL is None or USERZEP is None or PASSWORD is None:
    raise ValueError("One or more required environment variables are not set.")

app = Flask(__name__)
# executor = ThreadPoolExecutor()

zeppelin = ZeppelinAPI(base_url=ZEPPELIN_URL, username=USERZEP, password=PASSWORD)

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

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

@app.route("/calculate_jip", methods=['POST'])
def calculate_jip_execute():
    machine_name = request.args.get("machineName")
    as_of_week = request.args.get("asOfWeek")
    note_code = request.args.get("noteCode")
    uniq_name = request.args.get("uniqName")
    
    script_calculate = calculate_jip(machine_name=machine_name, as_of_week=int(as_of_week))
    # script_combine = combine_notebook()
    
    try:
        # script_test = {
        #             "name": "sample_testing",
        #             "defaultInterpreterGroup": "python",
        #             "paragraphs": [
        #                 {
        #                 "title": "Testing Wait",
        #                 "text": "%python\nimport time\n\ntime.sleep(1)"
        #                 }
        #             ]
        #         }
        
        zeppelin.create_notebook(
            script = script_calculate,
            run_all=bool(request.args.get('runAll')),
            check_status=bool(request.args.get('checkStatus')),
            uniq_name=bool(False)
        ) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
        
        # Delete Notebook
        # logging.debug('Delete notebook in background process')
        # zeppelin.delete_note(note_id=response['body'], background_process=True)
        # logging.debug('Delete notebook in background process Success')
        
        # Running Notebook for combine data machine
        # zeppelin.create_notebook(note_id=script_combine, background_process=True)
        zeppelin.run_all_paragraft(note_id="2JGUTCS7J", background_process=True)
        # logging.debug('Running notebook Success')
        
        # return the response from Zeppelin API
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
    
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
