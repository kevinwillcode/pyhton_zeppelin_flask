from flask import Flask, jsonify, request
import logging
import os

# from multiprocessing import Process
# from concurrent.futures import ThreadPoolExecutor


from zeppelin_api import ZeppelinAPI

ZEPPELIN_URL = os.getenv("ZEPPELIN_URL")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Check if any of the required environment variables is not set
if ZEPPELIN_URL is None or USERNAME is None or PASSWORD is None:
    raise ValueError("One or more required environment variables are not set.")

app = Flask(__name__)
# executor = ThreadPoolExecutor()

zeppelin = ZeppelinAPI(base_url=ZEPPELIN_URL, username=USERNAME, password=PASSWORD)

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
def calculate_jip():
    print("Test")
        
    
    
    
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
