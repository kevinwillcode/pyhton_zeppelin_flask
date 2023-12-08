import requests
import uuid
import time
import logging
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Define the log format

class ZeppelinAPI():
    def __init__(self, base_url, username, password):
        self.__private_session_id = None
        self.base_url = base_url
        self.username = username
        self.password = password
        self.note_id = None
        self.session = requests.Session()
        self.login()
        
    def login(self):
        try:
            self.session.cookies.clear()
            login_url = f"{self.base_url}/api/login"
            login_payload = {"username": self.username, "password": self.password}
            logging.debug("Payload : "+ str(login_payload))
            
            response = self.session.post(login_url, data=login_payload, verify=False)
            response.raise_for_status()

            # Move this line below the raise_for_status() to ensure it's only executed for successful requests
            self.__private_session_id = self.session.cookies.get_dict()
            
            # Use logging instead of print
            logging.info(f"Zeppelin at '{self.base_url}' connected! 🎉")
            logging.debug(f"Session ID: {self.__private_session_id}")
            
        except requests.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err} ❌")
        except requests.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err} ❌")
        except Exception as err:
            logging.error(f"An unexpected error occurred: {err} ❌")

    def run_all_paragraft(self, note_id: str=None, background_process=False ):
        execute_all_url = f"{self.base_url}/api/notebook/job/{note_id}" 
        
        if(note_id == None):
            raise ValueError("Error: 'note_id' not found 🔍🤔")
        
        try:
            if background_process is True : 
                logging.debug("Run all Notebook in background process")
                def _hit_api():
                    self.session.post(execute_all_url, cookies=self.__private_session_id, timeout=10, verify=False)
                
                thread = Thread(target=_hit_api)
                thread.start()
                return {"status": "Run all notebook execute in background process"}
            else: 
                response = self.session.post(execute_all_url, cookies=self.__private_session_id, timeout=10, verify=False)
                logging.info(f"Run all paragraft at notebook ID : {note_id} 💨")
                return response.json()
            
            
        except Exception as err:
            print(f"An unexpected error occurred: {err} ❌")          

    def get_status(self, note_id=None, interval=0.8, max_attempts=10000000, unlimited_attempts=False):
        if(note_id == None):
            logging.error("Error: 'note_id' not found 🔍🤔")
            raise ValueError("Error: 'note_id' not found 🔍🤔")
        
        url_base = f"{self.base_url}/api/notebook/job/{note_id}" 
        
        attempts = 0
        while attempts < max_attempts:
            try:
                response = self.session.get(url_base, cookies=self.__private_session_id, timeout=10, verify=False)
                response.raise_for_status()  # Raises an HTTPError for bad responses
                # print(response.json())  # Assuming the response is in JSON format
                data = response.json()
                paragraphs = data["body"]["paragraphs"]
                
                pending_paragraphs = [paragraph for paragraph in paragraphs if (paragraph["status"] == "RUNNING" ) or (paragraph["status"] == "PENDING" )] # out: array[{'id': 'paragraph_1700720673085_705953182', 'status': 'PENDING', 'started': 'Thu Nov 23 06:33:00 GMT 2023', 'finished': 'Thu Nov 23 06:28:29 GMT 2023', 'progress': '0'}] 
                
                # Check Error 
                error_paragraphs = [paragraph for paragraph in paragraphs if (paragraph["status"] == "ERROR" )] 
                
                if len(error_paragraphs) > 0 : 
                    logging.debug("Error Running Notebook ❌") 
                    return {"status": "Error running notebook", "message": "There is an error in your code. Please check the notebook or contact the developer. 👨‍💻⚒️"}
                
                logging.debug("Notebook stiLl running...")
                
                if len(pending_paragraphs) == 0:
                    logging.debug("Notebook Running Success ✅")
                    return {"status": "Success running notebook", "message": ""}
                
            except requests.HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err} ❌")
                raise ValueError(f"HTTP error occurred: {http_err} ❌")
            
            except requests.RequestException as req_err:
                logging.error(f"Request error occurred: {req_err} ❌")
                raise ValueError(f"Request error occurred: {req_err} ❌")
            
            except Exception as err:
                logging.error(f"An unexpected error occurred: {err} ❌")
                raise ValueError(f"An unexpected error occurred: {err} ❌")
                
            attempts += 1
            
            if unlimited_attempts is True:
                max_attempts += 1
            
            time.sleep(interval)
            
        return {"status":"Error", "message": f"notebook running to long, please check your notebook at ID: {note_id}, or contact developer"}
    
    def delete_note(self, note_id=None, delete_all=False, background_process=False):
        
        if((note_id == None) and delete_all == False ):
            logging.error("Error: 'note_id' not found 🔍🤔")
            raise ValueError("Error: 'note_id' not found 🔍🤔")
        
        url_base = f"{self.base_url}/api/notebook/" 
        
        try:
            if background_process is True : 
                logging.debug("Delete Notebook in background process")
                def _hit_api():
                    self.session.delete(url_base, cookies=self.__private_session_id, timeout=10, verify=False)
                
                thread = Thread(target=_hit_api)
                thread.start()
                
                return {"status": f"Delete notebook success", "message": "Delete notebook ID :'{note_id}'"}
                
            elif (delete_all is True) : 
                
                logging.warning("All Notebook Will be Delete! 😱")
                
                url_list_notebook = f"{self.base_url}/api/notebook"
                list_notebook = self.session.get(url_list_notebook, cookies=self.__private_session_id, timeout=10, verify=False)
                
                for note in list_notebook.json()['body']:
                    print(url_base+note['id'])
                    logging.debug(f"Delete Notebook ID {note['id']}")
                    response = self.session.delete(str(url_base+note['id']), cookies=self.__private_session_id, timeout=10, verify=False)
                    logging.debug(f"Delete Notebook ID {note['id']} Success ✅")
                    response.raise_for_status()  # Raises an HTTPError for bad responses
                
                    if response.status_code == 200:
                        logging.info(response.json())  # out: {"status": "OK","message": ""}
                    else:
                        logging.error(str({"status": "Error Delete Notebook", "message": f"Unexpected status code: {response.status_code}"})) 
                        break 
                
                
            else: 
                logging.debug(f"Delete Notebook ID {note_id}")
                response = self.session.delete(url_base+note_id, cookies=self.__private_session_id, timeout=10, verify=False)
                logging.debug(f"Delete Notebook ID {note_id} Success ✅")
                response.raise_for_status()  # Raises an HTTPError for bad responses
            
                if response.status_code == 200:
                    return response.json() # out: {"status": "OK","message": ""}
                else:
                    return {"status": "Error Delete Notebook", "message": f"Unexpected status code: {response.status_code}"}
                
            
        
        except requests.exceptions.HTTPError as http_err:
            raise ValueError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            raise ValueError(f"Request error occurred: {req_err}")
        except Exception as err:
            raise ValueError(f"An unexpected error occurred: {err}")
        
    def create_notebook(self, script: dict=None, default_interpreter="spark", uniq_name=False, run_all=False, delete_after_run=False, check_status=False):
        
        if script==None:
            logging.error("Script not found")
            raise ValueError("Script not found, are you add script?🤔")
        
        # Validate name notebook
        if(uniq_name or 'name' not in script):
            unique_id = str(uuid.uuid4())
            script['name'] = unique_id
            logging.debug(f"Name notebook change to :'{unique_id}'")
            
        # Validate Paragraft must be add
        if "paragraphs" not in script:
            logging.error("Error: key 'paragraphs' not found in script.")
            raise ValueError("Error: key 'paragraphs' not found in script.") 
    
        # Validate default_interpreter
        if "defaultInterpreterGroup" not in script:
            logging.info("interpreter not set, default interpreter will be set")
            script["defaultInterpreterGroup"] = default_interpreter
            
        try:
            logging.info("Creating Notebook...")
            
            create_notebook_url = f"{self.base_url}/api/notebook" 
            response = self.session.post(create_notebook_url, cookies=self.__private_session_id , json=script, timeout=10)
            response.raise_for_status()
            
            logging.info(f"Notebook name : '{script['name']}' success create!")

            # save id_notebook
            body = response.json()['body']
            self.note_id = body
            
            logging.debug(f"Notebook ID Zeppein: {self.note_id}")
                
            if (run_all and self.note_id is not None):
                # Run All Notebook
                self.run_all_paragraft(note_id=self.note_id)
                logging.debug(f"Notebook ID {self.note_id} Triggered to run all!")
                
                if check_status:
                    logging.info("Get Status Notebook...")
                    self.get_status(self.note_id)
                
                if (delete_after_run):
                    # Delete Notebook
                    self.delete_note(note_id=self.note_id)

            return response.json()

        except requests.HTTPError:
            error_message = response.json()['message']
            logging.error(f"HTTP error occurred: {error_message}")
            raise ValueError(f"HTTP error occurred: {error_message}")
            # print(f"HTTP error occurred: {http_err}")
        except requests.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
            raise ValueError(f"Request error occurred: {req_err}")
            # print(f"Request error occurred: {req_err}")
        except Exception as err:
            logging.error(f"An unexpected error occurred: {err}")
            raise ValueError(f"An unexpected error occurred: {err}")
            # print(f"An unexpected error occurred: {err}")
