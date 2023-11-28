
import requests
import json
import uuid
import time
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set the logging level
                    format='%(asctime)s - %(levelname)s - %(message)s')  # Define the log format


from flask import jsonify

class ZeppelinAPI:
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
            print(self.session.cookies.get_dict())
            login_url = f"{self.base_url}/api/login"
            login_payload = {"username": self.username, "password": self.password}
            
            response = self.session.post(login_url, data=login_payload, verify=False)
            response.raise_for_status()

            # Move this line below the raise_for_status() to ensure it's only executed for successful requests
            self.__private_session_id = self.session.cookies.get_dict()
            
            # Use logging instead of print
            logging.info("Login Success")
            logging.debug(f"Session ID: {self.__private_session_id}")
            
        except requests.HTTPError as http_err:
            logging.error(f"HTTP error occurred: {http_err}")
        except requests.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
        except Exception as err:
            logging.error(f"An unexpected error occurred: {err}")

    def run_all_paragraft(self, note_id: str=None):
        if(note_id == None):
            raise ValueError("Error: 'note_id' not found")
            
        try:
            execute_all_url = f"{self.base_url}/api/notebook/job/{note_id}" 
            response = requests.post(execute_all_url, cookies=self.__private_session_id, timeout=10).json()
            print(response)
            
        except Exception as err:
            print(f"An unexpected error occurred: {err}")
        
    def create_notebook(self, script: dict=None, default_interpreter="spark", run_all=False, uniq_name=False, delete_after_run=False):
        
        if(uniq_name or script['name']==None):
            unique_id = str(uuid.uuid4())
            script['name'] = unique_id
            logging.debug(f"Name notebook change to :'{unique_id}'")
    
        if "paragraphs" not in script:
            raise ValueError("Error: 'paragraphs' not found in script.") 
    
        try:
            create_notebook_url = f"{self.base_url}/api/notebook" 
            response = requests.post(create_notebook_url, cookies=self.__private_session_id , json=script, timeout=10)
            response.raise_for_status()
            
            logging.info(f"notebook '{script['name']}' success create!")
            
            # same id notebook
            body = response.json()['body']
            self.note_id = body
                
            if run_all:
                self.run_all_paragraft(note_id=self.note_id)
                
            if delete_after_run:
                self.delete_note(note_id=self.note_id)

        except requests.HTTPError as http_err:
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

    def get_status(self, note_id=None, interval=1, max_attempts=100000):
        if(note_id == None):
            logging.error()
            raise ValueError("Error: 'note_id' not found")
        
        url_base = f"{self.base_url}/api/notebook/job/{note_id}" 
        
        attempts = 0
        while attempts < max_attempts:
            try:
                response = self.session.get(url_base, cookies=self.__private_session_id, timeout=10)
                response.raise_for_status()  # Raises an HTTPError for bad responses
                # print(response.json())  # Assuming the response is in JSON format
                data = response.json()
                paragraphs = data["body"]["paragraphs"]
                pending_paragraphs = [paragraph for paragraph in paragraphs if paragraph["status"] == "PENDING"] # out: array[{'id': 'paragraph_1700720673085_705953182', 'status': 'PENDING', 'started': 'Thu Nov 23 06:33:00 GMT 2023', 'finished': 'Thu Nov 23 06:28:29 GMT 2023', 'progress': '0'}]
                
                if len(pending_paragraphs) == 0:
                    break
                
            except requests.HTTPError as http_err:
                logging.error(f"HTTP error occurred: {http_err}")
                raise ValueError(f"HTTP error occurred: {http_err}")
            
            except requests.RequestException as req_err:
                logging.error(f"Request error occurred: {req_err}")
                raise ValueError(f"Request error occurred: {req_err}")
            
            except Exception as err:
                logging.error(f"An unexpected error occurred: {err}")
                raise ValueError(f"An unexpected error occurred: {err}")
                
            attempts += 1
            
            time.sleep(interval)
            
        return {"status":"Execute All Paragraft Done"}
    
    def delete_note(self, note_id=None):
        
        if(note_id == None):
            raise ValueError("Error: 'note_id' not found")
        
        url_base = f"{self.base_url}/api/notebook/{note_id}" 
        
        try:
            response = self.session.delete(url_base, cookies=self.__private_session_id, timeout=10)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            print(response.json())
            return response.json() # out: {"status": "OK","message": ""}
        
        except requests.exceptions.HTTPError as http_err:
            return f"HTTP error occurred: {http_err}"
            print(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            return f"Request error occurred: {req_err}"
            print(f"Request error occurred: {req_err}")
            
        except Exception as err:
            return f"An unexpected error occurred: {err}"
            print(f"An unexpected error occurred: {err}")
        


zeppelin = ZeppelinAPI("http://127.0.0.1:8080", "admin", "admin")