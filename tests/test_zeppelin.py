import unittest
from zeppelin_api import ZeppelinAPI
import requests
from dotenv import load_dotenv
import os 

load_dotenv()

from utils.calculate import calculate_jip

ZEPPELIN_URL = os.getenv("ZEPPELIN_URL")
USERZEP = os.getenv("USERZEP")
PASSWORD = os.getenv("PASSWORD")

class TestZeppelinAPI(unittest.TestCase):
    
    def setUp(self):
        print(ZEPPELIN_URL)
        # Set up a ZeppelinAPI instance for testing
        self.zeppelin = ZeppelinAPI(ZEPPELIN_URL, USERZEP, PASSWORD)
        
        self.calculate = calculate_jip 
        

    def test_login(self):
        # Test the login method
        self.assertTrue(isinstance(self.zeppelin.session, requests.Session))

    # def test_get_status_notebook(self):
    #     self.zeppelin.get_status(note_id="2JJBY2WHJ")
    
    # def test_run_all_paragraft(self):
    #     self.zeppelin.run_all_paragraft(note_id="2JHR1TG3N")

    def test_create_notebook_uniq_name(self):
    # Test the create_notebook method
        script={
                    "name": "testing",
                    "paragraphs": [
                        {
                        "text": "This sample paragraft"
                        }
                    ]
                }
        
        self.assertEqual(self.zeppelin.create_notebook(script=script, uniq_name=True, delete_after_run=True)['status'], "OK")
    
    def test_create_notebook_execute_wait_delete(self):
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
        
        self.assertEqual(self.zeppelin.create_notebook(script=script, default_interpreter="python", uniq_name=True, run_all=True ,delete_after_run=True, check_status=True)['status'],"OK")
        
    def test_script_with_parameter(self):
        machine_name = "OPJ"
        as_of_week = 1234
        
        result = calculate_jip(machine_name=machine_name, as_of_week=as_of_week)['paragraphs'][0]['text']
        
        self.assertTrue('OPJ' in result)
        
    def test_create_run_delete_script(self):
        script_test = {
                    "name": "sample_testing",
                    "defaultInterpreterGroup": "python",
                    "paragraphs": [
                        {
                        "title": "Testing Wait",
                        "text": "%python\nimport time\n\ntime.sleep(5)"
                        }
                    ]
                }
        
        response = self.zeppelin.create_notebook(script=script_test, uniq_name=True, run_all=True, check_status=True) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
        print(response['body'])
        
        self.zeppelin.delete_note(note_id=response['body'], thread_status=True)
        
    def test_delete_all_notebook(self):
        print(self.zeppelin.delete_note(delete_all=True))
        
    def test_list_notebook(self):
        print(self.zeppelin.list_notebook())
        
    def test_search_notebook(self):
        key_search = "calculate"
        print(self.zeppelin.search_notebook(search_text=key_search))
    
    def test_check_create_notebook(self):
        
        key_search = "calculate"
        
        check_note = self.zeppelin.search_notebook(search_text=key_search) # {'id': '2JJEGXX4B', 'path': '/calculate'} or {'status': 'Not found', 'message': 'Notebook calculate not found'}
        
        if check_note is None: 
            script_test = {
                    "name": "calculate",
                    "defaultInterpreterGroup": "python",
                    "paragraphs": [
                        {
                        "title": "Testing Wait",
                        "text": "%python\nimport time\n\ntime.sleep(5)"
                        }
                    ]
                }
            response = self.zeppelin.create_notebook(script=script_test, uniq_name=False, run_all=False, check_status=False, notebook_name=key_search) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
            print(response)
        else: 
            response = self.zeppelin.run_all_paragraft(note_id=str(check_note['id']))
            print(response)
    
    def test_response_error_notebook(self):
        
        script_test = {
                "name": "calculate ",
                "defaultInterpreterGroup": "python",
                "paragraphs": [
                    {
                    "title": "Testing Wait",
                    "text": "%python\nimport time\n\ntime.sleep(2)"
                    },
                    {
                    "title": "Testing Wait",
                    "text": "%python\nimport time\n\ntime.sleep(2)"
                    }
                ]
            }
        
        response = self.zeppelin.create_notebook(script=script_test, uniq_name=True, run_all=True, check_status=True ) # out : {'status': 'OK', 'message': '', 'body': '2JH177N4Y'}
        
        print(str(response))
            
if __name__ == '__main__':
    unittest.main()
