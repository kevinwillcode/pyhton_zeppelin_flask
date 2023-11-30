import unittest
from zeppelin_api import ZeppelinAPI
import requests

class TestZeppelinAPI(unittest.TestCase):
    
    def setUp(self):
        # Set up a ZeppelinAPI instance for testing
        self.zeppelin = ZeppelinAPI("http://127.0.0.1:8080", "admin", "admin")

    def test_login(self):
        # Test the login method
        self.assertTrue(isinstance(self.zeppelin.session, requests.Session))

    def test_get_status_notebook(self):
        self.zeppelin.get_status(note_id="2JJBY2WHJ")
    
    def test_run_all_paragraft(self):
        self.zeppelin.run_all_paragraft(note_id="2JHR1TG3N")

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
        
if __name__ == '__main__':
    unittest.main()
