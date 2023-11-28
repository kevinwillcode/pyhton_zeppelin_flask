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

    def tearDown(self):
        print("Running tearDown method...")

    def test_create_notebook_uniq_name(self):
        # Test the create_notebook method
        
        self.assertEquals(self.zeppelin.create_notebook(script=script, uniq_name=True, delete_after_run=True), True)
        
        
        
    # def create_notebook_run(self):
        
    #     # Test the create_notebook method
    #     script={
    #                 "name": "testing",
    #                 "paragraphs": [
    #                     {
    #                     "text": "This sample paragraft"
    #                     }
    #                 ]
    #             }
        
    #     self.zeppelin.create_notebook(script=script, uniq_name=True, run_all=True, delete_after_run=True)

    # def get_status_notebook(self):
    #     self.zeppelin.get_status(note_id="2JJWEYSNV")
        
if __name__ == '__main__':
    unittest.main()
