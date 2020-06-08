import unittest
import json
import azure.functions as func
from ExecutePipelines import main as my_function
import json

class TestFunction(unittest.TestCase):
    def test_my_function(self):
        # Construct a mock HTTP request.
        mylist = {  
                    "PipelinePartType": "ADF",
                    "JsonDefinition" : {
                    "DataPipelineName" : ""
                    }
                  }
        jsonparams = json.dumps(mylist,sort_keys=True,indent=4, separators=(',', ': '))   
        jsondict = json.loads(jsonparams)   
        jsonparamsencoded = json.JSONEncoder().encode(jsondict)        
        
        #req = func.HttpRequest(
        #    method='GET',
        #    body=jsonparamsencoded.encode(),
        #    url='/api/HttpTrigger',
        #    params={})
        
        req = func.HttpRequest(
            method='POST',
            body=jsonparamsencoded.encode(),
            url='/api/HttpTrigger',
            params={})


        # Call the function.
        resp = my_function(req)
        
        print(resp.get_body())

        # Check the output.
        self.assertEqual(
            resp.get_body(),
            b'Hello Test',
        )
        
        
Test = TestFunction()
result = Test.test_my_function()
print(result)