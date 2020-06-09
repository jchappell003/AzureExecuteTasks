import logging
import azure.functions as func
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

def main(req: func.HttpRequest) -> func.HttpResponse:    
    try:        
        PipelinePartType = req.params.get('PipelinePartType')
        
        req_body = ""
        
        if not PipelinePartType:
            try:
                req_body = req.get_json()            
            except ValueError:
                pass
            else:
                PipelinePartType = req_body.get('PipelinePartType')
                
        #load environmental variables        
        load_dotenv()
        SubscriptionID = os.getenv("SubscriptionID")
        ResourceGroupName = os.getenv("ResourceGroupName")
        DataFactoryName = os.getenv("DataFactoryName")
        ClientID = os.getenv("ClientID")
        SecretKey = os.getenv("SecretKey")
        TenantID = os.getenv("TenantID")  
        credential = ManagedIdentityCredential()        
        try:
            credential.get_token() #token check
        except:
            if not ClientID:
                raise Exception("Managed identity or environment variables are needed")
            else:
                credentials = ServicePrincipalCredentials(client_id=ClientID, secret=SecretKey, tenant=TenantID)
        
        #Azure clients
        resource_client = ResourceManagementClient(credentials, SubscriptionID)            
        adf_client = DataFactoryManagementClient(credentials, SubscriptionID)  
    
        logging.info('Python HTTP trigger function processed a request.')        
        
        JsonDefinition = req_body
        DataPipelineName = req_body.get('JsonDefinition').get("DataPipelineName")
        DataPipelineName = req.params.get('DataPipelineName')
        print(req.params)
        print(req_body)
        
        logging.info(f"Processing PipelinePartType: {PipelinePartType}")        

        
        ##
        ## Azure Data Factory Objects
        ##       
        
        if PipelinePartType == "ADF":
            logging.info(f"Executing the ADF Pipeline: {PipelinePartType}")  

            run_response = adf_client.pipelines.create_run(ResourceGroupName, DataFactoryName, "wait", parameters={"JsonDefinition" : JsonDefinition})
            
            while adf_client.pipeline_runs.get(ResourceGroupName, DataFactoryName, run_response.run_id).status == "InProgress":
                time.sleep(2)
            
            Response = adf_client.pipeline_runs.get(ResourceGroupName, DataFactoryName, run_response.run_id).status

        if PipelinePartType:
            return func.HttpResponse(Response)
        else:
            return func.HttpResponse(
                "Parameters not configured correctly.",
                status_code=400
            )
            
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(
                e,
                status_code=400
            )
