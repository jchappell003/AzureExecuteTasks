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
        
        resource_client = ResourceManagementClient(credentials, SubscriptionID)            
        adf_client = DataFactoryManagementClient(credentials, SubscriptionID)  
    
        logging.info('Python HTTP trigger function processed a request.')
        
        PipelinePartType = req.params.get('PipelinePartType')
        JsonDefinition = req.params.get('JsonDefinition')
        DataPipelineName = req.params.get('DataPipelineName')
        
        logging.info(f"Processing PipelinePartType: {PipelinePartType}")
        
        if not PipelinePartType:
            try:
                req_body = req.get_json()            
            except ValueError:
                pass
            else:
                PipelinePartType = req_body.get('PipelinePartType')
        
        ##
        ## Azure Data Factory Objects
        ##       
        
        if PipelinePartType == "ADF":
            logging.info(f"Executing the ADF Pipeline: {PipelinePartType}")  

            run_response = adf_client.pipelines.create_run(ResourceGroupName, DataFactoryName, "wait", parameters={"JsonDefinition" : JsonDefinition})
            
            while adf_client.pipeline_runs.get(ResourceGroupName, DataFactoryName, run_response.run_id).status == "InProgress":
                time.sleep(2)
            
            Response = f"DataPipelineName {DataPipelineName} executed successfully!"

        if PipelinePartType:
            return func.HttpResponse()
        else:
            return func.HttpResponse(
                "Please pass a name on the query string or in the request body",
                status_code=400
            )
            
    except Exception as e:
        logging.error(e)
        return func.HttpResponse(
                e,
                status_code=400
            )
