import logging
import os, time, json
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import gzip
from random import randrange
import argparse

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import AtlasClient, AtlasEntity
from pyapacheatlas.core import EntityTypeDef, RelationshipTypeDef, TypeCategory

class Azure_Connection_Information:
    def __init__(self):
        # Blob Connection
        self.blob_connnect_str = os.environ["blob_connect_str"]
        self.blob_container_name = os.environ["blob_container"]

        # Purview Connect 
        self.tenant_id = os.environ["tenant_id"]
        self.client_id = os.environ["client_id"]
        self.client_secret = os.environ["client_secret"]
        self.endpoint_url = os.environ["endpoint_url"]


azure = Azure_Connection_Information()

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    param = req.route_params.get("parem")
        
    if param == "status" or param == "producer/status":
        writeToBlob("STATUS : ", "status")
        return func.HttpResponse("STATUS : Everything's working 1", status_code=200, headers={"ABSA-Spline-API-Version": "1", "ABSA-Spline-Accept-Request-Encoding": "gzip"}) 
    
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        # Get body of request
        req_body = req.get_body()
    except ValueError:
        writeToBlob("Body is empty or not JSON formattet", "")
        pass
    else:
        try: req_body_unzip = gzip.decompress(req_body)
        except OSError: pass
        
        # Convert Spline input Json to Apache Atlas output Json
        spline_JSON = req_body_unzip.decode()
        entities, lineage = convertJSON(spline_JSON)
    
        atlas_JSON = json.dumps(entities, indent=4) + " \n\n " + json.dumps(lineage, indent=4)
        writeToBlob(atlas_JSON, "uploadToPurview") 

        # Upload Apache Atlas Json to Purview
        uploadPurview(entities, "entities")
        uploadPurview(lineage, "lineage")
        
    return func.HttpResponse(
            "Everything's working",
            status_code=200
    )

def writeToBlob(text, fix):
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(Azure.blob_connnect_str)
    
    # Create a file in local data directory to upload and download
    timestr = time.strftime("%Y%m%d_%H%M%S")

    randInt = randrange(10)
    local_path = "/tmp/"
    local_file_name = "databricks_" + timestr + "_" + fix + "_" + str(randInt) + ".json"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
    file = open(upload_file_path, 'w')
    #file.write(json.dumps(text, indent=4))# + " : " + url)
    file.write(text)
    file.close()

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=Azure.blob_container_name, blob=local_file_name)

    print("\n 2 Uploading to Azure Storage as blob:\n\t" + local_file_name)
    logging.info("\n 1 Uploading to Azure Storage as blob:\n\t" + local_file_name)
    
    # Upload the created file
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data)

def uploadPurview(upload_json, fix):
    
    # Intellishore Test
    oauth = ServicePrincipalAuthentication(
        tenant_id = Azure.tenant_id, 
        client_id = Azure.client_id, 
        client_secret = Azure.client_secret
    )

    client = AtlasClient(
        endpoint_url = Azure.endpoint_url,
        authentication = oauth
    )

    upload_results = client.upload_entities(upload_json)
    writeToBlob(json.dumps(upload_results, indent=4) + "", fix)

def convertJSON(splineJson):
    splineJson = json.loads(splineJson)
    
    # Get notebook info
    notebookInfo = splineJson["extraInfo"]["notebookInfo"]["obj"]
    notebookURL = notebookInfo["notebookURL"].replace("\\","")

    # Empty dicts for entities 
    purviewDataJson = {
                        "entities": [],
                        "referredEntities": {}
        }
    purviewLineageJson = {
                "entities": [{}],
                "referredEntities": {}
        }
    
    # Loop over all read instances and convert them
    inputs = []
    for i, read in enumerate(splineJson["operations"]["reads"]):
        # Set Azure Datalake gen 2 as path
        input_path = read["inputSources"][0].replace(notebookInfo["mounts"][0], "https://adldata.dfs.core.windows.net/data/")
        readJson = {}
        # Set type as Azure Datalake gen 2 path. Should change depending on different sources. 
        readJson["typeName"] = "azure_datalake_gen2_path"
        readJson["attributes"] = {
                        "path": input_path.replace("https://adldata.dfs.core.windows.net/", ""),
                        "qualifiedName": input_path,
                        "name": input_path.split("/")[-1]
                                }
        readJson["classification"] = []
        readJson["status"] = "ACTIVE"
        purviewDataJson["entities"].append(readJson)
        
        inputs.append(
            {
            "typeName": "azure_datalake_gen2_path",
            "uniqueAttributes": {
                "qualifiedName": input_path 
                }
            }  
        )
    
    # Get write information and convert it
    write = splineJson["operations"]["write"]
    output_path = write["outputSource"].replace(notebookInfo["mounts"][0], "https://adldata.dfs.core.windows.net/data/")
    writeJson = {}
    writeJson["typeName"] = "azure_datalake_gen2_path"
    writeJson["attributes"] = {
                    "path": output_path.replace("https://adldata.dfs.core.windows.net/", ""),
                    "qualifiedName": output_path,
                    "name": output_path.split("/")[-1]
                            }
    writeJson["classification"] = []
    writeJson["status"] = "ACTIVE"
    purviewDataJson["entities"].append(writeJson)   
    
    outputs = []
    outputs.append(
        {
        "typeName": "azure_datalake_gen2_path",
        "uniqueAttributes": {
            "qualifiedName": output_path
                }
            }  
        )
    
    # Set meta-data information
    purviewLineageJson["entities"][0]["typeName"] = "spark_application"
    purviewLineageJson["entities"][0]["attributes"] = {}
    purviewLineageJson["entities"][0]["attributes"]["qualifiedName"] = f"adb-{notebookURL[4:20]}"
    purviewLineageJson["entities"][0]["attributes"]["name"] = notebookInfo["name"]
    purviewLineageJson["entities"][0]["attributes"]["owner"] = notebookInfo["user"]
    purviewLineageJson["entities"][0]["attributes"]["description"] = f"Link to spark job notebook: http://{notebookURL}"
    purviewLineageJson["entities"][0]["attributes"]["startTime"] = notebookInfo["timestamp"]
    purviewLineageJson["entities"][0]["attributes"]["endTime"] = notebookInfo["timestamp"]
    purviewLineageJson["entities"][0]["attributes"]["inputs"] =  inputs
    purviewLineageJson["entities"][0]["attributes"]["outputs"] =  outputs
    purviewLineageJson["entities"][0]["attributes"]["classification"] =  []
    purviewLineageJson["entities"][0]["attributes"]["status"] =  "ACTIVE"
    
    return purviewDataJson, purviewLineageJson