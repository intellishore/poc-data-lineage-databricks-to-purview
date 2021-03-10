import logging
import os, json
import azure.functions as func
import gzip

from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import AtlasClient, AtlasClient, AtlasEntity, AtlasProcess
from pyapacheatlas.core.util import GuidTracker

# Class for Purview Connection
class Azure_Connection_Information:
    def __init__(self):

        # Purview Connect Options
        self.tenant_id = os.environ["tenant_id"]
        self.client_id = os.environ["client_id"]
        self.client_secret = os.environ["client_secret"]
        self.endpoint_url = os.environ["endpoint_url"]

Azure = Azure_Connection_Information()

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    param = req.route_params.get("parem")
        
    if param == "status" or param == "producer/status":
        return func.HttpResponse("STATUS : Everything's working 1", status_code=200, headers={"ABSA-Spline-API-Version": "1", "ABSA-Spline-Accept-Request-Encoding": "gzip"}) 
    
    logging.info('Python HTTP trigger function processed a request.')
    
    try:
        # Get body of request
        req_body = req.get_body()
    except ValueError:
        pass
    else:
        try: req_body_unzip = gzip.decompress(req_body)
        except OSError: pass
        
        spline_JSON = req_body_unzip.decode()

        # Convert Spline input Json to Apache Atlas output
        purview_lineage = convert_Spline_to_Purview(spline_JSON)
    
        # Upload to Purview
        uploadPurview(purview_lineage)
        
    return func.HttpResponse(
            "Everything's working",
            status_code=200
    )

def uploadPurview(purview_lineage):
    
    oauth = ServicePrincipalAuthentication(
        tenant_id = Azure.tenant_id, 
        client_id = Azure.client_id, 
        client_secret = Azure.client_secret
    )

    client = AtlasClient(
        endpoint_url = Azure.endpoint_url,
        authentication = oauth
    )

    results = client.upload_entities(
        batch = purview_lineage
    )

def convert_Spline_to_Purview(splineJson):
    splineJson = json.loads(splineJson)
    
    # Get notebook info
    notebookInfo = splineJson["extraInfo"]["notebookInfo"]["obj"]
    notebookURL = notebookInfo["notebookURL"].replace("\\","")

    guid = GuidTracker()

    # Get inputs 
    inputs = []
    for read in splineJson["operations"]["reads"]: 
        input_path = read["inputSources"][0].replace(notebookInfo["mounts"][0], "https://adldata.dfs.core.windows.net/data/")
        input = AtlasEntity(
                            name = input_path.split("/")[-1],
                            typeName = "azure_datalake_gen2_path",
                            qualified_name = input_path,
                            guid = guid.get_guid()
                            )
        inputs.append(input)

    # Get outputs 
    write = splineJson["operations"]["write"]
    output_path = write["outputSource"].replace(notebookInfo["mounts"][0], "https://adldata.dfs.core.windows.net/data/")
    output = AtlasEntity(
                        name = output_path.split("/")[-1],
                        typeName = "azure_datalake_gen2_path",
                        qualified_name = output_path,
                        guid = guid.get_guid()
                        )

    # Get Process 
    process_attributes = {
                        "name": notebookInfo["name"],
                        "owner": notebookInfo["user"],
                        "description": f"Link to spark job notebook: http://{notebookURL}",
                        "startTime": notebookInfo["timestamp"],
                        "endTime": notebookInfo["timestamp"]
                        }
    process = AtlasProcess(
                            name = notebookInfo["name"],
                            typeName = "Process",
                            qualified_name = f"adb-{notebookURL[4:20]}",
                            inputs = inputs,
                            outputs = [output],
                            guid = guid.get_guid(),
                            attributes = process_attributes
                            )

    purview_lineage = inputs + [output] + [process]
    return purview_lineage