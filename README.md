## Step 1. Create  a workspace
    Create a new workspace in fabric
## Step 2. Create  a Lakehouse
  Navigate to the newly created workspace and create a new lakehouse in it.
  
## Step 3. Create a new notebook

1. Create a new Notebook
2. Attach the lakehouse
3. Copy paste each of the  below codeblocks into new cell in the notebook


<details>

  <summary>View notebook code</summary>


For cell 1, use the below code block if API call needs to be made under the context of the current user
```
from notebookutils.mssparkutils.credentials import getToken
from datetime import datetime,timedelta
import requests
token = getToken("https://analysis.windows.net/powerbi/api")
```

For cell 1, use the below code block if API call needs to be made under the context of service principal
```
from notebookutils.mssparkutils.credentials import getToken
from datetime import datetime,timedelta
import requests

client_id = ""
client_secret = ""
tenant_id = ""

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
headers = {"Content-Type": "application/x-www-form-urlencoded"}
data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "https://analysis.windows.net/powerbi/api/.default",
}

response = requests.post(token_url, headers=headers, data=data)
token = response.json()["access_token"]
```

```
base_url = "https://api.powerbi.com/v1.0/myorg/admin/"
token = getToken("https://analysis.windows.net/powerbi/api")
headers = {"Authorization": f"Bearer {token}"}


previous_day = datetime.now() - timedelta(days=1)


startDateTime = previous_day.strftime("'%Y-%m-%dT00:00:00Z'")
endDateTime   = previous_day.strftime("'%Y-%m-%dT23:59:59Z'")

folder_path = "Files/activityevents/year=" + previous_day.strftime("%Y")  + "/month=" +  previous_day.strftime("%Y%m")  + "/day=" +  previous_day.strftime("%Y%m%d")


resp_continuationUri = f"{base_url}/activityevents?startDateTime={startDateTime}&endDateTime={endDateTime}"
resp_lastResultSet = False

cont_counter = 0

while not resp_lastResultSet:
    response1 = requests.get(resp_continuationUri, headers=headers)

    file_path =  "/lakehouse/default/"  + folder_path + "/" + previous_day.strftime("%Y%m%d") + "_" + str(cont_counter).zfill(10) +".json"
    mssparkutils.fs.mkdirs(folder_path) 
    with open(file_path, "w") as file:
        file.write(response1.text)

    response1_json = response1.json()
    resp_continuationUri = response1_json['continuationUri']
    resp_lastResultSet   = response1_json['lastResultSet']
    cont_counter = cont_counter + 1
    

```


```
%%sql
DROP TABLE IF EXISTS activityevents_snapshot_step01;
CREATE TABLE  activityevents_snapshot_step01
(
activityEventEntities array<struct<
Id:string,
RecordType:string,
CreationTime:string,
Operation:string,
OrganizationId:string,
UserType:string,
UserKey:string,
Workload:string,
ResultStatus:string,
UserId:string,
ClientIP:string,
ItemName:string,
WorkSpaceName:string,
DatasetName:string,
ReportName:string,
WorkspaceId:string,
ObjectId:string,
ObjectType:string,
ObjectDisplayName:string,
Experience:string,
DatasetId:string,
ReportId:string,
ArtifactId:string,
ArtifactName:string,
ReportType:string,
DistributionMethod:string,
SensitivityLabelId:string,
ArtifactKind:string,
UserAgent:string,
Activity:string,
IsSuccess:string,
RequestId:string,
ActivityId:string,
ModelsSnapshots:string,
RefreshEnforcementPolicy:string,
SharingAction:string,
ShareLinkId:string,
SharingScope:string,
ConsumptionMethod:string,
CapacityId:string,
CapacityName:string
>>
) using  json
OPTIONS (
multiLine true,
path "Files/activityevents/*/*/*/*.json"
);
```

```
%%sql
DROP VIEW IF EXISTS activityevents_snapshot_step02;
CREATE VIEW  activityevents_snapshot_step02 AS
SELECT 
c2.Id,
c2.RecordType,
c2.CreationTime,
c2.Operation,
c2.OrganizationId,
c2.UserType,
c2.UserKey,
c2.Workload,
c2.UserId,
c2.ClientIP,
c2.ItemName,
c2.WorkSpaceName,
c2.DatasetName,
c2.ReportName,
c2.WorkspaceId,
c2.ObjectId,
c2.ObjectType,
c2.ObjectDisplayName,
c2.Experience,
c2.DatasetId,
c2.ReportId,
c2.ArtifactId,
c2.ArtifactName,
c2.ReportType,
c2.DistributionMethod,
c2.ConsumptionMethod,
c2.SensitivityLabelId,
c2.ArtifactKind,
c2.UserAgent,
c2.Activity,
c2.IsSuccess,
c2.RequestId,
c2.ActivityId,
c2.ModelsSnapshots,
c2.RefreshEnforcementPolicy,
c2.SharingAction,
c2.ShareLinkId,
c2.SharingScope,
c2.CapacityId,
c2.CapacityName
FROM  activityevents_snapshot_step01
LATERAL VIEW  
    posexplode(activityEventEntities) c01 as c1,c2
```

```
%%pyspark
resultsDF1=spark.sql("SELECT * FROM activityevents_snapshot_step02")
resultsDF1.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("Tables/FabricActivityEvents")
```


```
%%sql
DROP VIEW IF EXISTS  activityevents_snapshot_step02;
DROP TABLE IF EXISTS activityevents_snapshot_step01;
```
</details>

## Step 4. Schedule the notebook to run daily via pipeline
![Link](/screenshots/S05.jpg)



## View of the lakehouse with JSON files

![Link](/screenshots/S01.jpg)



## View of the lakehouse table

![Link](/screenshots/S02.jpg)

## Query  lakehouse tables using TSQL via SQL endpoint

![Link](/screenshots/S03.jpg)

![Link](/screenshots/S04.jpg)
