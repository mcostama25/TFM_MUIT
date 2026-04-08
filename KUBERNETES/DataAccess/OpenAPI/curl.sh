curl --location --request GET 'localhost:8080/openapi/entities/v1/latest?urns=urn:li:dataset:(urn:li:dataPlatform:platform,testSchemaIngest,PROD)&aspectNames=schemaMetadata' \
--header 'Accept: application/json' \
--header 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjYzOGIzZTEyLTA0ZTQtNDYzYi05ZjNiLTEwZWNmNjNkYWQwNCIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NzY2MjE3MjEsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.DyIsQvDxVJMgomPLpR-RsuLn2y4J9BkCwCGKEdAQl0M' \
| jq .

sleep 5

curl -X 'GET' \
    'http://localhost:8080/openapi/relationships/v1/?urn=urn%3Ali%3Acorpuser%3Adatahub&relationshipTypes=IsPartOf&direction=INCOMING&start=0&count=200' \
    --header 'accept: application/json' \
    --header 'Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImRhdGFodWIiLCJ0eXBlIjoiUEVSU09OQUwiLCJ2ZXJzaW9uIjoiMiIsImp0aSI6IjYzOGIzZTEyLTA0ZTQtNDYzYi05ZjNiLTEwZWNmNjNkYWQwNCIsInN1YiI6ImRhdGFodWIiLCJleHAiOjE3NzY2MjE3MjEsImlzcyI6ImRhdGFodWItbWV0YWRhdGEtc2VydmljZSJ9.DyIsQvDxVJMgomPLpR-RsuLn2y4J9BkCwCGKEdAQl0M' \
    | jq .