# kafka
The project aims towards doing a hands-on on confluent kafka.

### Problem Statement
There is a massive amount of GPS route data but each route only contains LineStrings for each step of the route. We should be able to know the total distance travelled for each route.

### Task 
Create a data pipeline which, when given GPS route data, reports the distance travelled for each route.

### Input
The GPS route data will be a .tsv file with only 3 columns defined in the table below:

| Name | Description | Type |
| ----------- | ----------- | ----------- |
| route | Unique identifier for each route | string |
| step | Step number for a given route | 32 bit integer |
| geom | LineString data for a given step of a given route | WKT string |
 
### Output
The output of the data pipeline should be a file with 3 columns as defined below:

| Name | Description | Type |
| ----------- | ----------- | ----------- |
| route | Unique identifier for each route | string |
| step_count | Total number of steps in a route | 32 bit integer |
| distance | Total distance of the route | 64 bit integer |


## Proposed Solution
I used confluent kafka as a pub-sub mechanism to cater to the problem. As a preface to the kafka broker there would be a producer application which would read the input tsv and send data to "routes_json" topic. The data sent would be JSON serialised(JSON_SR). For POC purpose, I have kept the partitions and replications to be 1 for all topics including "routes_json". 
Then a consumer application (based off of kafka-python) subscribes to the "routes_json" topic and transforms the WKT LineString into readable distance using Shapely module. The transformed data is sent back to the kafka broker to a topic called "reroutes_json". The data here as well is sent in a JSON_SR fashion.
The transformed data is then to be sent to ElasticSearch so that Kibana can built reports over it. For this purpose, I used KSQL to create an ElasticSearch sink.
Following is a diagramatic representation of the above:

![alt text](kafka_demo.png)