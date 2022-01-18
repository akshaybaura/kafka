# kafka
The project aims towards doing a hands-on on confluent kafka.

### Problem Statement
There is a massive amount of GPS route data but each route only contains LineStrings for each step of the route. We should be able to know the total distance travelled for each route.

### Task 
Create a data pipeline which, when given GPS route data, reports the distance travelled for each route.

### INPUT
The GPS route data will be a .tsv file with only 3 columns defined in the table below:

| Name | Description | Type |
| ----------- | ----------- | ----------- |
| route | Unique identifier for each route | string |
| step | Step number for a given route | 32 bit integer |
| geom | LineString data for a given step of a given route | WKT string |
 
### OUTPUT
The output of the data pipeline should be a file with 3 columns as defined below:

| Name | Description | Type |
| ----------- | ----------- | ----------- |
| route | Unique identifier for each route | string |
| step_count | Total number of steps in a route | 32 bit integer |
| distance | Total distance of the route | WKT string | 64 bit integer |

