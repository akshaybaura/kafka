import faust
import json
import string
import re
from shapely import wkt



app = faust.App(
    'user_group_json',
    broker='kafka://localhost:9092',
    value_serializer='raw'
)


routes_json_topic = app.topic('routes_example_json')
sink_topic = app.topic('sink_topic')

@app.agent(routes_json_topic,sink=[sink_topic])
async def routes_processor(routes):
    async for route in routes:
        route_json = json.loads(re.sub(f'[^{re.escape(string.printable)}]', '', route.decode('utf_8')))
        route_json.update({'geom':wkt.loads(route_json['geom']).length})
        sink_route=str(route_json)
        yield sink_route
