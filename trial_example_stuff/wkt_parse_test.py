from shapely import wkt
import json

ls = 'LINESTRING (139.74133 35.68213, 139.74133 35.68198, 139.74133 35.681810000000006, 139.74132 35.681450000000005, 139.74132 35.68142, 139.74132 35.68139, 139.74132 35.68135, 139.74149 35.68131, 139.74188 35.68121)'

print(wkt.loads(ls).length)

