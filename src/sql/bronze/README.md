# Bronze Layer SQL

| Order | File | Purpose |
| --- | --- | --- |
| 01 | 01_create_streaming_table.sql | Accident streaming ingest |
| 02 | 02_create_vehicle_stream.sql | Vehicle streaming ingest |
| 03 | 03_create_person_stream.sql | Person streaming ingest |
| 04 | 04_create_atmospheric_stream.sql | Atmospheric condition lookup ingest |
| 05 | 05_create_road_surface_stream.sql | Road surface condition lookup ingest |

Each script creates a streaming table and snapshot table under `yinli_catalog.bronze`.
