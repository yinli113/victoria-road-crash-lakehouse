# Bronze Layer SQL

| Order | File | Purpose |
| --- | --- | --- |
| 01 | 01_create_streaming_table.sql | Accident streaming ingest |
| 02 | 02_create_vehicle_stream.sql | Vehicle streaming ingest |
| 03 | 03_create_person_stream.sql | Person streaming ingest |

Each script creates a streaming table and snapshot table under `yinli_catalog.bronze`.
