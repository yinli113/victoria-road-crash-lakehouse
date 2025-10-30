# Silver Layer SQL

| Order | File | Purpose |
| --- | --- | --- |
| 10 | 10_transform_accident.sql | Clean accident records |
| 20 | 20_transform_vehicle.sql | Clean vehicle records |
| 30 | 30_transform_person.sql | Clean person records |
| 40 | 40_transform_atmospheric.sql | Clean atmospheric condition lookup |
| 50 | 50_transform_road_surface.sql | Clean road surface condition lookup |

Silver tables dedupe records and standardize columns for downstream facts/dimensions.
