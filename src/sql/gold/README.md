# Gold Layer SQL

| Order | File | Purpose |
| --- | --- | --- |
| 10 | 10_dim_time.sql | Time dimension |
| 15 | 15_dim_conditions.sql | Conditions dimension |
| 20 | 20_dim_location.sql | Location dimension |
| 25 | 25_dim_vehicle.sql | Vehicle dimension |
| 30 | 30_fact_accident.sql | Accident fact |
| 35 | 35_dim_casualty.sql | Casualty dimension |
| 40 | 40_materialized_views.sql | Summary materialized views |
| 45 | 45_dim_dca.sql | DCA dimension |
| 50 | 50_fact_vehicle.sql | Vehicle fact |
| 60 | 60_fact_person.sql | Person fact |

Materialized views can be refreshed via Databricks SQL Warehouses.
