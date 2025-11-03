# TL;DR

49.6k road crashes were recorded across Victoria between January 2022 and December 2024, including 800 fatal and 14.9k serious-injury collisions. Wet-weather and night-time events remain over-represented in fatal outcomes, and two-thirds of deadly crashes happen away from intersections. Mapping the 2022-2024 data reveals dense clusters of serious hotspots across metropolitan Melbourne—particularly along arterial roads managed by local councils and "Arterial Other" RMAs.

## Dataset & Scope
- Source: Transport Victoria Road Crash Data (accident.csv + lookup tables) ingested into Databricks Unity Catalog (`yinli_catalog`).
- Temporal window: **2022-01-01 – 2024-12-31** (post-lockdown, complete calendar years).
- Fact table used for analysis: `yinli_catalog.gold.fact_accident_enriched_2022_2024`.
- All metrics computed on SQL Warehouse `cd400faa731b591f`; key queries are listed in the appendix.

## Key Counts
| Metric | Value |
| --- | ---: |
| Total crashes | **49,619** |
| Serious-injury crashes (severity 1-2) | **15,779** |
| Fatal crashes (severity 1) | **800** |
| Average crashes per month | ~**1,378** |
| Severity mix | 1 (fatal) 1.6% · 2 (serious) 30.2% · 3 (other injury) 68.2% · 4 (non injury) ≈0% |

## Temporal Highlights
- Crash volumes are relatively stable across the post-lockdown window, with modest peaks every **March–May (~1.5–1.6k crashes/month)** and dips mid-winter and December.
- Fatal counts average ~22 per month; serious injury averages ~410 per month.
- Dashboard heatmaps show the **15:00–19:00 window** dominating weekday crash activity, while night-time (20:00–05:00) still accounts for ~10–11% of events but a higher share of fatalities.

## Conditions, Weather & Speed
- **Lighting**: Daylight (code `1`) makes up 33,393 crashes / 494 deaths, but dusk (`3`, 7,882 crashes / 136 deaths) and dark–street-lights-off (`5`, 2,201 crashes / 112 deaths) carry higher fatality ratios.
- **Road surface**: Dry roads dominate exposure (38,939 crashes / 678 deaths). Wet surfaces still deliver 6,711 crashes and 103 fatalities; icy roads (94 crashes) produced 4 deaths.
- **Speed zone**: Urban **60 km/h** zones see the most crashes (16k) while **100 km/h** zones contribute fewer crashes (5.9k) but **288 fatalities**, indicating severity escalates with speed limits.
- **Weather**: Clear conditions yield 38k crashes (686 deaths); rain (4.3k crashes / 57 deaths) and fog (323 crashes / 20 deaths) are relatively rare but riskier.

## Spatial Findings
- Joined `node.csv` provides latitude/longitude, node type, and LGA for each crash.
- `summary_hotspots_2022_2024` ranks intersections/segments: top metro hotspots each logged **18–24 crashes** over three years (e.g., location_key 43917 at −38.1335, 145.3243).
- **65.9%** of fatal crashes occur **“Not at intersection”**, while cross and T-intersections split ~32% of fatalities.
- Crash exposure by road manager (RMA): `Arterial Other` (17.9k crashes / 301 deaths) and `Local Road` (16.6k / 228 deaths) dominate; Freeways have 4.7k crashes but still 58 fatalities.
- Hotspot page now combines these summaries with a Mapbox scatter plot colored by severity to flag critical arterial segments.

## Recommendations / Next Steps
1. **Improve time-of-day fidelity**: store raw `ACCIDENT_TIME` alongside parsed timestamps to eliminate null hours and support richer diurnal analysis.
2. **Speed zone taxonomy**: create a small lookup mapping numeric zones to user-friendly categories (e.g., School Zone, Rural Highway) for stakeholders.
3. **Policy evaluation layer**: overlay enforcement/speed-change datasets with `summary_hotspots_2022_2024` to monitor interventions post-2022.
4. **Exposure metrics**: integrate traffic counts or population data to calculate crash rates, not just volumes, particularly for rural arterials.
5. **Automated refresh**: orchestrate bronze→gold rebuilds and summary tables in a Databricks Workflow for monthly dashboard updates.

## Appendix – Representative SQL
```sql
-- Overall counts
SELECT COUNT(*) AS total_accidents,
       SUM(CASE WHEN severity IN (1,2) THEN 1 ELSE 0 END) AS severe_accidents,
       SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS fatal_accidents
FROM yinli_catalog.gold.fact_accident_enriched_2022_2024;

-- Fatal crashes by road geometry
SELECT road_geometry_desc,
       SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS fatal_crashes
FROM yinli_catalog.gold.fact_accident_enriched_2022_2024
GROUP BY road_geometry_desc
ORDER BY fatal_crashes DESC;

-- Hotspot summary with coordinates
CREATE OR REPLACE TABLE yinli_catalog.gold.summary_hotspots_2022_2024 AS
SELECT location_key, RMA, road_geometry_desc, NODE_TYPE, LGA_NAME, DEG_URBAN_NAME,
       LATITUDE, LONGITUDE,
       COUNT(*) AS crash_count,
       SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS fatal_crashes,
       SUM(CASE WHEN severity IN (1,2) THEN 1 ELSE 0 END) AS serious_crashes,
       AVG(severity) AS avg_severity
FROM yinli_catalog.gold.fact_accident_enriched_2022_2024
WHERE location_key IS NOT NULL
GROUP BY location_key, RMA, road_geometry_desc, NODE_TYPE, LGA_NAME, DEG_URBAN_NAME, LATITUDE, LONGITUDE;
```
