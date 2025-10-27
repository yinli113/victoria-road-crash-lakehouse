# TL;DR

Define the Slowly Changing Dimension Type 2 (SCD2) strategy for policy and environment attributes (e.g., speed zone changes) in the accident lakehouse.

## 1. Purpose
- Preserve historical context for attributes that change over time (speed limits, enforcement areas)
- Support policy impact analysis and before/after comparisons

## 2. SCD2 Candidate Dimensions
- `dim_location` with speed zone, signage, enforcement program fields
- Potential future dimensions (e.g., `dim_speed_zone_policy`, `dim_infrastructure`)

## 3. Keys & Metadata
- Natural key: location identifier + jurisdiction codes
- Surrogate key: generated `location_sk`
- Metadata columns: `effective_start_date`, `effective_end_date`, `is_current`, `audit_ts`

## 4. ETL Logic
- Detect changes via comparison between incoming silver data and current dimension records
- On change: close existing row (`effective_end_date = change_date - 1`, `is_current = false`)
- Insert new version with updated attributes and open-ended end date (`9999-12-31`)
- Handle same-day multiple updates and late-arriving corrections

## 5. Implementation Approach
- Use MERGE statements in serverless SQL Warehouse with staged change tables
- Maintain change-tracking view or Delta table for audit
- Optimize using partitioning by `is_current` or `effective_end_date`

## 6. Testing & Validation
- Unit tests ensuring only one current record per natural key
- Verify historical joins return correct version for given accident date
- Monitor change frequency and dimension growth

## 7. Operational Considerations
- Document change sources (policy feeds, manual overrides)
- Plan for replay/backfill if policy history expands
- Integrate with monitoring: alerts on unexpected churn or metadata gaps
