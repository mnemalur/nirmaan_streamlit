# Flow Review: Counts → Explore Cohort

## Current Flow

### Step 1: User Gets Counts
**Genie generates SQL for counts:**
```sql
SELECT 
    COUNT(DISTINCT MEDREC_KEY) as patient_count,
    COUNT(DISTINCT PAT_KEY) as visit_count,
    COUNT(DISTINCT PROV_ID) as site_count
FROM ...
WHERE ... (filters based on criteria)
```
**Result**: 1 row with 3 columns (patient_count, visit_count, site_count)
**Purpose**: User sees counts to assess if criteria is correct

### Step 2: User Says "Explore"
**Genie generates NEW SQL for patient records:**
```sql
SELECT DISTINCT 
    MEDREC_KEY,  -- or PAT_KEY, or both
    PAT_KEY,
    ... (other relevant patient-level fields)
FROM ...
WHERE ... (SAME filters as Step 1)
```
**Result**: Multiple rows, each row is a patient/visit record
**Purpose**: Get actual patient records to materialize as cohort table

### Step 3: Materialize Cohort Table
**Create table from patient records SQL:**
```sql
CREATE OR REPLACE TABLE delta.`/tmp/clinical_cohorts/cohort_xyz`
USING DELTA
AS 
SELECT DISTINCT 
    MEDREC_KEY,
    PAT_KEY,
    ...
FROM ... (the patient records SQL from Step 2)
```
**Result**: Cohort table with patient rows
**Purpose**: Materialized table for dimension analysis

### Step 4: Dimension Analysis
**Run dimension queries on cohort table:**
```sql
SELECT 
    d.GENDER,
    COUNT(DISTINCT c.MEDREC_KEY) as patient_count,
    ...
FROM cohort_table c
JOIN phd_de_patdemo d ON c.MEDREC_KEY = d.MEDREC_KEY
GROUP BY d.GENDER
```
**Result**: Dimension breakdowns (gender, race, age, etc.)
**Purpose**: Visualize cohort characteristics

## Key Points

1. ✅ **Two separate SQL calls**: One for counts, one for patient records
2. ✅ **Same criteria**: Both use the same filters/codes, just different SELECT
3. ✅ **Patient records SQL**: Returns DISTINCT patient identifiers (MEDREC_KEY/PAT_KEY)
4. ✅ **Materialization**: Cohort table created from patient records SQL result
5. ✅ **Dimension analysis**: Uses cohort table to join with phd_de_patdemo

## Verification Needed

The prompt for patient records should be explicit about:
- ✅ Returning MEDREC_KEY or PAT_KEY (or both) - needed for dimension analysis joins
- ✅ Using DISTINCT to avoid duplicates
- ✅ NOT aggregating (no COUNT, no GROUP BY)
- ✅ Including all necessary join keys

