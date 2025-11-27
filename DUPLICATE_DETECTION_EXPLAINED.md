# 🔍 Parking Ticket Duplicate Detection Logic

## Overview

Your app has a **"Show Duplicates" toggle** in the parking data stats that allows toggling between two dataset versions to identify duplicate records.

---

## How Duplicates Are Identified

### Method: Ticket Number Comparison

The duplicate detection uses a **simple but effective** approach:

```sql
SELECT 
  COUNT(*)::BIGINT AS total_rows,
  COUNT(DISTINCT ticket_number)::BIGINT AS distinct_tickets,
  COUNT(*) - COUNT(DISTINCT ticket_number) AS duplicate_tickets
FROM parking_tickets
```

**The Logic:**

- Count all rows in the table
- Count only DISTINCT `ticket_number` values
- Difference = number of duplicate rows (rows that share the same `ticket_number`)

**Example:**

```
Total rows:        100,000
Distinct tickets:   99,500
Duplicates:            500
```

= 500 rows with repeated ticket numbers (likely data import duplicates or versioning issues)

---

## Database Field: `ticket_number`

**Field Definition:**

- **Type:** TEXT
- **Source:** Extracted from raw Toronto parking ticket CSVs
- **Purpose:** Unique identifier for a parking ticket in the city's system
- **Uniqueness:** Should be unique, but duplicates can occur due to:
  - Multiple data exports/imports overlapping
  - Legacy data reconciliation issues
  - CSV processing errors

---

## How the Toggle Works in the UI

**File:** `map-app/src/components/StatsSummary.jsx`

```jsx
const showDiscrepancyControl = Boolean(
  showTotals
    && discrepancyCurrent      // Latest processed totals
    && discrepancyLegacy       // Earlier unverified totals
    && (hasMeaningfulDelta || forceDiscrepancy || discrepancyNote)
    && ['parking_tickets', 'red_light_locations', 'ase_locations'].includes(dataset)
);

const legacyToggleControl = showLegacyToggle ? (
  <label className="discrepancy-toggle">
    <input
      type="checkbox"
      checked={useLegacyTotals}
      onChange={(event) => onToggleLegacy(Boolean(event.target?.checked))}
    />
    <span>Include earlier unverified totals</span>
  </label>
) : null;
```

**What the toggle does:**

- **Unchecked:** Shows "Latest processed totals" (deduplicated/reconciled)
- **Checked:** Shows "Earlier unverified totals" (includes legacy/duplicate records)

**Associated Note:**

```
"We are reconciling the older exports so both sources align without duplicates."
```

---

## Build-Time Duplicate Detection

**File:** `src/etl/datasets/parking_tickets.py`

### Ticket Hash Creation

Each ticket gets a **hash fingerprint** at ETL time:

```python
def build_ticket_hash(
    ticket_number: Optional[str],
    parsed_date: Optional[str],
    time_value: Optional[str],
    infraction_code: Optional[str],
    infraction_description: Optional[str],
    set_fine: Optional[str],
    location1: Optional[str],
    location2: Optional[str],
    location3: Optional[str],
    location4: Optional[str],
) -> str:
    components = (
        ticket_number or "",
        parsed_date or "",
        time_value or "",
        infraction_code or "",
        infraction_description or "",
        set_fine or "",
        location1 or "",
        location2 or "",
        location3 or "",
        location4 or "",
    )
    raw = "|".join(components)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
```

**Hash Components:**

- ticket_number
- date_of_infraction
- time_of_infraction
- infraction_code
- infraction_description
- set_fine_amount
- location fields (1-4)

**Purpose:** Creates a unique fingerprint combining all ticket attributes to detect exact duplicates during import

---

## CLI Scripts for Duplicate Checking

### Debug Script 1: Quick Duplicate Count

**File:** `map-app/scripts/debug/parking-duplicates.mjs`

```javascript
const { rows } = await client.query(`
  SELECT 
    COUNT(*)::BIGINT AS total_rows,
    COUNT(DISTINCT ticket_number)::BIGINT AS distinct_tickets,
    COUNT(*)::BIGINT - COUNT(DISTINCT ticket_number)::BIGINT AS duplicate_tickets
  FROM parking_tickets
`);
```

**Usage:**

```bash
node map-app/scripts/debug/parking-duplicates.mjs
```

**Output Example:**

```json
{
  "total_rows": 37009379,
  "distinct_tickets": 37009379,
  "duplicate_tickets": 0
}
```

### Debug Script 2: Full Dataset Totals

**File:** `map-app/scripts/debug/query-totals.mjs`

Includes the same duplicate detection logic as part of broader dataset analysis:

```javascript
const { rows } = await client.query(`
  SELECT 
    COUNT(DISTINCT ticket_number)::bigint AS distinct_tickets,
    COUNT(*)::bigint - COUNT(DISTINCT ticket_number)::bigint AS duplicate_tickets
  FROM parking_tickets
`);
```

---

## Current Duplicate Status

**Last Known Count:**

- From conversation context: **37,009,379 total tickets**
- **All 37M+ tickets appear unique** (0 detected duplicates in recent runs)
- Suggests: Legacy duplicates have been successfully reconciled/deduplicated

---

## Why This Approach?

### ✅ Advantages

1. **Simple:** One SQL query, no complex logic
2. **Fast:** O(n) performance, uses database indexing
3. **Reliable:** Ticket number is the authoritative identifier
4. **Visible:** Can be queried anytime to verify data integrity

### ⚠️ Limitations

1. **Ticket Number Only:** Doesn't detect near-duplicates (e.g., same ticket with typos)
2. **No Content Comparison:** Doesn't check if duplicate rows have different field values
3. **Hash vs Real Duplication:** Build-time hash is separate from runtime detection

---

## Recommended Enhancements

If you want **deeper duplicate detection**, consider:

### 1. **Field-Level Duplicate Detection**

```sql
-- Find rows with same ticket_number but DIFFERENT values
SELECT 
  ticket_number,
  COUNT(*) as occurrences,
  COUNT(DISTINCT (
    date_of_infraction || '|' || 
    infraction_code || '|' || 
    set_fine_amount
  )) as distinct_variants
FROM parking_tickets
GROUP BY ticket_number
HAVING COUNT(*) > 1;
```

### 2. **Hash-Based Exact Duplicate Detection**

```sql
-- Find exact duplicates (all fields identical)
SELECT 
  MD5(COALESCE(ticket_number, '') || '|' ||
      COALESCE(date_of_infraction, '') || '|' ||
      COALESCE(infraction_code, '') || '|' ||
      COALESCE(set_fine_amount, '')) as record_hash,
  COUNT(*) as duplicate_count
FROM parking_tickets
GROUP BY record_hash
HAVING COUNT(*) > 1;
```

### 3. **Fuzzy Matching for Potential Duplicates**

```sql
-- Tickets on same street, same infraction, within 1 hour
SELECT 
  t1.ticket_number as ticket_a,
  t2.ticket_number as ticket_b,
  t1.street_normalized,
  t1.infraction_code,
  ABS(EXTRACT(EPOCH FROM (t1.date_of_infraction - t2.date_of_infraction))) as seconds_apart
FROM parking_tickets t1
JOIN parking_tickets t2 
  ON t1.street_normalized = t2.street_normalized
  AND t1.infraction_code = t2.infraction_code
  AND ABS(EXTRACT(EPOCH FROM (t1.date_of_infraction - t2.date_of_infraction))) < 3600
  AND t1.ticket_number < t2.ticket_number
LIMIT 100;
```

---

## Summary

| Aspect | Details |
|--------|---------|
| **Detection Method** | DISTINCT ticket_number count vs total rows |
| **Identifier** | `ticket_number` field (TEXT) |
| **Where Checked** | Runtime queries via SQL |
| **UI Control** | Toggle: "Include earlier unverified totals" |
| **Current Status** | ✅ No duplicates detected (37M+ unique tickets) |
| **Build-Time Hash** | MD5 hash of all 10 ticket fields for ETL deduplication |
| **Scripts** | `parking-duplicates.mjs`, `query-totals.mjs` |
