## Defined Stages:
    1. Research (Done)
    2. Source Raw Data (Done)
    3. Perform Data Preprocessing And Create Relevant Python Scripts (In Progress)
    4. Create and Configure Azure Environment // Resource Groups // Management Groups etc (In Progress)
    5. Upload Data to ADLS Gen2
    6. Build the Data Ingestion Pipeline
    7. Implement Data Quality and Governance Controls for the first pipeline
    8. Perform the fault injection Scenarios
    9. Monitor Pipeline Performance and Costs
    10-12. Repeat steps 7 through 9 with the second pipeline

    Steps 4-12 Need to be completed within a month

## Additional Notes About Storage, Scripts, Etc.
Each time the pipeline is run all files are run (for obvious cost consistency reasoning).

All partitioned files are uploaded to ADLS Gen2, these default files are immutable
Additional Corrupted files are uploaded in the same data lake but in a seperate directory

When a corrupted file is used, it is replaced in the pipeline, as opposed to being run along side the normal version

## Specific Fault Injection Scenarios and Associated Tables
Cost measured for the 2 tiers of governance, 3 runs of the pipeliene. By the end, should have atleast 36 rows of usable data to draw conclusions on

1. Base Pipeline Run: No Fault, Clean Output
2. DISPATCH_UNIT_SCADA: Type/Format fault, Replace 1 of the 7 partitions with a file that has been pre-corrupted (Set 2-5% of the SCADA values to Non-Numeric)
3. TRADINGPRICE: Missing/Null Fault, Replace 1 of the 7 partitions with a file that has been pre-corrupted (Set a block of time to Null values)
4. DISPATCHREGIONSUM: Schema Change, Replace one DISPATCHREGIONSUM partition with a file where REGIONID is renamed to REGION_ID (OR Add an unexpected column)
    (Upstream Team modifying a database export without notifying downstream teams)
5. DU_DETAIL_SUMMARY: Duplicate Records, duplicates a DUID subset before the join
6. File Join: Null Propagation, Change a subset of SCADA DUID values to validly formatted but nonexistent identifiers so that they fail to match DU_DETAIL_SUMMARY

Then could export the joined file to Azure Synapse analytics for something? (Could get some extra data to use?)

Additional Faults that could be used if necessary:
- Remove one of the seven daily partitions entirely.
- Changing numeric values to extremes


## Specifics Regarding Governance Tiers

### Governance Response to Detected Faults

When a fault is detected, the pipeline will take one of the following actions:

- **Fail the pipeline** when the fault makes the output unreliable, such as a missing column, duplicate join key or excessive unmatched records.
- **Quarantine invalid records** in a separate ADLS Gen2 directory while valid records continue.
- **Complete with a warning** when the fault is below a predefined tolerance.
- **Generate an alert** through Azure Monitor and Log Analytics.
- **Prevent publication** by only writing to the final output directory after all required checks pass.
- **Require a controlled rerun** after the corrupted file is replaced with the clean version.

### Governance Tier 1: Basic Governance

No dedicated data-quality validation 

Controls will include:
- Standard ADF ingestion, transformation and join activities.
- Default ADF pipeline and activity logging.

### Governance Tier 2: Enhanced Governance

The governed pipeline will add controls using Azure Data Factory, Microsoft Purview Enterprise, Azure Monitor, Log Analytics and ADLS Gen2.

Controls will include:

- Expected file and partition-count validation.
- Required-column and schema validation.
- Numeric type validation for fields such as SCADAVALUE and RRP.
- Required-field null checks.
- Time-series completeness checks for Trading Price intervals.
- Duplicate DUID detection before joins.
- Referential-integrity checks between SCADA and DU_DETAIL_SUMMARY.
- Join-cardinality and row-amplification checks.
- Data-quality thresholds.
- Quarantine output for invalid records.
- Alerts containing the failed rule and affected record count.
- Prevention of invalid output publication.
- Purview metadata cataloguing, lineage and selected quality rules.

The business transformation should remain identical across both tiers.

### Expected Response by Fault

| Fault                           | Tier 1: Baseline                                  | Tier 2: Governed                                              |
| ------------------------------- | ------------------------------------------------- | ------------------------------------------------------------- |
| Clean baseline                  | Complete successfully                             | Complete successfully and record quality results              |
| Non-numeric SCADA values        | May fail during conversion or produce null values | Detect invalid values, quarantine records and fail if needed  |
| Null Trading Price block        | May continue with incomplete output               | Detect nulls and missing time intervals                       |
| Renamed REGIONID                | Likely fail during mapping or transformation      | Reject the file through schema validation                     |
| Duplicate DUID records          | May increase joined row counts                    | Detect duplicate keys and invalid join cardinality            |
| Nonexistent SCADA DUID values   | May produce unmatched or null joined records      | Detect unmatched keys before publication                      |

## Dependent Variables and Measurement Tools

### Performance

Total pipeline duration: ADF Monitor
Activity duration: ADF Monitor or Log Analytics
Validation duration: ADF activity logs
Rows processed per second: ADF activity output and Python
Bytes read & written: ADF activity output

### Cost

ADF pipeline consumption: ADF Monitor
Copy Activity usage: ADF activity details
Mapping Data Flow usage: ADF activity details
Purview data-quality usage: Microsoft Purview and Azure Cost Management
ADLS Gen2 storage usage: Azure Monitor
Log Analytics ingestion: Log Analytics workspace usage
Estimated cost per pipeline run: Azure Cost Management and Python


### Fault Detection

Fault detected: ADF or Microsoft Purview
Detection stage: ADF activity logs
Failed rule: ADF validation output or Microsoft Purview
Pipeline blocked: ADF Monitor
Alert generated: Azure Monitor
Detection time: Log Analytics and Python
Rerun required: Experiment record

### Data Quality

Input and output row counts: ADF, Synapse or Python
Invalid row count: ADF quarantine output
Null count: ADF, Microsoft Purview, Synapse or Python
Non-numeric count: ADF or Python
Duplicate count: ADF, Synapse or Python
Unmatched join count: ADF or Python
Join-match rate: Python
Quarantined row count: ADF and ADLS Gen2
False-positive and false-negative rates: Python
