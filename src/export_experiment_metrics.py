import os
from dotenv import load_dotenv
from pathlib import Path

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient

load_dotenv()

WORKSPACE_ID = os.getenv("LOG_ANALYTICS_WORKSPACE_ID")
PIPELINE_NAME = "pl_master_experiment"

OUTPUT_DIR = Path("results")
RAW_OUTPUT_PATH = OUTPUT_DIR / "experiment_run_metrics.csv"
FINAL_OUTPUT_PATH = OUTPUT_DIR / "final_experiment_results.csv"
AVERAGED_OUTPUT_PATH = OUTPUT_DIR / "averaged_experiment_results.csv"


# From Log Analytics Workspace
ADF_KQL_QUERY = f"""
let LatestRunId =
    toscalar(
        ADFPipelineRun
        | where PipelineName == "{PIPELINE_NAME}"
        | top 1 by TimeGenerated desc
        | project RunId
    );
ADFActivityRun
| where PipelineName == "{PIPELINE_NAME}"
| where PipelineRunId == LatestRunId
| where ActivityName in ("Run_Governed", "Run_Ungoverned")
| where Status == "Succeeded"
| extend Props = parse_json(UserProperties)
| extend Out = parse_json(Output)
| extend
    Scenario = tostring(Props.Scenario),
    GovernanceMode = tostring(Props.GovernanceMode),
    Repetition = tostring(Props.Repetition)
| extend
    RuntimeSeconds = round(datetime_diff("millisecond", End, Start) / 1000.0, 2),
    ComputeAcquisitionSec = round(todouble(Out.runStatus.computeAcquisitionDuration) / 1000.0, 2),
    ProcessingSeconds = round((datetime_diff("millisecond", End, Start) - todouble(Out.runStatus.computeAcquisitionDuration))/ 1000.0, 2)
| extend
    BillableCoreHours = todouble(Out.billingReference.billableDuration[0].duration),
    BillingUnit = tostring(Out.billingReference.billableDuration[0].unit),
    SessionType = tostring(Out.billingReference.billableDuration[0].sessionType)
| extend
    EstimatedCostAUD = round(BillableCoreHours * 0.395, 4)
| extend
    SCADARowsWritten = tolong(Out.runStatus.metrics.sinkSCADA.rowsWritten),
    TradingRowsWritten = tolong(Out.runStatus.metrics.sinkTRADING.rowsWritten),
    RegionRowsWritten = tolong(Out.runStatus.metrics.sinkREGION.rowsWritten),
    DURowsWritten = tolong(Out.runStatus.metrics.sinkDU.rowsWritten)
| extend
    SCADAFaultRows = coalesce(tolong(Out.runStatus.metrics.sinkFaultSCADA.rowsWritten), 0),
    TradingFaultRows = coalesce(tolong(Out.runStatus.metrics.sinkFaultTRADING.rowsWritten), 0),
    RegionFaultRows = coalesce(tolong(Out.runStatus.metrics.sinkFaultREGION.rowsWritten), 0),
    DUFaultRows = coalesce(tolong(Out.runStatus.metrics.sinkFaultDU.rowsWritten), 0)
| extend
    TotalRowsWritten = SCADARowsWritten + TradingRowsWritten + RegionRowsWritten + DURowsWritten,
    TotalFaultRows = SCADAFaultRows + TradingFaultRows + RegionFaultRows + DUFaultRows
| extend
    QuarantineRatePct = iff(TotalRowsWritten + TotalFaultRows > 0, round(100 * todouble(TotalFaultRows) / todouble(TotalRowsWritten + TotalFaultRows), 4), 0.0)
| project
    TimeGenerated,
    Scenario,
    GovernanceMode,
    Repetition,
    RuntimeSeconds,
    ComputeAcquisitionSec,
    ProcessingSeconds,
    BillableCoreHours,
    BillingUnit,
    SessionType,
    EstimatedCostAUD,
    SCADARowsWritten,
    SCADAFaultRows,
    TradingRowsWritten,
    TradingFaultRows,
    RegionRowsWritten,
    RegionFaultRows,
    DURowsWritten,
    DUFaultRows,
    TotalRowsWritten,
    TotalFaultRows,
    QuarantineRatePct,
    PipelineRunId,
    ActivityRunId
| order by Scenario asc, Repetition asc, GovernanceMode asc
"""

CONSUMER_KQL_QUERY = f"""
let LatestRunId =
    toscalar(
        ADFPipelineRun
        | where PipelineName == "{PIPELINE_NAME}"
        | top 1 by TimeGenerated desc
        | project RunId
    );

ADFActivityRun
| where PipelineName == "{PIPELINE_NAME}"
| where PipelineRunId == LatestRunId
| where ActivityName == "Run_Propagation_Consumer"

| extend Props = parse_json(UserProperties)
| extend Out = parse_json(Output)

| extend
    Scenario = tostring(Props.Scenario),
    GovernanceMode = tostring(Props.GovernanceMode),
    Repetition = tostring(Props.Repetition)

| extend
    Consumer = Out.resultSets[0].rows[0]

| extend
    ConsumerReportRows =
        tolong(Consumer.REPORT_ROWS),

    ConsumerRows =
        tolong(Consumer.CONSUMER_ROWS),

    ConsumerUnitCountSum =
        tolong(Consumer.UNIT_COUNT_SUM),

    ConsumerTotalMWh =
        todouble(Consumer.TOTAL_MWH),

    ConsumerTotalValue =
        todouble(Consumer.TOTAL_VALUE),

    NullSCADARows =
        tolong(Consumer.NULL_SCADA_ROWS),

    NullMWhRows =
        tolong(Consumer.NULL_MWH_ROWS),

    NullRegionRows =
        tolong(Consumer.NULL_REGION_ROWS),

    NullDemandRows =
        tolong(Consumer.NULL_DEMAND_ROWS),

    NullPriceRows =
        tolong(Consumer.NULL_PRICE_ROWS)

| extend
    ConsumerExecutionSeconds =
        todouble(Out.executionDuration),

    ConsumerIRHours =
        todouble(
            Out.billingReference.billableDuration[0].duration
        )

| project
    Scenario,
    GovernanceMode,
    Repetition,

    ConsumerStatus = Status,

    ConsumerReportRows,
    ConsumerRows,
    ConsumerUnitCountSum,
    ConsumerTotalMWh,
    ConsumerTotalValue,

    NullSCADARows,
    NullMWhRows,
    NullRegionRows,
    NullDemandRows,
    NullPriceRows,

    ConsumerExecutionSeconds,
    ConsumerIRHours,

    ConsumerActivityRunId = ActivityRunId

| order by Scenario asc,
           Repetition asc,
           GovernanceMode asc
"""
def query_log_analytics(query: str) -> pd.DataFrame:
    client = LogsQueryClient(DefaultAzureCredential())
    result = client.query_workspace(
        workspace_id=WORKSPACE_ID,
        query=query,
        timespan=None,
    )

    table = result.tables[0]

    return pd.DataFrame(
        table.rows,
        columns=table.columns,
    )

def merge_metrics(adf: pd.DataFrame, consumer: pd.DataFrame) -> pd.DataFrame:
    return adf.merge(consumer, on=["Scenario", "GovernanceMode", "Repetition"], how="left", validate="one_to_one")


def build_summary(df):
    df = df.copy()

    metrics = [
        #ADF
        "ProcessingSeconds",
        "BillableCoreHours",
        "EstimatedCostAUD",
        "TotalRowsWritten",
        "TotalFaultRows",
        "QuarantineRatePct",

        # Consumer
        "ConsumerReportRows",
        "ConsumerRows",
        "ConsumerUnitCountSum",
        "ConsumerTotalMWh",
        "ConsumerTotalValue",
        
        # Null propagation
        "NullSCADARows",
        "NullMWhRows",
        "NullRegionRows",
        "NullDemandRows",
        "NullPriceRows",

        # Consumer execution
        "ConsumerExecutionSeconds",
        "ConsumerIRHours",
    ]

    gov = (
        df[df["GovernanceMode"] == "governed"]
        [["Scenario", "Repetition"] + metrics]
        .set_index(["Scenario", "Repetition"])
        .add_prefix("Gov_")
    )

    ungov = (
        df[df["GovernanceMode"] == "ungoverned"]
        [["Scenario", "Repetition"] + metrics]
        .set_index(["Scenario", "Repetition"])
        .add_prefix("Ungov_")
    )

    final = gov.join(ungov, how="outer").reset_index()

    #Governance Calculations

    final["ProcessingOverheadPct"] = (
        100 * (final["Gov_ProcessingSeconds"] - final["Ungov_ProcessingSeconds"])
        / final["Ungov_ProcessingSeconds"]
    ).round(2)

    final["CostOverheadAUD"] = (
        final["Gov_EstimatedCostAUD"]
        - final["Ungov_EstimatedCostAUD"]
    ).round(4)

    #Quarantine Calculations

    final["QuarantinedRows"] = final["Gov_TotalFaultRows"]

    final["RowReconciliationDifference"] = (
        final["Ungov_TotalRowsWritten"]
        - (
            final["Gov_TotalRowsWritten"]
            + final["Gov_TotalFaultRows"]
        )
    )

    #Consumer Difference Calculations
    final["ConsumerRowDifference"] = (
        final["Gov_ConsumerRows"]
        - final["Ungov_ConsumerRows"]
    )

    final["ConsumerMWhDifference"] = (
        final["Gov_ConsumerTotalMWh"]
        - final["Ungov_ConsumerTotalMWh"]
    )

    final["ConsumerValueDifference"] = (
        final["Gov_ConsumerTotalValue"]
        - final["Ungov_ConsumerTotalValue"]
    )

    final["SCADANullsAvoided"] = (
        final["Ungov_NullSCADARows"]
        - final["Gov_NullSCADARows"]
    )

    final["MWhNullsAvoided"] = (
        final["Ungov_NullMWhRows"]
        - final["Gov_NullMWhRows"]
    )

    final["RegionNullsAvoided"] = (
        final["Ungov_NullRegionRows"]
        - final["Gov_NullRegionRows"]
    )

    final["DemandNullsAvoided"] = (
        final["Ungov_NullDemandRows"]
        - final["Gov_NullDemandRows"]
    )

    final["PriceNullsAvoided"] = (
        final["Ungov_NullPriceRows"]
        - final["Gov_NullPriceRows"]
    )

    return final

def add_baseline_comparison(final: pd.DataFrame) -> pd.DataFrame:
    final = final.copy()

    baseline = (
        final[final["Scenario"] == "s0"]
        [
            [
                "Repetition",
                "Gov_ConsumerRows",
                "Gov_ConsumerTotalMWh",
                "Gov_ConsumerTotalValue",
                "Ungov_ConsumerRows",
                "Ungov_ConsumerTotalMWh",
                "Ungov_ConsumerTotalValue",
            ]
        ]
        .rename(
            columns={
                "Gov_ConsumerRows":
                    "S0_Gov_ConsumerRows",
                "Gov_ConsumerTotalMWh":
                    "S0_Gov_TotalMWh",
                "Gov_ConsumerTotalValue":
                    "S0_Gov_TotalValue",
                "Ungov_ConsumerRows":
                    "S0_Ungov_ConsumerRows",
                "Ungov_ConsumerTotalMWh":
                    "S0_Ungov_TotalMWh",
                "Ungov_ConsumerTotalValue":
                    "S0_Ungov_TotalValue",
            }
        )
    )

    final = final.merge(
        baseline,
        on="Repetition",
        how="left",
    )

    # Ungoverned fault impact relative to clean S0
    final["Ungov_RowDeltaVsS0"] = (
        final["Ungov_ConsumerRows"]
        - final["S0_Ungov_ConsumerRows"]
    )

    final["Ungov_MWhDeltaVsS0"] = (
        final["Ungov_ConsumerTotalMWh"]
        - final["S0_Ungov_TotalMWh"]
    )

    final["Ungov_ValueDeltaVsS0"] = (
        final["Ungov_ConsumerTotalValue"]
        - final["S0_Ungov_TotalValue"]
    )

    # Governed fault impact relative to clean S0
    final["Gov_RowDeltaVsS0"] = (
        final["Gov_ConsumerRows"]
        - final["S0_Gov_ConsumerRows"]
    )

    final["Gov_MWhDeltaVsS0"] = (
        final["Gov_ConsumerTotalMWh"]
        - final["S0_Gov_TotalMWh"]
    )

    final["Gov_ValueDeltaVsS0"] = (
        final["Gov_ConsumerTotalValue"]
        - final["S0_Gov_TotalValue"]
    )

    return final


def average_repetitions(final):
    numeric_cols = final.select_dtypes(include="number").columns

    averaged = (final.groupby("Scenario")[numeric_cols].mean().reset_index())

    return averaged

def main():
    if not WORKSPACE_ID:
        raise ValueError("Set LOG_ANALYTICS_WORKSPACE_ID first.")

    OUTPUT_DIR.mkdir(exist_ok=True)

    adf = query_log_analytics(ADF_KQL_QUERY)
    consumer = query_log_analytics(CONSUMER_KQL_QUERY)

    merged = merge_metrics(adf, consumer)
    merged.to_csv(RAW_OUTPUT_PATH, index=False)

    final = build_summary(merged)
    final = add_baseline_comparison(final)
    final.to_csv(FINAL_OUTPUT_PATH, index=False)

    averaged = average_repetitions(final)
    averaged.to_csv(AVERAGED_OUTPUT_PATH,index=False)

    print(f"Saved: {RAW_OUTPUT_PATH}")
    print(f"Saved: {FINAL_OUTPUT_PATH}")
    print(f"Saved: {AVERAGED_OUTPUT_PATH}")

if __name__ == "__main__":
    main()
