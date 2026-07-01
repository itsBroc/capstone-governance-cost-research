# Quantifying the Joint Cost of Governance and Fault Detection in Cloud-Native Data Pipelines

## Overview

This repository contains the implementation and experimental artefacts for my Software Engineering Capstone Project at the University of Technology Sydney (UTS).

The project investigates how different levels of governance maturity influence the operational cost of cloud-native data pipelines when faults occur. While existing research studies cloud costs, governance and fault detection independently, there is limited empirical work examining how these factors interact within a unified cloud environment.

Using Microsoft Azure, this project develops a reproducible experimental pipeline to evaluate the trade-offs between governance overhead and downstream fault recovery costs.

---

## Research Question

> How does governance maturity influence the relationship between fault propagation and the total cost of ownership in cloud-native data pipelines?

---

## Objectives

- Design a cloud-native Azure data pipeline.
- Implement multiple governance configurations.
- Inject controlled faults throughout the pipeline.
- Measure operational and financial impacts.
- Develop and validate a governance-aware cost model.
- Evaluate governance trade-offs using empirical experimentation.

---

## Technology Stack

### Cloud Platform

- Microsoft Azure
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Synapse Analytics
- Microsoft Purview
- Azure Monitor
- Azure Cost Management

### Development

- Python
- Git
- GitHub

## Experimental Workflow

1. Configure governance tier.
2. Deploy Azure pipeline.
3. Inject controlled fault.
4. Execute pipeline.
5. Collect Azure metrics.
6. Export cost and monitoring data.
7. Analyse results.
8. Construct and validate the cost model.

---

## Current Status

Project currently under active development.

Completed:
- Literature Review
- Research Proposal
- Experimental Design

In Progress:
- Azure Infrastructure
- Pipeline Development
- Governance Implementation

Planned:
- Experimental Evaluation
- Statistical Analysis
- Cost Model Validation
- Final Research Paper

---

## Expected Contributions

This research aims to provide:

- An experimentally validated governance-aware cost model.
- Comparative analysis of governance configurations.
- Insights into the financial impact of delayed fault detection.
- Practical guidance for enterprise cloud data engineering.

---
