# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Pre-requisites

All code runs inside the Spark/Jupyter container managed by the sibling `quant-infrastructure/` repo. The container must be running before executing any notebooks or ETL code. Infrastructure setup is documented in `../../CLAUDE.md`.

## Extractions
When running extractions, Claude may use local python virtual environment (`env/`) to execute python code that do not depends on spark.  
A module `src.extractions.alpaca` has been created to extract stocks values. 