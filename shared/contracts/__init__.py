# shared/contracts
#
# Data contracts: canonical Python dataclasses / TypedDicts that define the
# shape of data as it flows between layers (ETL → Models → Backtesting).
#
# Having contracts here forces all three layers to agree on field names,
# types, and nullability in one place. If a field changes in the ETL output,
# the breakage is visible immediately in the models and backtesting layers.
#
# Example usage:
#   from shared.contracts import OHLCVBar, Signal
