"""Step registry for pipeline v2 prototype."""

from importlib import import_module
from typing import Protocol

from pipelinev2.state import RunContext, StepResult


class StepModule(Protocol):
    STEP_NAME: str

    def run(self, context: RunContext) -> StepResult: ...


# Expose step modules
auctions = import_module("pipelinev2.steps.auctions")
inbox_ingest = import_module("pipelinev2.steps.inbox_ingest")
judgment_extract = import_module("pipelinev2.steps.judgment_extract")
bulk_enrich = import_module("pipelinev2.steps.bulk_enrich")
hcpa_enrich = import_module("pipelinev2.steps.hcpa_enrich")
ori_iterative = import_module("pipelinev2.steps.ori_iterative")
survival = import_module("pipelinev2.steps.survival")
permits = import_module("pipelinev2.steps.permits")
flood = import_module("pipelinev2.steps.flood")
market = import_module("pipelinev2.steps.market")
tax = import_module("pipelinev2.steps.tax")
tax_deeds = import_module("pipelinev2.steps.tax_deeds")
skip_tax = import_module("pipelinev2.steps.skip_tax")
geocode = import_module("pipelinev2.steps.geocode")
status_refresh = import_module("pipelinev2.steps.status_refresh")

__all__ = [
    "StepModule",
    "auctions",
    "inbox_ingest",
    "judgment_extract",
    "bulk_enrich",
    "hcpa_enrich",
    "ori_iterative",
    "survival",
    "permits",
    "flood",
    "market",
    "tax",
    "tax_deeds",
    "skip_tax",
    "geocode",
    "status_refresh",
]
