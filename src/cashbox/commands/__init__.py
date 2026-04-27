from __future__ import annotations

from . import backtests, evaluation, execution, experiments, gateway, governance, health, ingest, paper, research, risk

COMMAND_FAMILIES = (
    ingest.register,
    research.register,
    experiments.register,
    backtests.register,
    evaluation.register,
    paper.register,
    risk.register,
    execution.register,
    governance.register,
    gateway.register,
    health.register,
)


def register_all(subparsers: object) -> None:
    for register in COMMAND_FAMILIES:
        register(subparsers)


__all__ = ["register_all"]
