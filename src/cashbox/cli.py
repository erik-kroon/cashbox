from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path

from .backtests import BacktestServiceError, build_backtest_service
from .evaluator import EvaluationServiceError, PROMOTION_TARGET_STAGES, build_evaluator_service
from .experiments import (
    EXPERIMENT_STATUSES,
    ExperimentFilter,
    ExperimentServiceError,
    build_experiment_service,
)
from .gateway import AgentGatewayError, build_agent_gateway
from .ingest import FileSystemMarketStore, ingest_polymarket_markets
from .models import MarketFilter, parse_datetime
from .research import ResearchMarketReadPath


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cashbox market ingest and research read path.")
    parser.add_argument("--root", type=Path, default=Path(".cashbox/market-data"), help="Storage root.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_file = subparsers.add_parser("ingest-file", help="Ingest Polymarket-style market payloads from a JSON file.")
    ingest_file.add_argument("input", type=Path)
    ingest_file.add_argument("--source-name", default="polymarket-gamma")
    ingest_file.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    ingest_live = subparsers.add_parser("ingest-polymarket", help="Fetch and ingest markets from Polymarket Gamma.")
    ingest_live.add_argument("--limit", type=int, default=100)
    ingest_live.add_argument("--offset", type=int, default=0)
    ingest_live.add_argument("--active", choices=("true", "false"))
    ingest_live.add_argument("--received-at", help="Override the ingest receive timestamp in ISO-8601.")

    active = subparsers.add_parser("list-active-markets", help="List sanitized active markets from the latest dataset.")
    active.add_argument("--category")
    active.add_argument("--query")
    active.add_argument("--limit", type=int)
    active.add_argument("--include-inactive", action="store_true")
    active.add_argument("--dataset-id")

    metadata = subparsers.add_parser("get-market-metadata", help="Read sanitized market metadata.")
    metadata.add_argument("market_id")
    metadata.add_argument("--dataset-id")

    timeseries = subparsers.add_parser("get-market-timeseries", help="Read append-only market history.")
    timeseries.add_argument("market_id")
    timeseries.add_argument("--start")
    timeseries.add_argument("--end")
    timeseries.add_argument("--field", action="append", dest="fields")

    health = subparsers.add_parser("get-ingest-health", help="Summarize dataset freshness.")
    health.add_argument("--dataset-id")
    health.add_argument("--stale-after-seconds", type=int, default=3600)

    families = subparsers.add_parser("list-strategy-families", help="List supported experiment strategy families.")

    template = subparsers.add_parser("get-strategy-template", help="Read the schema template for a strategy family.")
    template.add_argument("strategy_family")

    validate = subparsers.add_parser("validate-strategy-config", help="Validate a strategy config payload.")
    validate.add_argument("strategy_family")
    validate.add_argument("--config-json", required=True)

    create_experiment = subparsers.add_parser(
        "create-experiment",
        help="Create an immutable experiment definition with an initial DRAFT lifecycle event.",
    )
    create_experiment.add_argument("--hypothesis", required=True)
    create_experiment.add_argument("--strategy-family", required=True)
    create_experiment.add_argument("--config-json", required=True)
    create_experiment.add_argument("--dataset-id", required=True)
    create_experiment.add_argument("--code-version", required=True)
    create_experiment.add_argument("--generated-by", required=True)

    clone_experiment = subparsers.add_parser(
        "clone-experiment",
        help="Clone an experiment into a new immutable definition with optional modifications.",
    )
    clone_experiment.add_argument("experiment_id")
    clone_experiment.add_argument("--modifications-json", default="{}")
    clone_experiment.add_argument("--generated-by", required=True)

    note = subparsers.add_parser("attach-research-note", help="Attach an append-only markdown note to an experiment.")
    note.add_argument("experiment_id")
    note.add_argument("--author", required=True)
    note.add_argument("--markdown", required=True)

    experiments = subparsers.add_parser("list-experiments", help="List experiments from the local registry.")
    experiments.add_argument("--strategy-family")
    experiments.add_argument("--status", choices=EXPERIMENT_STATUSES)
    experiments.add_argument("--generated-by")
    experiments.add_argument("--dataset-id")
    experiments.add_argument("--limit", type=int)

    experiment = subparsers.add_parser("get-experiment", help="Read an experiment definition and lifecycle history.")
    experiment.add_argument("experiment_id")

    transition = subparsers.add_parser(
        "transition-experiment-status",
        help="Append a status transition event for an experiment.",
    )
    transition.add_argument("experiment_id")
    transition.add_argument("--status", required=True, choices=EXPERIMENT_STATUSES)
    transition.add_argument("--changed-by", required=True)
    transition.add_argument("--reason")

    run_backtest = subparsers.add_parser(
        "run-backtest",
        help="Execute a deterministic backtest for a validated experiment against its immutable dataset.",
    )
    run_backtest.add_argument("experiment_id")
    run_backtest.add_argument("--dataset-id")
    run_backtest.add_argument("--assumptions-json", required=True)

    backtest_artifacts = subparsers.add_parser(
        "get-backtest-artifacts",
        help="Read persisted artifacts for a backtest run.",
    )
    backtest_artifacts.add_argument("run_id")

    backtest_failure = subparsers.add_parser(
        "explain-backtest-failure",
        help="Explain why a persisted backtest run failed.",
    )
    backtest_failure.add_argument("run_id")

    score_experiment = subparsers.add_parser(
        "score-experiment",
        help="Compute and persist a deterministic evaluator score for a successful backtest.",
    )
    score_experiment.add_argument("experiment_id")
    score_experiment.add_argument("--run-id")

    promotion = subparsers.add_parser(
        "check-promotion-eligibility",
        help="Evaluate a promotion gate and optionally promote a strategy when the gate passes.",
    )
    promotion.add_argument("experiment_id")
    promotion.add_argument("--target-stage", required=True, choices=PROMOTION_TARGET_STAGES)
    promotion.add_argument("--run-id")
    promotion.add_argument("--changed-by", default="evaluator")
    promotion.add_argument("--promote-if-eligible", action="store_true")
    promotion.add_argument("--min-out-of-sample-trades", type=int, default=250)
    promotion.add_argument("--min-distinct-markets", type=int, default=25)
    promotion.add_argument("--max-drawdown-limit-usd")

    credential = subparsers.add_parser(
        "issue-agent-credential",
        help="Issue a scoped credential for the read-only agent gateway.",
    )
    credential.add_argument("--subject", required=True)
    credential.add_argument("--allow-tool", action="append", dest="allowed_tools")
    credential.add_argument("--rate-limit-count", type=int, default=60)
    credential.add_argument("--rate-limit-window-seconds", type=int, default=60)
    credential.add_argument("--token", help="Optional fixed token for local development or tests.")

    gateway_call = subparsers.add_parser(
        "gateway-call",
        help="Call a read-only market tool through the scoped agent gateway.",
    )
    gateway_call.add_argument("tool_name")
    gateway_call.add_argument("--token", required=True)
    gateway_call.add_argument("--args-json", default="{}")
    gateway_call.add_argument("--user-id", required=True)
    gateway_call.add_argument("--session-id", required=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = FileSystemMarketStore(args.root)
    read_path = ResearchMarketReadPath(store)
    gateway = build_agent_gateway(args.root)
    experiments = build_experiment_service(args.root)
    backtests = build_backtest_service(args.root)
    evaluator = build_evaluator_service(args.root)

    if args.command == "ingest-file":
        payload = json.loads(args.input.read_text())
        manifest = store.ingest_market_payloads(
            payload,
            source_name=args.source_name,
            received_at=parse_datetime(args.received_at),
        )
        print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "ingest-polymarket":
        active = None if args.active is None else args.active == "true"
        manifest = ingest_polymarket_markets(
            store,
            limit=args.limit,
            offset=args.offset,
            active=active,
            received_at=parse_datetime(args.received_at),
        )
        print(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "list-active-markets":
        result = read_path.list_active_markets(
            MarketFilter(
                category=args.category,
                query=args.query,
                active_only=not args.include_inactive,
                limit=args.limit,
            ),
            dataset_id=args.dataset_id,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-market-metadata":
        result = read_path.get_market_metadata(args.market_id, dataset_id=args.dataset_id)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-market-timeseries":
        result = read_path.get_market_timeseries(
            args.market_id,
            start=parse_datetime(args.start),
            end=parse_datetime(args.end),
            fields=args.fields,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-ingest-health":
        result = read_path.get_ingest_health(
            dataset_id=args.dataset_id,
            stale_after=timedelta(seconds=args.stale_after_seconds),
        )
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "list-strategy-families":
        print(json.dumps(experiments.list_strategy_families(), indent=2))
        return 0

    if args.command == "get-strategy-template":
        try:
            result = experiments.get_strategy_template(args.strategy_family)
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "validate-strategy-config":
        try:
            config = json.loads(args.config_json)
            result = experiments.validate_strategy_config(args.strategy_family, config)
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --config-json payload: {exc}")
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "create-experiment":
        try:
            config = json.loads(args.config_json)
            result = experiments.create_experiment(
                hypothesis=args.hypothesis,
                strategy_family=args.strategy_family,
                config=config,
                dataset_id=args.dataset_id,
                code_version=args.code_version,
                generated_by=args.generated_by,
            )
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --config-json payload: {exc}")
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "clone-experiment":
        try:
            modifications = json.loads(args.modifications_json)
            result = experiments.clone_experiment(
                args.experiment_id,
                modifications,
                generated_by=args.generated_by,
            )
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --modifications-json payload: {exc}")
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "attach-research-note":
        try:
            result = experiments.attach_research_note(
                args.experiment_id,
                author=args.author,
                markdown=args.markdown,
            )
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "list-experiments":
        result = experiments.list_experiments(
            ExperimentFilter(
                strategy_family=args.strategy_family,
                status=args.status,
                generated_by=args.generated_by,
                dataset_id=args.dataset_id,
                limit=args.limit,
            )
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-experiment":
        try:
            result = experiments.get_experiment(args.experiment_id)
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "transition-experiment-status":
        try:
            result = experiments.transition_experiment_status(
                args.experiment_id,
                to_status=args.status,
                changed_by=args.changed_by,
                reason=args.reason,
            )
        except ExperimentServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "run-backtest":
        try:
            assumptions = json.loads(args.assumptions_json)
            result = backtests.run_backtest(
                args.experiment_id,
                dataset_id=args.dataset_id,
                assumptions=assumptions,
            )
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --assumptions-json payload: {exc}")
        except (BacktestServiceError, ExperimentServiceError) as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "get-backtest-artifacts":
        try:
            result = backtests.get_backtest_artifacts(args.run_id)
        except BacktestServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "explain-backtest-failure":
        try:
            result = backtests.explain_backtest_failure(args.run_id)
        except BacktestServiceError as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "score-experiment":
        try:
            result = evaluator.score_experiment(
                args.experiment_id,
                run_id=args.run_id,
            )
        except (BacktestServiceError, EvaluationServiceError, ExperimentServiceError) as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "check-promotion-eligibility":
        try:
            result = evaluator.check_promotion_eligibility(
                args.experiment_id,
                args.target_stage,
                run_id=args.run_id,
                changed_by=args.changed_by,
                promote=args.promote_if_eligible,
                min_out_of_sample_trades=args.min_out_of_sample_trades,
                min_distinct_markets=args.min_distinct_markets,
                max_drawdown_limit_usd=args.max_drawdown_limit_usd,
            )
        except (BacktestServiceError, EvaluationServiceError, ExperimentServiceError) as exc:
            parser.error(str(exc))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    if args.command == "issue-agent-credential":
        credential, raw_token = gateway.issue_read_only_credential(
            subject=args.subject,
            allowed_tools=None if args.allowed_tools is None else tuple(args.allowed_tools),
            rate_limit_count=args.rate_limit_count,
            rate_limit_window_seconds=args.rate_limit_window_seconds,
            token=args.token,
        )
        print(
            json.dumps(
                {
                    "allowed_tools": list(credential.allowed_tools),
                    "created_at": credential.created_at,
                    "credential_id": credential.credential_id,
                    "rate_limit_count": credential.rate_limit_count,
                    "rate_limit_window_seconds": credential.rate_limit_window_seconds,
                    "subject": credential.subject,
                    "token": raw_token,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "gateway-call":
        try:
            tool_arguments = json.loads(args.args_json)
            result = gateway.call_tool(
                args.tool_name,
                tool_arguments,
                token=args.token,
                user_id=args.user_id,
                session_id=args.session_id,
            )
        except json.JSONDecodeError as exc:
            parser.error(f"invalid --args-json payload: {exc}")
        except AgentGatewayError as exc:
            print(
                json.dumps(
                    {
                        "error": {
                            "code": exc.code,
                            "message": str(exc),
                        },
                        "ok": False,
                        "tool_name": args.tool_name,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
