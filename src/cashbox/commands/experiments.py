from __future__ import annotations

from .base import CLIContext, parse_json_argument, register_command
from ..experiments import EXPERIMENT_STATUSES, ExperimentFilter, ExperimentServiceError


def register(subparsers: object) -> None:
    register_command(
        subparsers,
        name="list-strategy-families",
        help_text="List supported experiment strategy families.",
        handler=_list_strategy_families,
    )

    template = register_command(
        subparsers,
        name="get-strategy-template",
        help_text="Read the schema template for a strategy family.",
        handler=_get_strategy_template,
    )
    template.add_argument("strategy_family")

    validate = register_command(
        subparsers,
        name="validate-strategy-config",
        help_text="Validate a strategy config payload.",
        handler=_validate_strategy_config,
    )
    validate.add_argument("strategy_family")
    validate.add_argument("--config-json", required=True)

    create_experiment = register_command(
        subparsers,
        name="create-experiment",
        help_text="Create an immutable experiment definition with an initial DRAFT lifecycle event.",
        handler=_create_experiment,
    )
    create_experiment.add_argument("--hypothesis", required=True)
    create_experiment.add_argument("--strategy-family", required=True)
    create_experiment.add_argument("--config-json", required=True)
    create_experiment.add_argument("--dataset-id", required=True)
    create_experiment.add_argument("--code-version", required=True)
    create_experiment.add_argument("--generated-by", required=True)

    clone_experiment = register_command(
        subparsers,
        name="clone-experiment",
        help_text="Clone an experiment into a new immutable definition with optional modifications.",
        handler=_clone_experiment,
    )
    clone_experiment.add_argument("experiment_id")
    clone_experiment.add_argument("--modifications-json", default="{}")
    clone_experiment.add_argument("--generated-by", required=True)

    note = register_command(
        subparsers,
        name="attach-research-note",
        help_text="Attach an append-only markdown note to an experiment.",
        handler=_attach_research_note,
    )
    note.add_argument("experiment_id")
    note.add_argument("--author", required=True)
    note.add_argument("--markdown", required=True)

    experiments = register_command(
        subparsers,
        name="list-experiments",
        help_text="List experiments from the local registry.",
        handler=_list_experiments,
    )
    experiments.add_argument("--strategy-family")
    experiments.add_argument("--status", choices=EXPERIMENT_STATUSES)
    experiments.add_argument("--generated-by")
    experiments.add_argument("--dataset-id")
    experiments.add_argument("--limit", type=int)

    experiment = register_command(
        subparsers,
        name="get-experiment",
        help_text="Read an experiment definition and lifecycle history.",
        handler=_get_experiment,
    )
    experiment.add_argument("experiment_id")

    transition = register_command(
        subparsers,
        name="transition-experiment-status",
        help_text="Append a status transition event for an experiment.",
        handler=_transition_experiment_status,
    )
    transition.add_argument("experiment_id")
    transition.add_argument("--status", required=True, choices=EXPERIMENT_STATUSES)
    transition.add_argument("--changed-by", required=True)
    transition.add_argument("--reason")


def _list_strategy_families(context: CLIContext, args: object) -> int:
    return context.emit(context.experiments.list_strategy_families())


def _get_strategy_template(context: CLIContext, args: object) -> int:
    try:
        result = context.experiments.get_strategy_template(args.strategy_family)
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _validate_strategy_config(context: CLIContext, args: object) -> int:
    config = parse_json_argument(context, args.config_json, flag_name="--config-json")
    try:
        result = context.experiments.validate_strategy_config(args.strategy_family, config)
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _create_experiment(context: CLIContext, args: object) -> int:
    config = parse_json_argument(context, args.config_json, flag_name="--config-json")
    try:
        result = context.experiments.create_experiment(
            hypothesis=args.hypothesis,
            strategy_family=args.strategy_family,
            config=config,
            dataset_id=args.dataset_id,
            code_version=args.code_version,
            generated_by=args.generated_by,
        )
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _clone_experiment(context: CLIContext, args: object) -> int:
    modifications = parse_json_argument(context, args.modifications_json, flag_name="--modifications-json")
    try:
        result = context.experiments.clone_experiment(
            args.experiment_id,
            modifications,
            generated_by=args.generated_by,
        )
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _attach_research_note(context: CLIContext, args: object) -> int:
    try:
        result = context.experiments.attach_research_note(
            args.experiment_id,
            author=args.author,
            markdown=args.markdown,
        )
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _list_experiments(context: CLIContext, args: object) -> int:
    result = context.experiments.list_experiments(
        ExperimentFilter(
            strategy_family=args.strategy_family,
            status=args.status,
            generated_by=args.generated_by,
            dataset_id=args.dataset_id,
            limit=args.limit,
        )
    )
    return context.emit(result)


def _get_experiment(context: CLIContext, args: object) -> int:
    try:
        result = context.experiments.get_experiment(args.experiment_id)
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)


def _transition_experiment_status(context: CLIContext, args: object) -> int:
    try:
        result = context.experiments.transition_experiment_status(
            args.experiment_id,
            to_status=args.status,
            changed_by=args.changed_by,
            reason=args.reason,
        )
    except ExperimentServiceError as exc:
        context.fail(str(exc))
    return context.emit(result)
