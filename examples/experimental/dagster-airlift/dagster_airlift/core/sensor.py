from datetime import timedelta
from typing import Any, Iterable, Iterator, List, Mapping, Optional, Sequence, Set, Tuple

from dagster import (
    AssetCheckKey,
    AssetKey,
    AssetMaterialization,
    DefaultSensorStatus,
    JsonMetadataValue,
    MarkdownMetadataValue,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    TimestampMetadataValue,
    _check as check,
    sensor,
)
from dagster._core.definitions.asset_selection import AssetSelection
from dagster._core.definitions.definitions_class import Definitions
from dagster._core.definitions.repository_definition.repository_definition import (
    RepositoryDefinition,
)
from dagster._grpc.client import DEFAULT_SENSOR_GRPC_TIMEOUT
from dagster._record import record
from dagster._serdes import deserialize_value, serialize_value
from dagster._serdes.serdes import whitelist_for_serdes
from dagster._time import datetime_from_timestamp, get_current_datetime, get_current_timestamp

from dagster_airlift.constants import EFFECTIVE_TIMESTAMP_METADATA_KEY
from dagster_airlift.core.airflow_defs_data import AirflowDefinitionsData
from dagster_airlift.core.airflow_instance import AirflowInstance, DagRun, TaskInstance

MAIN_LOOP_TIMEOUT_SECONDS = DEFAULT_SENSOR_GRPC_TIMEOUT - 20
DEFAULT_AIRFLOW_SENSOR_INTERVAL_SECONDS = 1
START_LOOKBACK_SECONDS = 60  # Lookback one minute in time for the initial setting of the cursor.


@whitelist_for_serdes
@record
class AirflowPollingSensorCursor:
    """A cursor that stores the last effective timestamp and the last polled dag id."""

    end_date_gte: Optional[float] = None
    end_date_lte: Optional[float] = None
    dag_query_offset: Optional[int] = None


def check_keys_for_asset_keys(
    repository_def: RepositoryDefinition, asset_keys: Set[AssetKey]
) -> Iterable[AssetCheckKey]:
    for assets_def in repository_def.asset_graph.assets_defs:
        for check_spec in assets_def.check_specs:
            if check_spec.asset_key in asset_keys:
                yield check_spec.key


def build_airflow_polling_sensor_defs(
    airflow_instance: AirflowInstance,
    airflow_data: AirflowDefinitionsData,
    minimum_interval_seconds: int = DEFAULT_AIRFLOW_SENSOR_INTERVAL_SECONDS,
) -> Definitions:
    @sensor(
        name="airflow_dag_status_sensor",
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=DefaultSensorStatus.RUNNING,
        # This sensor will only ever execute asset checks and not asset materializations.
        asset_selection=AssetSelection.all_asset_checks(),
    )
    def airflow_dag_sensor(context: SensorEvaluationContext) -> SensorResult:
        """Sensor to report materialization events for each asset as new runs come in."""
        try:
            cursor = (
                deserialize_value(context.cursor, AirflowPollingSensorCursor)
                if context.cursor
                else AirflowPollingSensorCursor()
            )
        except Exception as e:
            context.log.info(f"Failed to interpret cursor. Starting from scratch. Error: {e}")
            cursor = AirflowPollingSensorCursor()
        current_date = get_current_datetime()
        current_dag_offset = cursor.dag_query_offset or 0
        end_date_gte = (
            cursor.end_date_gte
            or (current_date - timedelta(seconds=START_LOOKBACK_SECONDS)).timestamp()
        )
        end_date_lte = cursor.end_date_lte or current_date.timestamp()
        sensor_iter = materializations_and_requests_from_batch_iter(
            end_date_gte=end_date_gte,
            end_date_lte=end_date_lte,
            offset=current_dag_offset,
            airflow_instance=airflow_instance,
            airflow_data=airflow_data,
        )
        all_materializations: List[Tuple[float, AssetMaterialization]] = []
        all_check_keys: Set[AssetCheckKey] = set()
        latest_offset = current_dag_offset
        repository_def = check.not_none(context.repository_def)
        while get_current_datetime() - current_date < timedelta(seconds=MAIN_LOOP_TIMEOUT_SECONDS):
            batch_result = next(sensor_iter, None)
            if batch_result is None:
                break
            all_materializations.extend(batch_result.materializations_and_timestamps)

            all_check_keys.update(
                check_keys_for_asset_keys(repository_def, batch_result.all_asset_keys_materialized)
            )
            latest_offset = batch_result.idx

        if batch_result is not None:
            new_cursor = AirflowPollingSensorCursor(
                end_date_gte=end_date_gte,
                end_date_lte=end_date_lte,
                dag_query_offset=latest_offset + 1,
            )
        else:
            # We have completed iteration for this range
            new_cursor = AirflowPollingSensorCursor(
                end_date_gte=end_date_lte,
                end_date_lte=None,
                dag_query_offset=0,
            )
        context.update_cursor(serialize_value(new_cursor))

        return SensorResult(
            asset_events=sorted_asset_events(all_materializations, repository_def),
            run_requests=[RunRequest(asset_check_keys=list(all_check_keys))]
            if all_check_keys
            else None,
        )

    return Definitions(sensors=[airflow_dag_sensor])


def sorted_asset_events(
    all_materializations: List[Tuple[float, AssetMaterialization]],
    repository_def: RepositoryDefinition,
) -> List[AssetMaterialization]:
    """Sort materializations by end date and toposort order."""
    topo_aks = repository_def.asset_graph.toposorted_asset_keys
    return [
        sorted_mat[1]
        for sorted_mat in sorted(
            all_materializations, key=lambda x: (x[0], topo_aks.index(x[1].asset_key))
        )
    ]


@record
class BatchResult:
    idx: int
    materializations_and_timestamps: List[Tuple[float, AssetMaterialization]]
    all_asset_keys_materialized: Set[AssetKey]


def materializations_and_requests_from_batch_iter(
    end_date_gte: float,
    end_date_lte: float,
    offset: int,
    airflow_instance: AirflowInstance,
    airflow_data: AirflowDefinitionsData,
) -> Iterator[Optional[BatchResult]]:
    runs = airflow_instance.get_dag_runs_batch(
        dag_ids=list(airflow_data.all_dag_ids),
        end_date_gte=datetime_from_timestamp(end_date_gte),
        end_date_lte=datetime_from_timestamp(end_date_lte),
        offset=offset,
    )
    for i, dag_run in enumerate(runs):
        mats: List[AssetMaterialization] = []
        mats.extend(materializations_for_dag_run(dag_run, airflow_data))
        for task_run in airflow_instance.get_task_instance_batch(
            run_id=dag_run.run_id,
            dag_id=dag_run.dag_id,
            # We need to make sure to ignore tasks that have already been proxied.
            task_ids=[
                task_id
                for task_id in airflow_data.task_ids_in_dag(dag_run.dag_id)
                if not airflow_data.proxied_state_for_task(dag_run.dag_id, task_id)
            ],
            states=["success"],
        ):
            mats.extend(
                materializations_for_task_instance(
                    airflow_data=airflow_data, dag_run=dag_run, task_instance=task_run
                )
            )
        all_asset_keys_materialized = {mat.asset_key for mat in mats}
        yield (
            BatchResult(
                idx=i + offset,
                materializations_and_timestamps=[
                    (get_timestamp_from_materialization(mat), mat) for mat in mats
                ],
                all_asset_keys_materialized=all_asset_keys_materialized,
            )
            if mats
            else None
        )


def get_timestamp_from_materialization(mat: AssetMaterialization) -> float:
    return check.float_param(
        mat.metadata[EFFECTIVE_TIMESTAMP_METADATA_KEY].value, "Materialization Effective Timestamp"
    )


def materializations_for_dag_run(
    dag_run: DagRun, airflow_data: AirflowDefinitionsData
) -> Sequence[AssetMaterialization]:
    return [
        AssetMaterialization(
            asset_key=airflow_data.asset_key_for_dag(dag_run.dag_id),
            description=dag_run.note,
            metadata=get_dag_run_metadata(dag_run),
        )
    ]


def get_dag_run_metadata(dag_run: DagRun) -> Mapping[str, Any]:
    return {
        **get_common_metadata(dag_run),
        "Run Details": MarkdownMetadataValue(f"[View Run]({dag_run.url})"),
        "Start Date": TimestampMetadataValue(dag_run.start_date),
        "End Date": TimestampMetadataValue(dag_run.end_date),
        EFFECTIVE_TIMESTAMP_METADATA_KEY: TimestampMetadataValue(dag_run.end_date),
    }


def get_common_metadata(dag_run: DagRun) -> Mapping[str, Any]:
    return {
        "Airflow Run ID": dag_run.run_id,
        "Run Metadata (raw)": JsonMetadataValue(dag_run.metadata),
        "Run Type": dag_run.run_type,
        "Airflow Config": JsonMetadataValue(dag_run.config),
        "Creation Timestamp": TimestampMetadataValue(get_current_timestamp()),
    }


def get_task_instance_metadata(dag_run: DagRun, task_instance: TaskInstance) -> Mapping[str, Any]:
    return {
        **get_common_metadata(dag_run),
        "Run Details": MarkdownMetadataValue(f"[View Run]({task_instance.details_url})"),
        "Task Logs": MarkdownMetadataValue(f"[View Logs]({task_instance.log_url})"),
        "Start Date": TimestampMetadataValue(task_instance.start_date),
        "End Date": TimestampMetadataValue(task_instance.end_date),
        EFFECTIVE_TIMESTAMP_METADATA_KEY: TimestampMetadataValue(task_instance.end_date),
    }


def materializations_for_task_instance(
    airflow_data: AirflowDefinitionsData,
    dag_run: DagRun,
    task_instance: TaskInstance,
) -> Sequence[AssetMaterialization]:
    mats = []
    asset_keys = airflow_data.asset_keys_in_task(dag_run.dag_id, task_instance.task_id)
    for asset_key in asset_keys:
        mats.append(
            AssetMaterialization(
                asset_key=asset_key,
                description=task_instance.note,
                metadata=get_task_instance_metadata(dag_run, task_instance),
            )
        )
    return mats
