from datetime import datetime, timedelta, timezone

import mock
import pytest
import pytz
from dagster import (
    AssetCheckKey,
    AssetKey,
    AssetSpec,
    Definitions,
    SensorResult,
    asset_check,
    build_sensor_context,
)
from dagster._check.functions import CheckError
from dagster._core.definitions.events import AssetMaterialization
from dagster._core.definitions.time_window_partitions import DailyPartitionsDefinition
from dagster._core.test_utils import freeze_time
from dagster._serdes import deserialize_value
from dagster_airlift.core import dag_defs, task_defs
from dagster_airlift.core.load_defs import build_defs_from_airflow_instance
from dagster_airlift.core.sensor import AirflowPollingSensorCursor
from dagster_airlift.test.airflow_test_instance import make_dag_run, make_instance

from dagster_airlift_tests.unit_tests.conftest import (
    assert_expected_key_order,
    build_and_invoke_sensor,
    fully_loaded_repo_from_airflow_asset_graph,
)


def make_dag_key(dag_id: str) -> AssetKey:
    return AssetKey(["test_instance", "dag", dag_id])


def make_dag_key_str(dag_id: str) -> str:
    return make_dag_key(dag_id).to_user_string()


def test_dag_and_task_metadata(init_load_context: None) -> None:
    """Test the metadata produced by a sensor for a single dag and task."""
    freeze_datetime = datetime(2021, 1, 1)

    with freeze_time(freeze_datetime):
        result, _ = build_and_invoke_sensor(
            assets_per_task={
                "dag": {"task": [("a", [])]},
            },
        )
        assert len(result.asset_events) == 2
        assert_expected_key_order(result.asset_events, ["a", make_dag_key_str("dag")])
        dag_mat = result.asset_events[1]
        expected_dag_metadata_keys = {
            "Airflow Run ID",
            "Start Date",
            "End Date",
            "Run Metadata (raw)",
            "Creation Timestamp",
            "Run Details",
            "Airflow Config",
            "Run Type",
            "dagster-airlift/effective_timestamp",
        }
        assert set(dag_mat.metadata.keys()) == expected_dag_metadata_keys
        task_mat = result.asset_events[0]
        expected_task_metadata_keys = {
            "Airflow Run ID",
            "Start Date",
            "End Date",
            "Run Metadata (raw)",
            "Creation Timestamp",
            "Run Details",
            "Task Logs",
            "Airflow Config",
            "Run Type",
            "dagster-airlift/effective_timestamp",
        }
        assert set(task_mat.metadata.keys()) == expected_task_metadata_keys

        assert task_mat.metadata["Airflow Run ID"].value == "run-dag"
        assert (
            task_mat.metadata["Start Date"].value
            == (freeze_datetime - timedelta(minutes=10)).timestamp()
        )
        assert (
            task_mat.metadata["End Date"].value
            == (freeze_datetime - timedelta(seconds=1)).timestamp()
        )
        assert task_mat.metadata["Creation Timestamp"].value == freeze_datetime.timestamp()
        assert (
            task_mat.metadata["Run Details"].value
            == "[View Run](http://dummy.domain/dags/dag/grid?dag_run_id=run-dag&task_id=task)"
        )
        assert (
            task_mat.metadata["Task Logs"].value
            == "[View Logs](http://dummy.domain/dags/dag/grid?dag_run_id=run-dag&task_id=task&tab=logs)"
        )
        assert task_mat.metadata["Airflow Config"].value == {}
        assert task_mat.metadata["Run Type"].value == "manual"

        assert dag_mat.metadata["Airflow Run ID"].value == "run-dag"
        assert (
            dag_mat.metadata["Start Date"].value
            == (freeze_datetime - timedelta(minutes=10)).timestamp()
        )
        assert dag_mat.metadata["End Date"].value == freeze_datetime.timestamp()
        assert dag_mat.metadata["Creation Timestamp"].value == freeze_datetime.timestamp()
        assert (
            dag_mat.metadata["Run Details"].value
            == "[View Run](http://dummy.domain/dags/dag/grid?dag_run_id=run-dag&tab=details)"
        )
        assert dag_mat.metadata["Airflow Config"].value == {}
        assert dag_mat.metadata["Run Type"].value == "manual"


def test_interleaved_executions(init_load_context: None) -> None:
    """Test that the when task / dag completion is interleaved the correct ordering is preserved."""
    # Asset graph structure:
    #   a -> b where a and b are each in their own airflow tasks.
    #   c -> d where c and d are each in their own airflow tasks, in a different dag.
    freeze_datetime = datetime(2021, 1, 1)
    with freeze_time(freeze_datetime):
        result, context = build_and_invoke_sensor(
            assets_per_task={
                "dag1": {"task1": [("a", [])], "task2": [("b", ["a"])]},
                "dag2": {"task1": [("c", [])], "task2": [("d", ["c"])]},
            },
        )
        # We expect one asset materialization per asset.
        assert len(result.asset_events) == 6
        assert all(isinstance(event, AssetMaterialization) for event in result.asset_events)

        mats_order = [mat.asset_key.to_user_string() for mat in result.asset_events]
        # a should be before b
        assert mats_order.index("a") < mats_order.index("b")
        # c should be before d
        assert mats_order.index("c") < mats_order.index("d")
        # dag1 and dag2 should be after all task-mapped assets
        assert mats_order.index(make_dag_key_str("dag1")) >= 4
        assert mats_order.index(make_dag_key_str("dag2")) >= 4
        assert context.cursor
        cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert cursor.end_date_gte == freeze_datetime.timestamp()
        assert cursor.end_date_lte is None
        assert cursor.dag_query_offset == 0


def test_dependencies_within_tasks(init_load_context: None) -> None:
    """Test that a complex asset graph structure can be ingested in correct order from the sensor.
    Where a, b, and c are part of task 1, and d, e, and f are part of task 2.
    """
    # Asset graph structure:
    #   a
    #  / \
    # b   c
    #  \ /
    #   d
    #  / \
    # e   f
    freeze_datetime = datetime(2021, 1, 1)
    with freeze_time(freeze_datetime):
        result, context = build_and_invoke_sensor(
            assets_per_task={
                "dag": {
                    "task1": [("a", []), ("b", ["a"]), ("c", ["a"])],
                    "task2": [("d", ["b", "c"]), ("e", ["d"]), ("f", ["d"])],
                },
            },
        )
        assert len(result.asset_events) == 7
        assert_expected_key_order(
            result.asset_events, ["a", "b", "c", "d", "e", "f", make_dag_key_str("dag")]
        )
        assert context.cursor
        cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert cursor.end_date_gte == freeze_datetime.timestamp()
        assert cursor.end_date_lte is None
        assert cursor.dag_query_offset == 0


def test_outside_of_dag_dependency(init_load_context: None) -> None:
    """Test that if an asset has a transitive dependency on another asset within the same task, ordering is respected."""
    # a -> b -> c where a and c are in the same task, and b is not in any dag.
    freeze_datetime = datetime(2021, 1, 1)
    with freeze_time(freeze_datetime):
        result, context = build_and_invoke_sensor(
            assets_per_task={
                "dag": {"task": [("a", []), ("c", ["b"])]},
            },
            additional_defs=Definitions(assets=[AssetSpec(key="b", deps=["a"])]),
        )
        assert len(result.asset_events) == 3
        assert all(isinstance(event, AssetMaterialization) for event in result.asset_events)
        assert_expected_key_order(result.asset_events, ["a", "c", make_dag_key_str("dag")])
        assert context.cursor
        cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert cursor.end_date_gte == freeze_datetime.timestamp()
        assert cursor.end_date_lte is None
        assert cursor.dag_query_offset == 0


def test_request_asset_checks(init_load_context: None) -> None:
    """Test that when a new dag or task run is detected, a new check run is requested for all checks which may target that dag/task."""
    freeze_datetime = datetime(2021, 1, 1)

    dag_asset_key = make_dag_key("dag")

    @asset_check(asset="a")
    def check_task_asset():
        pass

    @asset_check(asset=dag_asset_key)
    def check_dag_asset():
        pass

    @asset_check(asset="c")
    def check_unrelated_asset():
        pass

    with freeze_time(freeze_datetime):
        result, context = build_and_invoke_sensor(
            assets_per_task={
                "dag": {"task": [("a", []), ("b", ["a"])]},
            },
            additional_defs=Definitions(
                asset_checks=[check_task_asset, check_dag_asset, check_unrelated_asset],
                assets=[AssetSpec(key="c")],
            ),
        )

        assert len(result.asset_events) == 3
        assert result.run_requests
        assert len(result.run_requests) == 1
        run_request = result.run_requests[0]
        assert run_request.asset_check_keys
        assert set(run_request.asset_check_keys) == {
            AssetCheckKey(name="check_task_asset", asset_key=AssetKey(["a"])),
            AssetCheckKey(name="check_dag_asset", asset_key=dag_asset_key),
        }
        assert context.cursor
        cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert cursor.end_date_gte == freeze_datetime.timestamp()
        assert cursor.end_date_lte is None
        assert cursor.dag_query_offset == 0


_CALLCOUNT = [0]


def _mock_get_current_datetime() -> datetime:
    if _CALLCOUNT[0] < 2:
        _CALLCOUNT[0] += 1
        return datetime(2021, 2, 1, tzinfo=timezone.utc)
    next_time = datetime(2021, 2, 1, tzinfo=timezone.utc) + timedelta(seconds=46 * _CALLCOUNT[0])
    _CALLCOUNT[0] += 1
    return next_time


def test_cursor(init_load_context: None) -> None:
    """Test expected cursor behavior for sensor."""
    asset_and_dag_structure = {
        "dag1": {"task1": [("a", [])]},
        "dag2": {"task1": [("b", [])]},
    }

    with freeze_time(datetime(2021, 1, 1, tzinfo=timezone.utc)):
        # First, run through a full successful iteration of the sensor. Expect time to move forward, and polled dag id to be None, since we completed iteration of all dags.
        # Then, run through a partial iteration of the sensor. We mock get_current_datetime to return a time after timeout passes iteration start after the first call, meaning we should pause iteration.
        repo_def = fully_loaded_repo_from_airflow_asset_graph(asset_and_dag_structure)
        sensor = next(iter(repo_def.sensor_defs))
        context = build_sensor_context(repository_def=repo_def)
        result = sensor(context)
        assert isinstance(result, SensorResult)
        assert context.cursor
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert new_cursor.end_date_gte == datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.end_date_lte is None
        assert new_cursor.dag_query_offset == 0

    with mock.patch(
        "dagster._time._mockable_get_current_datetime", wraps=_mock_get_current_datetime
    ):
        result = sensor(context)
        assert isinstance(result, SensorResult)
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        # We didn't advance to the next effective timestamp, since we didn't complete iteration
        assert new_cursor.end_date_gte == datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        # We have not yet moved forward
        assert new_cursor.end_date_lte == datetime(2021, 2, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.dag_query_offset == 1

        _CALLCOUNT[0] = 0
        # We weren't able to complete iteration, so we should pause iteration again
        result = sensor(context)
        assert isinstance(result, SensorResult)
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert new_cursor.end_date_gte == datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.end_date_lte == datetime(2021, 2, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.dag_query_offset == 2

        _CALLCOUNT[0] = 0
        # Now it should finish iteration.
        result = sensor(context)
        assert isinstance(result, SensorResult)
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert new_cursor.end_date_gte == datetime(2021, 2, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.end_date_lte is None
        assert new_cursor.dag_query_offset == 0


def test_legacy_cursor(init_load_context: None) -> None:
    """Test the case where a legacy/uninterpretable cursor is provided to the sensor execution."""
    freeze_datetime = datetime(2021, 1, 1, tzinfo=timezone.utc)
    with freeze_time(freeze_datetime):
        repo_def = fully_loaded_repo_from_airflow_asset_graph({"dag": {"task": [("a", [])]}})
        sensor = next(iter(repo_def.sensor_defs))
        context = build_sensor_context(
            repository_def=repo_def, cursor=str(freeze_datetime.timestamp())
        )
        result = sensor(context)
        assert isinstance(result, SensorResult)
        assert context.cursor
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert new_cursor.end_date_gte == datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.end_date_lte is None
        assert new_cursor.dag_query_offset == 0


def test_no_runs(init_load_context: None) -> None:
    """Test the case with no runs."""
    freeze_datetime = datetime(2021, 1, 1, tzinfo=timezone.utc)
    with freeze_time(freeze_datetime):
        repo_def = fully_loaded_repo_from_airflow_asset_graph(
            {"dag": {"task": [("a", [])]}},
            create_runs=False,
        )
        sensor = next(iter(repo_def.sensor_defs))
        context = build_sensor_context(repository_def=repo_def)
        result = sensor(context)
        assert isinstance(result, SensorResult)
        assert context.cursor
        new_cursor = deserialize_value(context.cursor, AirflowPollingSensorCursor)
        assert new_cursor.end_date_gte == datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp()
        assert new_cursor.end_date_lte is None
        assert new_cursor.dag_query_offset == 0
        assert not result.asset_events
        assert not result.run_requests


def test_default_time_partitioned_asset(init_load_context: None) -> None:
    """Test that a task instance for a time-partitioned asset is correctly ingested."""
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance(
            dag_and_task_structure={
                "dag": ["task"],
            },
            dag_runs=[
                make_dag_run(
                    dag_id="dag",
                    run_id="run-dag",
                    start_date=datetime(2021, 1, 2, 5, tzinfo=timezone.utc),
                    end_date=datetime(2021, 1, 2, 6, tzinfo=timezone.utc),
                    logical_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
                )
            ],
        ),
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                start_date=datetime(2021, 1, 1, tzinfo=timezone.utc)
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    with freeze_time(datetime(2021, 1, 2, 6, tzinfo=timezone.utc)):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        result = sensor(sensor_context)
        assert isinstance(result, SensorResult)
        assert len(result.asset_events) == 2
        assert_expected_key_order(result.asset_events, ["a", "airflow_instance/dag/dag"])
        a_asset_mat = result.asset_events[0]
        assert isinstance(a_asset_mat, AssetMaterialization)
        # We expect the partition to match the logical date.
        assert a_asset_mat.partition == "2021-01-01"


def test_timezones_mismatch_partitioned_asset(init_load_context: None) -> None:
    """We expect that timezones should match for the logical date and the passed-in partitions definition."""
    instance_with_pacific_run = make_instance(
        dag_and_task_structure={
            "dag": ["task"],
        },
        dag_runs=[
            make_dag_run(
                dag_id="dag",
                run_id="run-dag",
                start_date=datetime(2021, 1, 2, 5, tzinfo=pytz.timezone("US/Pacific")),
                end_date=datetime(2021, 1, 2, 6, tzinfo=pytz.timezone("US/Pacific")),
                logical_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("US/Pacific")),
            )
        ],
    )
    # We have a dag run whose logical date aligns with the partition definition, except for the timezone.
    # As a result, we should expect an error, since there is no partition window bounded at this exact timestamp.
    defs = build_defs_from_airflow_instance(
        airflow_instance=instance_with_pacific_run,
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("US/Eastern")),
                                timezone="US/Eastern",
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    # Freeze at a time s.t. the run will be processed.
    with freeze_time(datetime(2021, 1, 2, 6, tzinfo=pytz.timezone("US/Pacific"))):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        with pytest.raises(CheckError, match="does not match"):
            sensor(sensor_context)


def test_before_start_of_partitioned_asset(init_load_context: None) -> None:
    """We expect to throw an error if there is no matching partition after the start date."""
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance(
            dag_and_task_structure={
                "dag": ["task"],
            },
            dag_runs=[
                make_dag_run(
                    dag_id="dag",
                    run_id="run-dag",
                    start_date=datetime(2021, 1, 1, 5, tzinfo=timezone.utc),
                    end_date=datetime(2021, 1, 1, 6, tzinfo=timezone.utc),
                    logical_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
                )
            ],
        ),
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                # Partitions definition starts after the logical date.
                                start_date=datetime(2021, 1, 2, tzinfo=timezone.utc)
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    with freeze_time(datetime(2021, 1, 1, 6, tzinfo=timezone.utc)):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        with pytest.raises(CheckError, match="before the start of the partitions definition"):
            sensor(sensor_context)


def test_logical_date_mismatch(init_load_context: None) -> None:
    """Test a logical date which does not align with the partition definition due to date mismatch."""
    defs = build_defs_from_airflow_instance(
        airflow_instance=make_instance(
            dag_and_task_structure={
                "dag": ["task"],
            },
            dag_runs=[
                make_dag_run(
                    dag_id="dag",
                    run_id="run-dag",
                    start_date=datetime(2021, 1, 1, 5, tzinfo=timezone.utc),
                    end_date=datetime(2021, 1, 1, 6, tzinfo=timezone.utc),
                    # Logical date isn't aligned with midnight.
                    logical_date=datetime(2021, 1, 1, 3, tzinfo=timezone.utc),
                )
            ],
        ),
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                start_date=datetime(2021, 1, 1, tzinfo=timezone.utc)
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    with freeze_time(datetime(2021, 1, 1, 6, tzinfo=timezone.utc)):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        with pytest.raises(CheckError, match="match a partition"):
            sensor(sensor_context)


def test_partition_offset_mismatch(init_load_context: None) -> None:
    """Test that partition offsets are respected when determining the partition corresponding to a logical date."""
    instance = make_instance(
        dag_and_task_structure={
            "dag": ["task"],
        },
        dag_runs=[
            make_dag_run(
                dag_id="dag",
                run_id="run-dag",
                start_date=datetime(2021, 1, 1, 5, tzinfo=timezone.utc),
                end_date=datetime(2021, 1, 1, 6, tzinfo=timezone.utc),
                # Logical date is 3 AM.
                logical_date=datetime(2021, 1, 1, 3, tzinfo=timezone.utc),
            )
        ],
    )

    defs = build_defs_from_airflow_instance(
        airflow_instance=instance,
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                start_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
                                hour_offset=6,
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    # Due to differing hours offset, we expect an error to throw.
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    with freeze_time(datetime(2021, 1, 1, 6, tzinfo=timezone.utc)):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        with pytest.raises(CheckError, match="match a partition"):
            sensor(sensor_context)

    # now, align the offset and expect success.
    defs = build_defs_from_airflow_instance(
        airflow_instance=instance,
        defs=dag_defs(
            "dag",
            task_defs(
                "task",
                Definitions(
                    assets=[
                        AssetSpec(
                            key="a",
                            partitions_def=DailyPartitionsDefinition(
                                start_date=datetime(2021, 1, 1, tzinfo=timezone.utc),
                                hour_offset=3,
                            ),
                        )
                    ],
                ),
            ),
        ),
    )
    assert defs.sensors
    sensor = next(iter(defs.sensors))
    with freeze_time(datetime(2021, 1, 1, 6, tzinfo=timezone.utc)):
        sensor_context = build_sensor_context(repository_def=defs.get_repository_def())
        result = sensor(sensor_context)
        assert isinstance(result, SensorResult)
        assert len(result.asset_events) == 2
        assert_expected_key_order(result.asset_events, ["a", "airflow_instance/dag/dag"])
        a_asset_mat = result.asset_events[0]
        assert isinstance(a_asset_mat, AssetMaterialization)
        # We expect the partition to match the logical date.
        assert a_asset_mat.partition == "2021-01-01"
