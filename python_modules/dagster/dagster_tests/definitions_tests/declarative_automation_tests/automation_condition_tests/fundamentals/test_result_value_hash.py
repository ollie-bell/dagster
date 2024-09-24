import pytest
from dagster import (
    AssetSpec,
    # doing this rename to make the test cases fit on a single line for readability
    AutomationCondition as SC,
    DailyPartitionsDefinition,
)

from dagster_tests.definitions_tests.declarative_automation_tests.scenario_utils.automation_condition_scenario import (
    AutomationConditionScenarioState,
)
from dagster_tests.definitions_tests.declarative_automation_tests.scenario_utils.base_scenario import (
    run_request,
)
from dagster_tests.definitions_tests.declarative_automation_tests.scenario_utils.scenario_specs import (
    ScenarioSpec,
)

one_parent = ScenarioSpec(asset_specs=[AssetSpec("A"), AssetSpec("downstream", deps=["A"])])
two_parents = ScenarioSpec(
    asset_specs=[AssetSpec("A"), AssetSpec("B"), AssetSpec("downstream", deps=["A", "B"])]
)

daily_partitions = DailyPartitionsDefinition(start_date="2020-01-01")
one_parent_daily = one_parent.with_asset_properties(partitions_def=daily_partitions)
two_parents_daily = two_parents.with_asset_properties(partitions_def=daily_partitions)


@pytest.mark.parametrize(
    ["expected_value_hash", "condition", "scenario_spec", "materialize_A"],
    [
        # cron condition returns a unique value hash if parents change, if schedule changes, if the
        # partitions def changes, or if an asset is materialized
        ("355aa63710b58d612e31720814166598", SC.on_cron("0 * * * *"), one_parent, False),
        ("b00de3e41220dd2bd15578a2cf7a6bdc", SC.on_cron("0 * * * *"), one_parent, True),
        ("8af5e92d74e43b29cbbd24f648fdf0a0", SC.on_cron("0 0 * * *"), one_parent, False),
        ("5e450e48dc778bbdb754f5da3469293d", SC.on_cron("0 * * * *"), one_parent_daily, False),
        ("fd782a2f407c8c1667f36dd5ab94829c", SC.on_cron("0 * * * *"), two_parents, False),
        ("d224b2798a9dfe65891441a4298b26a0", SC.on_cron("0 * * * *"), two_parents_daily, False),
        # same as above
        ("6b220f1f942df6f50d6f6ca8fce001ee", SC.eager(), one_parent, False),
        ("85905133c3361fc98522ad4739cd2a71", SC.eager(), one_parent, True),
        ("f8c7147ae0876e95ba5570a868bdba20", SC.eager(), one_parent_daily, False),
        ("416cd3051774347e21f81b69f35b81bb", SC.eager(), two_parents, False),
        ("73d03cd3ae641723bf9aad0af18a4702", SC.eager(), two_parents_daily, False),
        # missing condition is invariant to changes other than partitions def changes
        ("5c24ffc21af9983a4917b91290de8f5d", SC.missing(), one_parent, False),
        ("5c24ffc21af9983a4917b91290de8f5d", SC.missing(), one_parent, True),
        ("5c24ffc21af9983a4917b91290de8f5d", SC.missing(), two_parents, False),
        ("c722c1abf97c5f5fe13b2f6fc00af739", SC.missing(), two_parents_daily, False),
        ("c722c1abf97c5f5fe13b2f6fc00af739", SC.missing(), one_parent_daily, False),
    ],
)
def test_value_hash(
    condition: SC, scenario_spec: ScenarioSpec, expected_value_hash: str, materialize_A: bool
) -> None:
    state = AutomationConditionScenarioState(
        scenario_spec, automation_condition=condition
    ).with_current_time("2024-01-01T00:00")

    state, _ = state.evaluate("downstream")
    if materialize_A:
        state = state.with_runs(run_request("A"))

    state, result = state.evaluate("downstream")
    assert result.value_hash == expected_value_hash
