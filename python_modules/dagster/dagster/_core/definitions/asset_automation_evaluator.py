from abc import ABC, abstractmethod
from typing import List, NamedTuple, Optional, Sequence

from dagster._core.definitions.asset_daemon_cursor import AssetDaemonAssetCursor
from dagster._core.definitions.events import AssetKey

from .asset_automation_condition import AssetAutomationConditionEvaluationContext
from .asset_automation_condition_cursor import AssetAutomationConditionCursor
from .asset_subset import AssetSubset


class AssetAutomationConditionSnapshot(NamedTuple):
    """A serializable snapshot of an AssetAutomationCondition."""

    class_name: str
    description: Optional[str]


class ConditionEvaluation(NamedTuple):
    """Internal representation of the results of evaluating a node in the evaluation tree."""

    condition_snapshot: "AssetAutomationConditionSnapshot"
    true_subset: AssetSubset
    candidate_subset: AssetSubset
    child_evaluations: Sequence["ConditionEvaluation"] = []

    def for_child(self, child_condition: "AutomationCondition") -> Optional["ConditionEvaluation"]:
        for child_evaluation in self.child_evaluations:
            if child_evaluation.condition_snapshot == child_condition.to_snapshot():
                return child_evaluation
        return None

    def is_equivalent(self, other: Optional["ConditionEvaluation"]) -> bool:
        return (
            other is not None
            and self.condition_snapshot == other.condition_snapshot
            and self.true_subset == other.true_subset
            and self.candidate_subset == other.candidate_subset
            and len(self.child_evaluations) == len(other.child_evaluations)
            and all(
                self.child_evaluations[i].is_equivalent(other.child_evaluations[i])
                for i in range(len(self.child_evaluations))
            )
        )


class ConditionEvaluationResult(NamedTuple):
    asset_key: AssetKey
    evaluation: ConditionEvaluation
    cursor: AssetAutomationConditionCursor

    @property
    def true_subset(self) -> AssetSubset:
        return self.evaluation.true_subset

    @staticmethod
    def create(
        context: AssetAutomationConditionEvaluationContext,
        true_subset: AssetSubset,
        cursor: Optional[AssetAutomationConditionCursor] = None,
        child_results: Optional[Sequence["ConditionEvaluationResult"]] = None,
    ) -> "ConditionEvaluationResult":
        condition_snapshot = context.condition.to_snapshot()
        return ConditionEvaluationResult(
            asset_key=context.asset_key,
            evaluation=ConditionEvaluation(
                condition_snapshot=condition_snapshot,
                true_subset=true_subset,
                candidate_subset=context.candidates_subset,
                child_evaluations=[result.evaluation for result in child_results or []],
            ),
            cursor=cursor
            or AssetAutomationConditionCursor(
                condition_snapshot=condition_snapshot,
                child_cursors=[result.cursor for result in child_results or []],
                cursor_value=None,
            ),
        )

    def to_asset_cursor(self) -> AssetDaemonAssetCursor:
        return AssetDaemonAssetCursor(
            asset_key=self.asset_key,
            latest_evaluation=self.evaluation,
            condition_cursor=self.cursor,
        )


class AutomationCondition(ABC):
    @property
    def children(self) -> Sequence["AutomationCondition"]:
        return []

    @property
    def description(self) -> Optional[str]:
        return None

    @abstractmethod
    def evaluate(
        self, context: AssetAutomationConditionEvaluationContext
    ) -> ConditionEvaluationResult:
        ...

    def to_snapshot(self) -> AssetAutomationConditionSnapshot:
        return AssetAutomationConditionSnapshot(
            class_name=self.__class__.__name__, description=self.description
        )

    def __and__(self, other: "AutomationCondition") -> "AutomationCondition":
        # group AndAutomationConditions together
        if isinstance(self, AndAutomationCondition):
            return AndAutomationCondition(children=[*self.children, other])
        return AndAutomationCondition(children=[self, other])

    def __or__(self, other: "AutomationCondition") -> "AutomationCondition":
        # group OrAutomationConditions together
        if isinstance(self, OrAutomationCondition):
            return OrAutomationCondition(children=[*self.children, other])
        return OrAutomationCondition(children=[self, other])

    def __invert__(self) -> "AutomationCondition":
        if isinstance(self, OrAutomationCondition):
            return NorAutomationCondition(children=self.children)
        return NorAutomationCondition(children=[self])


class AndAutomationCondition(
    AutomationCondition,
    NamedTuple("_AndAutomationCondition", [("children", Sequence[AutomationCondition])]),
):
    def evaluate(
        self, context: AssetAutomationConditionEvaluationContext
    ) -> ConditionEvaluationResult:
        child_results = []
        true_subset = context.candidates_subset
        for child_condition in self.children:
            child_context = context.for_child(
                condition=child_condition, candidates_subset=true_subset
            )
            child_result = child_condition.evaluate(child_context)
            child_results.append(child_result)
            true_subset &= child_result.true_subset
        return ConditionEvaluationResult.create(context, true_subset, child_results=child_results)


class OrAutomationCondition(
    AutomationCondition,
    NamedTuple("_OrAutomationCondition", [("children", Sequence[AutomationCondition])]),
):
    def evaluate(
        self, context: AssetAutomationConditionEvaluationContext
    ) -> ConditionEvaluationResult:
        child_results: List[ConditionEvaluationResult] = []
        true_subset = context.asset_context.empty_subset()
        for child_condition in self.children:
            child_context = context.for_child(
                condition=child_condition, candidates_subset=context.candidates_subset
            )
            child_result = child_condition.evaluate(child_context)
            child_results.append(child_result)
            true_subset |= child_result.true_subset
        return ConditionEvaluationResult.create(context, true_subset, child_results=child_results)


class NorAutomationCondition(
    AutomationCondition,
    NamedTuple("_NorAutomationCondition", [("children", Sequence[AutomationCondition])]),
):
    def evaluate(
        self, context: AssetAutomationConditionEvaluationContext
    ) -> ConditionEvaluationResult:
        child_results: List[ConditionEvaluationResult] = []
        true_subset = context.candidates_subset
        for child_condition in self.children:
            child_context = context.for_child(
                condition=child_condition, candidates_subset=context.candidates_subset
            )
            child_result = child_condition.evaluate(child_context)
            child_results.append(child_result)
            true_subset -= child_result.true_subset
        return ConditionEvaluationResult.create(context, true_subset, child_results=child_results)


############
# NOT BOOLEAN STUFF
############

...
