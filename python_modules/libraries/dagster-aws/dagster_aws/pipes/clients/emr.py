from typing import TYPE_CHECKING, Any, Dict, Optional

import boto3
import dagster._check as check
from dagster import PipesClient
from dagster._annotations import public
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.utils import PipesEnvContextInjector, open_pipes_session

from dagster_aws.pipes.message_readers import PipesS3MessageReader

if TYPE_CHECKING:
    from mypy_boto3_emr import EMRClient
    from mypy_boto3_emr.literals import ClusterStateType
    from mypy_boto3_emr.type_defs import (
        DescribeClusterOutputTypeDef,
        RunJobFlowInputRequestTypeDef,
        RunJobFlowOutputTypeDef,
    )

AWS_SERVICE_NAME = "EMR"
AWS_SERVICE_NAME_LOWER = AWS_SERVICE_NAME.lower()
AWS_SERVICE_NAME_LOWER_KEBAB = AWS_SERVICE_NAME_LOWER.replace("_", "-")
BOTO3_START_METHOD = "run_job_flow"


class PipesEMRClient(
    PipesClient,
    TreatAsResourceParam,
):
    f"""A pipes client for running workloads on AWS {AWS_SERVICE_NAME}.

    Args:
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the {AWS_SERVICE_NAME} workload.
                Recommended to use :py:class:`PipesS3MessageReader` with `expect_s3_message_writer` set to `True`.
        client (Optional[boto3.client]): The boto3 {AWS_SERVICE_NAME} client used to interact with AWS {AWS_SERVICE_NAME}.
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into AWS {AWS_SERVICE_NAME} workload. Defaults to :py:class:`PipesEnvContextInjector`.
        forward_termination (bool): Whether to cancel the {AWS_SERVICE_NAME} workload if the Dagster process receives a termination signal.
    """

    AWS_SERVICE_NAME = AWS_SERVICE_NAME

    def __init__(
        self,
        message_reader: PipesMessageReader,
        client=None,
        context_injector: Optional[PipesContextInjector] = None,
        forward_termination: bool = True,
    ):
        self._client = client or boto3.client("emr")
        self._message_reader = message_reader
        self._context_injector = context_injector or PipesEnvContextInjector()
        self.forward_termination = check.bool_param(forward_termination, "forward_termination")

    @property
    def client(self) -> "EMRClient":
        return self._client

    @property
    def context_injector(self) -> PipesContextInjector:
        return self._context_injector

    @property
    def message_reader(self) -> PipesMessageReader:
        return self._message_reader

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @public
    def run(
        self,
        *,
        context: OpExecutionContext,
        run_job_flow_params: "RunJobFlowInputRequestTypeDef",
        extras: Optional[Dict[str, Any]] = None,
    ) -> PipesClientCompletedInvocation:
        f"""Run a workload on AWS {AWS_SERVICE_NAME}, enriched with the pipes protocol.

            Args:
                context (OpExecutionContext): The context of the currently executing Dagster op or asset.
                params (dict): Parameters for the ``{BOTO3_START_METHOD}`` boto3 {AWS_SERVICE_NAME} client call.
                    See `Boto3 API Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/{AWS_SERVICE_NAME_LOWER}/client/{AWS_SERVICE_NAME_LOWER}.html#{AWS_SERVICE_NAME_LOWER_KEBAB}>`_
                extras (Optional[Dict[str, Any]]): Additional information to pass to the Pipes session in the external process.

            Returns:
                PipesClientCompletedInvocation: Wrapper containing results reported by the external
                process.
            """
        with open_pipes_session(
            context=context,
            message_reader=self.message_reader,
            context_injector=self.context_injector,
            extras=extras,
        ) as session:
            start_response = self._start(context, run_job_flow_params)
            try:
                wait_response = self._wait_for_completion(context, start_response)
                context.log.info(f"[pipes] {self.AWS_SERVICE_NAME} workload is complete!")
                self._read_messages(context, wait_response)
                return PipesClientCompletedInvocation(session)

            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.warning(
                        f"[pipes] Dagster process interrupted! Will terminate external {self.AWS_SERVICE_NAME} workload."
                    )
                    self._terminate(context, start_response)
                raise

    def _start(
        self, context: OpExecutionContext, params: "RunJobFlowInputRequestTypeDef"
    ) -> "RunJobFlowOutputTypeDef":
        response = self._client.run_job_flow(**params)
        cluster_id = response["JobFlowId"]
        context.log.info(
            f"[pipes] {self.AWS_SERVICE_NAME} job started with cluster id {cluster_id}"
        )
        return response

    def _wait_for_completion(
        self, context: OpExecutionContext, start_response: "RunJobFlowOutputTypeDef"
    ) -> "DescribeClusterOutputTypeDef":
        cluster_id = start_response["JobFlowId"]
        self._client.get_waiter("cluster_running").wait(ClusterId=cluster_id)
        context.log.info(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} running")
        # now wait for the job to complete
        self._client.get_waiter("cluster_terminated").wait(ClusterId=cluster_id)

        response = self._client.describe_cluster(ClusterId=cluster_id)

        state: ClusterStateType = response["Cluster"]["Status"]["State"]

        context.log.info(
            f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} completed with state: {state}"
        )

        if state == "FAILED":
            context.log.error(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} failed")
            raise Exception(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} failed")

        return response

    def _read_messages(
        self, context: OpExecutionContext, wait_response: "DescribeClusterOutputTypeDef"
    ):
        # TODO: figure out how to find driver logs
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-debugging.html

        logs_uri = wait_response.get("Cluster", {}).get("LogUri", {})

        if isinstance(self.message_reader, PipesS3MessageReader) and logs_uri is None:
            context.log.warning(
                "[pipes] LogUri is not set in the EMR cluster configuration. Won't be able to read logs."
            )
        elif isinstance(self.message_reader, PipesS3MessageReader) and isinstance(logs_uri, str):
            # TODO: call self.message_reader.update_params with the right key
            # and add PipesS3LogReader for each log file
            pass

    def _terminate(self, context: OpExecutionContext, start_response: "RunJobFlowOutputTypeDef"):
        cluster_id = start_response["JobFlowId"]
        context.log.info(f"[pipes] Terminating {self.AWS_SERVICE_NAME} job {cluster_id}")
        self._client.terminate_job_flows(JobFlowIds=[cluster_id])
