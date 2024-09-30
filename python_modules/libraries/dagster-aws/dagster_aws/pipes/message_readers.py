import base64
import random
import string
import sys
from contextlib import contextmanager
from datetime import datetime
from threading import Event, Thread
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    TypedDict,
    cast,
)

import boto3
import dagster._check as check
from botocore.exceptions import ClientError
from dagster import DagsterInvariantViolationError
from dagster._annotations import experimental
from dagster._core.pipes.client import PipesMessageReader, PipesParams
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesBlobStoreMessageReader,
    PipesLogReader,
    PipesThreadedMessageReader,
    extract_message_or_forward_to_stdout,
    forward_only_logs_to_file,
)
from dagster_pipes import PipesDefaultMessageWriter

if TYPE_CHECKING:
    from mypy_boto3_logs import LogsClient


class PipesS3MessageReader(PipesBlobStoreMessageReader):
    """Message reader that reads messages by periodically reading message chunks from a specified S3
    bucket.

    If `log_readers` is passed, this reader will also start the passed readers
    when the first message is received from the external process.

    Args:
        interval (float): interval in seconds between attempts to download a chunk
        bucket (str): The S3 bucket to read from.
        client (WorkspaceClient): A boto3 client.
        log_readers (Optional[Sequence[PipesLogReader]]): A set of readers for logs on S3.
    """

    def __init__(
        self,
        *,
        interval: float = 10,
        bucket: str,
        client: boto3.client,  # pyright: ignore (reportGeneralTypeIssues)
        log_readers: Optional[Mapping[str, PipesLogReader]] = None,
    ):
        super().__init__(
            interval=interval,
            log_readers=log_readers,
        )
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        key_prefix = "".join(random.choices(string.ascii_letters, k=30))
        yield {"bucket": self.bucket, "key_prefix": key_prefix}

    def download_messages_chunk(self, index: int, params: PipesParams) -> Optional[str]:
        key = f"{params['key_prefix']}/{index}.json"
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            return obj["Body"].read().decode("utf-8")
        except ClientError:
            return None

    def no_messages_debug_text(self) -> str:
        return (
            f"Attempted to read messages from S3 bucket {self.bucket}. Expected"
            " PipesS3MessageWriter to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )


class PipesLambdaLogsMessageReader(PipesMessageReader):
    """Message reader that consumes buffered pipes messages that were flushed on exit from the
    final 4k of logs that are returned from issuing a sync lambda invocation. This means messages
    emitted during the computation will only be processed once the lambda completes.

    Limitations: If the volume of pipes messages exceeds 4k, messages will be lost and it is
    recommended to switch to PipesS3MessageWriter & PipesS3MessageReader.
    """

    @contextmanager
    def read_messages(
        self,
        handler: PipesMessageHandler,
    ) -> Iterator[PipesParams]:
        self._handler = handler
        try:
            # use buffered stdio to shift the pipes messages to the tail of logs
            yield {PipesDefaultMessageWriter.BUFFERED_STDIO_KEY: PipesDefaultMessageWriter.STDERR}
        finally:
            self._handler = None

    def consume_lambda_logs(self, response) -> None:
        handler = check.not_none(
            self._handler, "Can only consume logs within context manager scope."
        )

        log_result = base64.b64decode(response["LogResult"]).decode("utf-8")

        for log_line in log_result.splitlines():
            extract_message_or_forward_to_stdout(handler, log_line)

    def no_messages_debug_text(self) -> str:
        return (
            "Attempted to read messages by extracting them from the tail of lambda logs directly."
        )


class CloudWatchEvent(TypedDict):
    timestamp: int
    message: str
    ingestionTime: int


def tail_cloudwatch_events(
    client: "LogsClient",
    log_group: str,
    log_stream: str,
    start_time: Optional[int] = None,
) -> Generator[List[CloudWatchEvent], None, None]:
    """Yields events from a CloudWatch log stream."""
    params: Dict[str, Any] = {
        "logGroupName": log_group,
        "logStreamName": log_stream,
    }

    if start_time is not None:
        params["startTime"] = start_time

    response = client.get_log_events(**params)

    while True:
        events = response.get("events")

        if events:
            yield events

        params["nextToken"] = response["nextForwardToken"]

        response = client.get_log_events(**params)


@experimental
class PipesCloudWatchLogReader(PipesLogReader):
    def __init__(
        self,
        client=None,
        log_group: Optional[str] = None,
        log_stream: Optional[str] = None,
        target_stream: Optional[IO[str]] = None,
        start_time: Optional[int] = None,
    ):
        self.client = client or boto3.client("logs")
        self.log_group = log_group
        self.log_stream = log_stream
        self.target_stream = target_stream or sys.stdout
        self.thread = None
        self.start_time = start_time

    def can_start(self, params: PipesParams) -> bool:
        log_group = params.get("log_group") or self.log_group
        log_stream = params.get("log_stream") or self.log_stream

        if log_group is not None and log_stream is not None:
            # check if the stream actually exists
            try:
                self.client.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=log_stream,
                )
                return True
            except self.client.exceptions.ResourceNotFoundException:
                return False
        else:
            return False

    def start(self, params: PipesParams, is_session_closed: Event) -> None:
        if not self.can_start(params):
            raise DagsterInvariantViolationError(
                "log_group and log_stream must be set either in the constructor or in Pipes params."
            )

        self.thread = Thread(
            target=self._start, kwargs={"params": params, "is_session_closed": is_session_closed}
        )
        self.thread.start()

    def _start(self, params: PipesParams, is_session_closed: Event) -> None:
        log_group = cast(str, params.get("log_group") or self.log_group)
        log_stream = cast(str, params.get("log_stream") or self.log_stream)
        start_time = cast(int, self.start_time or params.get("start_time"))

        for events in tail_cloudwatch_events(
            self.client, log_group, log_stream, start_time=start_time
        ):
            for event in events:
                for line in event["message"].splitlines():
                    forward_only_logs_to_file(line, self.target_stream)

            if is_session_closed.is_set():
                return

    def stop(self) -> None:
        pass

    def is_running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


@experimental
class PipesCloudWatchMessageReader(PipesThreadedMessageReader):
    """Message reader that consumes AWS CloudWatch logs to read pipes messages."""

    def __init__(
        self,
        client=None,
        log_group: Optional[str] = None,
        log_stream: Optional[str] = None,
        log_readers: Optional[Mapping[str, PipesLogReader]] = None,
    ):
        """Args:
        client (boto3.client): boto3 CloudWatch client.
        """
        self.client: "LogsClient" = client or boto3.client("logs")
        self.log_group = log_group
        self.log_stream = log_stream

        self.start_time = datetime.now()

        super().__init__(log_readers=log_readers)

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}

    def can_start(self, params: PipesParams) -> bool:
        log_group = params.get("log_group") or self.log_group
        log_stream = params.get("log_stream") or self.log_stream

        if log_group is not None and log_stream is not None:
            # check if the stream actually exists
            try:
                self.client.describe_log_streams(
                    logGroupName=log_group,
                    logStreamNamePrefix=log_stream,
                )
                return True
            except self.client.exceptions.ResourceNotFoundException:
                return False
        else:
            return False

    def download_messages_parts(
        self, cursor: Optional[str], params: PipesParams
    ) -> Optional[Tuple[str, str]]:
        params = {
            "logGroupName": params.get("log_group") or self.log_group,
            "logStreamName": params.get("log_stream") or self.log_stream,
            "startTime": int(self.start_time.timestamp() * 1000),
        }

        if cursor is not None:
            params["nextToken"] = cursor

        response = self.client.get_log_events(**params)

        events = response.get("events")

        if not events:
            return None
        else:
            cursor = cast(str, response["nextForwardToken"])
            return cursor, "\n".join(event["message"] for event in events)

    def no_messages_debug_text(self) -> str:
        return "Attempted to read messages by extracting them from CloudWatch logs."
