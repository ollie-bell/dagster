import datetime
import json
import logging
import time
import uuid
from abc import abstractmethod
from contextlib import contextmanager
from typing import List, Mapping, Optional, Sequence, Type, Union

import jwt
import requests
import tableauserverclient as TSC
import xmltodict
from dagster import (
    AssetsDefinition,
    ConfigurableResource,
    Definitions,
    Failure,
    ObserveResult,
    Output,
    _check as check,
    external_assets_from_specs,
    get_dagster_logger,
    multi_asset,
)
from dagster._annotations import experimental
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._utils.cached_method import cached_method
from pydantic import Field, PrivateAttr
from tableauserverclient.server.endpoint.auth_endpoint import Auth

from dagster_tableau.translator import (
    DagsterTableauTranslator,
    StartWorkbookRefreshRequest,
    TableauContentData,
    TableauContentType,
    TableauWorkspaceData,
)

DEFAULT_POLL_INTERVAL_SECONDS = 10
DEFAULT_POLL_TIMEOUT = 600


@experimental
class BaseTableauClient:
    def __init__(
        self,
        connected_app_client_id: str,
        connected_app_secret_id: str,
        connected_app_secret_value: str,
        username: str,
        site_name: str,
    ):
        self.connected_app_client_id = connected_app_client_id
        self.connected_app_secret_id = connected_app_secret_id
        self.connected_app_secret_value = connected_app_secret_value
        self.username = username
        self.site_name = site_name
        self._server = TSC.Server(self.base_url)
        self._server.use_server_version()

    @property
    @abstractmethod
    def base_url(self) -> str:
        raise NotImplementedError()

    @property
    @cached_method
    def _log(self) -> logging.Logger:
        return get_dagster_logger()

    @cached_method
    def get_workbooks(self) -> Mapping[str, object]:
        """Fetches a list of all Tableau workbooks in the workspace."""
        return self._response_to_dict(
            self._server.workbooks.get_request(self._server.workbooks.baseurl)
        )

    @cached_method
    def get_workbook(self, workbook_id) -> Mapping[str, object]:
        """Fetches information, including sheets, dashboards and data sources, for a given workbook."""
        return self._server.metadata.query(
            query=self.workbook_graphql_query, variables={"luid": workbook_id}
        )

    @cached_method
    def get_view(
        self,
        view_id: str,
    ) -> Mapping[str, object]:
        """Fetches information for a given view."""
        return self._response_to_dict(
            self._server.views.get_request(f"{self._server.views.baseurl}/{view_id}")
        )

    def get_job(
        self,
        job_id: str,
    ) -> TSC.JobItem:
        """Fetches information for a given job."""
        return self._server.jobs.get_by_id(job_id)

    def cancel_job(
        self,
        job_id: str,
    ) -> Mapping[str, object]:
        """Fetches information for a given job."""
        return self._response_to_dict(self._server.jobs.cancel(job_id))

    def refresh_workbook(self, workbook_id) -> TSC.JobItem:
        """Refreshes all extracts for a given workbook and return the JobItem object."""
        return self._server.workbooks.refresh(workbook_id)

    def refresh_and_poll(
        self,
        workbook_id: str,
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
    ) -> str:
        job = self.refresh_workbook(workbook_id)

        if not poll_interval:
            poll_interval = DEFAULT_POLL_INTERVAL_SECONDS
        if not poll_timeout:
            poll_timeout = DEFAULT_POLL_TIMEOUT

        self._log.info(f"Job {job.id} initialized for workbook_id={workbook_id}.")
        start = time.monotonic()

        try:
            while True:
                if poll_timeout and start + poll_timeout < time.monotonic():
                    raise Failure(
                        f"Timeout: Tableau job {job.id} is not ready after the timeout"
                        f" {poll_timeout} seconds"
                    )
                time.sleep(poll_interval)
                job = self.get_job(job_id=job.id)

                if job.finish_code == -1:
                    continue
                elif job.finish_code == 0:
                    break
                elif job.finish_code == 1:
                    raise Failure(f"Job failed: {job.id}")
                elif job.finish_code == 2:
                    raise Failure(f"Job was cancelled: {job.id}")
                else:
                    raise Failure(
                        f"Encountered unexpected finish code `{job.finish_code}` for job {job.id}"
                    )
        finally:
            # if Tableau sync has not completed, make sure to cancel it so that it doesn't outlive
            # the python process
            if job.finish_code not in (0, 1, 2):
                self.cancel_job(job.id)

        return job.workbook_id

    def sign_in(self) -> Auth.contextmgr:
        """Sign in to the site in Tableau."""
        jwt_token = jwt.encode(
            {
                "iss": self.connected_app_client_id,
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
                "jti": str(uuid.uuid4()),
                "aud": "tableau",
                "sub": self.username,
                "scp": ["tableau:content:read", "tableau:tasks:run"],
            },
            self.connected_app_secret_value,
            algorithm="HS256",
            headers={"kid": self.connected_app_secret_id, "iss": self.connected_app_client_id},
        )

        tableau_auth = TSC.JWTAuth(jwt_token, site_id=self.site_name)
        return self._server.auth.sign_in(tableau_auth)

    @staticmethod
    def _response_to_dict(response: requests.Response):
        return json.loads(
            json.dumps(xmltodict.parse(response.text, attr_prefix="", cdata_key="")["tsResponse"])
        )

    @property
    def workbook_graphql_query(self) -> str:
        return """
            query workbooks($luid: String!) { 
              workbooks(filter: {luid: $luid}) {
                luid
                name
                createdAt
                updatedAt
                uri
                sheets {
                  luid
                  name
                  createdAt
                  updatedAt
                  path
                  parentEmbeddedDatasources {
                    parentPublishedDatasources {
                      luid
                      name
                    }
                  }
                }
                dashboards {
                  luid
                  name
                  createdAt
                  updatedAt
                  path
                  sheets {
                    luid
                  }
                }
              }
            }
        """


@experimental
class TableauCloudClient(BaseTableauClient):
    """Represents a client for Tableau Cloud and provides utilities
    to interact with the Tableau API.
    """

    def __init__(
        self,
        connected_app_client_id: str,
        connected_app_secret_id: str,
        connected_app_secret_value: str,
        username: str,
        site_name: str,
        pod_name: str,
    ):
        self.pod_name = pod_name
        super().__init__(
            connected_app_client_id=connected_app_client_id,
            connected_app_secret_id=connected_app_secret_id,
            connected_app_secret_value=connected_app_secret_value,
            username=username,
            site_name=site_name,
        )

    @property
    def base_url(self) -> str:
        """Base URL for Tableau Cloud."""
        return f"https://{self.pod_name}.online.tableau.com"


@experimental
class TableauServerClient(BaseTableauClient):
    """Represents a client for Tableau Server and provides utilities
    to interact with Tableau APIs.
    """

    def __init__(
        self,
        connected_app_client_id: str,
        connected_app_secret_id: str,
        connected_app_secret_value: str,
        username: str,
        site_name: str,
        server_name: str,
    ):
        self.server_name = server_name
        super().__init__(
            connected_app_client_id=connected_app_client_id,
            connected_app_secret_id=connected_app_secret_id,
            connected_app_secret_value=connected_app_secret_value,
            username=username,
            site_name=site_name,
        )

    @property
    def base_url(self) -> str:
        """Base URL for Tableau Cloud."""
        return f"https://{self.server_name}"


@experimental
class BaseTableauWorkspace(ConfigurableResource):
    """Base class to represent a workspace in Tableau and provides utilities
    to interact with Tableau APIs.
    """

    connected_app_client_id: str = Field(
        ..., description="The client id of the connected app used to connect to Tableau Workspace."
    )
    connected_app_secret_id: str = Field(
        ..., description="The secret id of the connected app used to connect to Tableau Workspace."
    )
    connected_app_secret_value: str = Field(
        ...,
        description="The secret value of the connected app used to connect to Tableau Workspace.",
    )
    username: str = Field(..., description="The username to authenticate to Tableau Workspace.")
    site_name: str = Field(..., description="The name of the Tableau site to use.")

    _client: Optional[Union[TableauCloudClient, TableauServerClient]] = PrivateAttr(default=None)

    @abstractmethod
    def build_client(self) -> None:
        raise NotImplementedError()

    @contextmanager
    def get_client(self):
        if not self._client:
            self.build_client()
        with self._client.sign_in():
            yield self._client

    def fetch_tableau_workspace_data(
        self,
    ) -> TableauWorkspaceData:
        """Retrieves all Tableau content from the workspace and returns it as a TableauWorkspaceData object.
        Future work will cache this data to avoid repeated calls to the Tableau API.

        Returns:
            TableauWorkspaceData: A snapshot of the Tableau workspace's content.
        """
        with self.get_client() as client:
            workbooks_data = client.get_workbooks()["workbooks"]
            workbook_ids = [workbook["id"] for workbook in workbooks_data["workbook"]]

            workbooks_by_id = {}
            sheets_by_id = {}
            dashboards_by_id = {}
            data_sources_by_id = {}
            for workbook_id in workbook_ids:
                workbook_data = client.get_workbook(workbook_id=workbook_id)["data"]["workbooks"][0]
                workbooks_by_id[workbook_id] = TableauContentData(
                    content_type=TableauContentType.WORKBOOK, properties=workbook_data
                )

                for sheet_data in workbook_data["sheets"]:
                    sheet_id = sheet_data["luid"]
                    if sheet_id:
                        augmented_sheet_data = {**sheet_data, "workbook": {"luid": workbook_id}}
                        sheets_by_id[sheet_id] = TableauContentData(
                            content_type=TableauContentType.SHEET, properties=augmented_sheet_data
                        )

                    for embedded_data_source_data in sheet_data.get(
                        "parentEmbeddedDatasources", []
                    ):
                        for published_data_source_data in embedded_data_source_data.get(
                            "parentPublishedDatasources", []
                        ):
                            data_source_id = published_data_source_data["luid"]
                            if data_source_id and data_source_id not in data_sources_by_id:
                                data_sources_by_id[data_source_id] = TableauContentData(
                                    content_type=TableauContentType.DATA_SOURCE,
                                    properties=published_data_source_data,
                                )

                for dashboard_data in workbook_data["dashboards"]:
                    dashboard_id = dashboard_data["luid"]
                    if dashboard_id:
                        augmented_dashboard_data = {
                            **dashboard_data,
                            "workbook": {"luid": workbook_id},
                        }
                        dashboards_by_id[dashboard_id] = TableauContentData(
                            content_type=TableauContentType.DASHBOARD,
                            properties=augmented_dashboard_data,
                        )

        return TableauWorkspaceData.from_content_data(
            self.site_name,
            list(workbooks_by_id.values())
            + list(sheets_by_id.values())
            + list(dashboards_by_id.values())
            + list(data_sources_by_id.values()),
        )

    def build_assets(
        self,
        start_workbook_refresh_requests: Sequence[StartWorkbookRefreshRequest],
        dagster_tableau_translator: Type[DagsterTableauTranslator],
    ) -> Sequence[CacheableAssetsDefinition]:
        """Returns a set of CacheableAssetsDefinition which will load Tableau content from
        the workspace and translates it into AssetSpecs, using the provided translator.

        Args:
            start_workbook_refresh_requests (Sequence[StartWorkbookRefreshRequest]): A list of
                requests to start workbook refreshes. This feature is equivalent to selecting Refreshing Extracts
                for a workbook in Tableau UI and only works for workbooks for which the data sources are extracts.
                See https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#update_workbook_now
                for documentation.
            dagster_tableau_translator (Type[DagsterTableauTranslator]): The translator to use
                to convert Tableau content into AssetSpecs. Defaults to DagsterTableauTranslator.

        Returns:
            Sequence[CacheableAssetsDefinition]: A list of CacheableAssetsDefinitions which
                will load the Tableau content.
        """
        return [
            TableauCacheableAssetsDefinition(
                self, start_workbook_refresh_requests, dagster_tableau_translator
            )
        ]

    def build_defs(
        self,
        start_workbook_refresh_requests: Optional[Sequence[StartWorkbookRefreshRequest]] = None,
        dagster_tableau_translator: Type[DagsterTableauTranslator] = DagsterTableauTranslator,
    ) -> Definitions:
        """Returns a Definitions object which will load Tableau content from
        the workspace and translate it into assets, using the provided translator.

        Args:
            start_workbook_refresh_requests (Optional[Sequence[StartWorkbookRefreshRequest]]): A list of
                requests to start workbook refreshes. This feature is equivalent to selecting Refreshing Extracts
                for a workbook in Tableau UI and only works for workbooks for which the data sources are extracts.
                See https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#update_workbook_now
                for documentation.
            dagster_tableau_translator (Type[DagsterTableauTranslator]): The translator to use
                to convert Tableau content into AssetSpecs. Defaults to DagsterTableauTranslator.

        Returns:
            Definitions: A Definitions object which will build and return the Power BI content.
        """
        defs = Definitions(
            assets=self.build_assets(
                start_workbook_refresh_requests=start_workbook_refresh_requests or [],
                dagster_tableau_translator=dagster_tableau_translator,
            ),
        )
        return defs


@experimental
class TableauCloudWorkspace(BaseTableauWorkspace):
    """Represents a workspace in Tableau Cloud and provides utilities
    to interact with Tableau APIs.
    """

    pod_name: str = Field(..., description="The pod name of the Tableau Cloud workspace.")

    def build_client(self) -> None:
        self._client = TableauCloudClient(
            connected_app_client_id=self.connected_app_client_id,
            connected_app_secret_id=self.connected_app_secret_id,
            connected_app_secret_value=self.connected_app_secret_value,
            username=self.username,
            site_name=self.site_name,
            pod_name=self.pod_name,
        )


@experimental
class TableauServerWorkspace(BaseTableauWorkspace):
    """Represents a workspace in Tableau Server and provides utilities
    to interact with Tableau APIs.
    """

    server_name: str = Field(..., description="The server name of the Tableau Server workspace.")

    def build_client(self) -> None:
        self._client = TableauServerClient(
            connected_app_client_id=self.connected_app_client_id,
            connected_app_secret_id=self.connected_app_secret_id,
            connected_app_secret_value=self.connected_app_secret_value,
            username=self.username,
            site_name=self.site_name,
            server_name=self.server_name,
        )


class TableauCacheableAssetsDefinition(CacheableAssetsDefinition):
    def __init__(
        self,
        workspace: BaseTableauWorkspace,
        refresh_requests: Sequence[StartWorkbookRefreshRequest],
        translator: Type[DagsterTableauTranslator],
    ):
        self._workspace = workspace
        self._refresh_requests = refresh_requests
        self._translator_cls = translator
        super().__init__(unique_id=self._workspace.site_name)

    def compute_cacheable_data(self) -> Sequence[AssetsDefinitionCacheableData]:
        workspace_data: TableauWorkspaceData = self._workspace.fetch_tableau_workspace_data()
        return [
            AssetsDefinitionCacheableData(extra_metadata=data.to_cached_data())
            for data in [
                *workspace_data.workbooks_by_id.values(),
                *workspace_data.sheets_by_id.values(),
                *workspace_data.dashboards_by_id.values(),
                *workspace_data.data_sources_by_id.values(),
            ]
        ]

    def build_definitions(
        self, data: Sequence[AssetsDefinitionCacheableData]
    ) -> Sequence[AssetsDefinition]:
        workspace_data = TableauWorkspaceData.from_content_data(
            self._workspace.site_name,
            [
                TableauContentData.from_cached_data(check.not_none(entry.extra_metadata))
                for entry in data
            ],
        )

        translator = self._translator_cls(context=workspace_data)

        external_assets = external_assets_from_specs(
            [
                translator.get_asset_spec(content)
                for content in workspace_data.data_sources_by_id.values()
            ]
        )

        tableau_assets = self._build_tableau_assets_from_workspace_data(
            workspace_data=workspace_data,
            translator=translator,
        )

        return external_assets + tableau_assets

    def _build_tableau_assets_from_workspace_data(
        self,
        workspace_data: TableauWorkspaceData,
        translator: DagsterTableauTranslator,
    ) -> List[AssetsDefinition]:
        @multi_asset(
            name=f"tableau_sync_site_{self._workspace.site_name.replace('-', '_')}",
            compute_kind="tableau",
            can_subset=False,
            specs=[
                translator.get_asset_spec(content)
                for content in [
                    *workspace_data.sheets_by_id.values(),
                    *workspace_data.dashboards_by_id.values(),
                ]
            ],
            resource_defs={"tableau": self._workspace.get_resource_definition()},
        )
        def _assets(tableau: BaseTableauWorkspace):
            with tableau.get_client() as client:
                refreshed_workbooks = set()
                for refresh_request in self._refresh_requests:
                    refreshed_workbooks.add(client.refresh_and_poll(refresh_request.workbook_id))
                for view_id, view_content_data in [
                    *workspace_data.sheets_by_id.items(),
                    *workspace_data.dashboards_by_id.items(),
                ]:
                    data = client.get_view(view_id)["view"]
                    if view_content_data.content_type == TableauContentType.SHEET:
                        asset_key = translator.get_sheet_asset_key(view_content_data)
                    elif view_content_data.content_type == TableauContentType.DASHBOARD:
                        asset_key = translator.get_dashboard_asset_key(view_content_data)
                    else:
                        check.assert_never(view_content_data.content_type)
                    if view_content_data.properties["workbook"]["luid"] in refreshed_workbooks:
                        yield Output(
                            value=None,
                            output_name="__".join(asset_key.path),
                            metadata={
                                "workbook_id": data["workbook"]["id"],
                                "owner_id": data["owner"]["id"],
                                "name": data["name"],
                                "contentUrl": data["contentUrl"],
                                "createdAt": data["createdAt"],
                                "updatedAt": data["updatedAt"],
                            },
                        )
                    else:
                        yield ObserveResult(
                            asset_key=asset_key,
                            metadata={
                                "workbook_id": data["workbook"]["id"],
                                "owner_id": data["owner"]["id"],
                                "name": data["name"],
                                "contentUrl": data["contentUrl"],
                                "createdAt": data["createdAt"],
                                "updatedAt": data["updatedAt"],
                            },
                        )

        return [_assets]
