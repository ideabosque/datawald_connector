#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

__author__ = "bibow"

import base64, time, traceback, boto3, hashlib, hmac, requests, humps
from silvaengine_utility import Utility


class DatawaldConnector(object):
    def __init__(self, logger, **setting):
        self.logger = logger
        self.setting = setting
        self.expires_time_ts = time.time()
        self.id_token = None
        self.encode = "utf-8"
        self.except_keys = ["data", "entities"]
        self.headers = self.connect()

    @property
    def encode(self):
        return self._encode

    @encode.setter
    def encode(self, encode):
        self._encode = encode

    @property
    def headers(self):
        return self._headers

    @headers.setter
    def headers(self, headers):
        self._headers = headers

    def connect(self):
        if self.setting.get("DW_USER_POOL_ID"):
            if self.id_token is None or (self.expires_time_ts - time.time()) <= 0:
                data = self.get_token_id()
                self.expires_time_ts = time.time() + data["expires_in"]
                self.id_token = data["id_token"]
            return {
                "Authorization": self.id_token,
                "Content-Type": "application/json",
                "x-api-key": self.setting["DW_API_KEY"],
            }
        else:
            return {
                "Authorization": "token",
                "Content-Type": "application/json",
                "x-api-key": self.setting["DW_API_KEY"],
            }

    def get_token_id(self):
        digest = hmac.new(
            self.setting["DW_SECRET_KEY"],
            msg=self.setting["DW_USER"] + self.setting["DW_CLIENT_ID"],
            digestmod=hashlib.sha256,
        ).digest()
        signature = base64.b64encode(digest).decode()
        try:
            response = boto3.client("cognito-idp").admin_initiate_auth(
                UserPoolId=self.setting["DW_USER_POOL_ID"],
                ClientId=self.setting["DW_CLIENT_ID"],
                AuthFlow="ADMIN_NO_SRP_AUTH",
                AuthParameters={
                    "USERNAME": self.setting["DW_USER"],
                    "PASSWORD": self.setting["DW_PASSWORD"],
                    "SECRET_HASH": signature,
                },
            )
            expires_in = response["AuthenticationResult"]["ExpiresIn"]
            id_token = response["AuthenticationResult"]["IdToken"]
            return {"expires_in": expires_in, "id_token": id_token}
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def transform(self, data, format=None, except_keys=[]):
        keys = list(data.keys())
        json_elements = {}
        while True:
            key = keys.pop()
            if key in except_keys:
                json_elements[key] = data.pop(key)
            if len(keys) == 0:
                if format == "camelize":
                    data = humps.camelize(data)
                elif format == "decamelize":
                    data = humps.decamelize(data)
                else:
                    pass

                data.update(json_elements)
                break
        return data

    def graphql_execute(self, query, variables):
        # Instantiate the client with an endpoint.
        request_url = f"{self.setting['DW_API_URL']}/{self.setting['DW_AREA']}/{self.setting['DW_ENDPOINT_ID']}/datawald_interface_graphql"

        variables = self.transform(
            variables, format="camelize", except_keys=self.except_keys
        )

        # Synchronous request
        response = requests.post(
            request_url,
            headers=self.headers,
            data=Utility.json_dumps({"query": query, "variables": variables}),
            timeout=60,
            verify=True,
        )
        if response.status_code == 200:
            result = Utility.json_loads(response.content)
            if result.get("errors"):
                raise Exception(result["errors"])
            return result["data"]
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def get_last_cute_date(self, tx_type, source, offset=False):
        query = """
            query($txType: String!, $source: String!) {
                cutDate(txType: $txType, source: $source) {
                    cutDate
                    offset
                }
            }
        """
        variables = {
            "tx_type": tx_type,
            "source": source,
        }
        self.logger.info(variables)

        data = self.graphql_execute(query, variables)
        if offset:
            return (data["cutDate"]["offset"], data["cutDate"]["cutDate"])
        return data["cutDate"]["cutDate"]

    def insert_tx_staging(self, **variables):
        query = """
            mutation insertTxStaging(
                    $source: String!,
                    $txTypeSrcId: String!,
                    $target: String!,
                    $data: JSON!,
                    $txStatus: String!,
                    $txNote: String!,
                    $createdAt: DateTime!,
                    $updatedAt: DateTime!

                ) {
                insertTxStaging(
                    source: $source,
                    txTypeSrcId: $txTypeSrcId,
                    target: $target,
                    data: $data,
                    txStatus: $txStatus,
                    txNote: $txNote,
                    createdAt: $createdAt,
                    updatedAt: $updatedAt
                ) {
                    txStaging{
                        source
                        txTypeSrcId
                        target
                        tgtId
                        data
                        oldData
                        createdAt
                        updatedAt
                        txNote
                        txStatus
                    }
                }
            }
        """
        tx_staging = self.graphql_execute(query, variables)["insertTxStaging"][
            "txStaging"
        ]
        return self.transform(
            tx_staging, format="decamelize", except_keys=self.except_keys
        )

    def update_tx_staging(self, **variables):
        query = """
            mutation updateTxStaging(
                    $source: String!,
                    $txTypeSrcId: String!,
                    $tgtId: String!,
                    $txStatus: String!,
                    $txNote: String!,
                    $updatedAt: DateTime!

                ) {
                updateTxStaging(
                    source: $source,
                    txTypeSrcId: $txTypeSrcId,
                    tgtId: $tgtId,
                    txStatus: $txStatus,
                    txNote: $txNote,
                    updatedAt: $updatedAt
                ) {
                    status
                }
            }
        """
        return self.graphql_execute(query, variables)["updateTxStaging"]["status"]

    def get_tx_staging(self, **variables):
        query = """
            query($source: String!, $txTypeSrcId: String!) {
                txStaging(source: $source, txTypeSrcId: $txTypeSrcId) {
                    source
                    txTypeSrcId
                    target
                    tgtId
                    data
                    oldData
                    createdAt
                    updatedAt
                    txNote
                    txStatus
                }
            }
        """
        tx_staging = self.graphql_execute(query, variables)["txStaging"]
        return self.transform(
            tx_staging, format="decamelize", except_keys=self.except_keys
        )

    def insert_sync_task(self, **variables):
        query = """
            mutation insertSyncTask(
                    $txType: String!,
                    $source: String!,
                    $target: String!,
                    $cutDate: DateTime!,
                    $offset: Int,
                    $entities: [JSON]!,
                    $funct: String!
                ) {
                insertSyncTask(
                    txType: $txType,
                    source: $source,
                    target: $target,
                    cutDate: $cutDate,
                    offset: $offset,
                    entities: $entities,
                    funct: $funct
                ) {
                    syncTask{
                        txType
                        id
                        source
                        target
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities
                    }
                }
            }
        """
        sync_task = self.graphql_execute(query, variables)["insertSyncTask"]["syncTask"]
        return self.transform(
            sync_task, format="decamelize", except_keys=self.except_keys
        )

    def update_sync_task(self, **variables):
        query = """
            mutation updateSyncTask(
                    $txType: String!,
                    $id: String!,
                    $entities: [JSON]!
                ) {
                updateSyncTask(
                    txType: $txType,
                    id: $id,
                    entities: $entities
                ) {
                    syncTask{
                        txType
                        id
                        source
                        target
                        cutDate
                        startDate
                        endDate
                        offset
                        syncNote
                        syncStatus
                        entities
                    }
                }
            }
        """
        sync_task = self.graphql_execute(query, variables)["updateSyncTask"]["syncTask"]
        return self.transform(
            sync_task, format="decamelize", except_keys=self.except_keys
        )

    def delete_sync_task(self, **variables):
        query = """
            mutation deleteSyncTask(
                    $txType: String!,
                    $id: String!
                ) {
                deleteSyncTask(
                    txType: $txType,
                    id: $id
                ) {
                    status
                }
            }
        """
        return self.graphql_execute(query, variables)["deleteSyncTask"]["status"]

    def get_sync_task(self, **variables):
        query = """
            query($txType: String!, $id: String!) {
                syncTask(txType: $txType, id: $id) {
                    txType
                    id
                    source
                    target
                    cutDate
                    startDate
                    endDate
                    offset
                    syncNote
                    syncStatus
                    entities
                }
            }
        """
        sync_task = self.graphql_execute(query, variables)["syncTask"]
        return self.transform(
            sync_task, format="decamelize", except_keys=self.except_keys
        )

    def insert_product_metadata(self, **variables):
        query = """
            mutation insertProductMetadata(
                    $target: String!,
                    $column: String!,
                    $metadata: JSON!
                ) {
                insertProductMetadata(
                    target: $target,
                    column: $column,
                    metadata: $metadata
                ) {
                    productMetadata{
                        target
                        column
                        metadata
                        createdAt
                        updatedAt
                    }
                }
            }
        """
        product_metadata = self.graphql_execute(query, variables)[
            "insertProductMetadata"
        ]["productMetadata"]
        return self.transform(
            product_metadata, format="decamelize", except_keys=self.except_keys
        )

    def update_product_metadata(self, **variables):
        query = """
            mutation updateProductMetadata(
                    $target: String!,
                    $column: String!,
                    $metadata: JSON!
                ) {
                updateProductMetadata(
                    target: $target,
                    column: $column,
                    metadata: $metadata
                ) {
                    productMetadata{
                        target
                        column
                        metadata
                        createdAt
                        updatedAt
                    }
                }
            }
        """
        product_metadata = self.graphql_execute(query, variables)[
            "updateProductMetadata"
        ]["productMetadata"]
        return self.transform(
            product_metadata, format="decamelize", except_keys=self.except_keys
        )

    def delete_product_metadata(self, **variables):
        query = """
            mutation deleteProductMetadata(
                    $target: String!,
                    $column: String!
                ) {
                deleteProductMetadata(
                    target: $target,
                    column: $column
                ) {
                    status
                }
            }
        """
        return self.graphql_execute(query, variables)["deleteProductMetadata"]["status"]

    def get_product_metadatas(self, **variables):
        query = """
            query($target: String!) {
                productMetadatas(target: $target) {
                    target
                    column
                    metadata
                    createdAt
                    updatedAt
                }
            }
        """
        product_metadatas = self.graphql_execute(query, variables)["productMetadatas"]
        return [
            self.transform(
                product_metadata, format="decamelize", except_keys=self.except_keys
            )
            for product_metadata in product_metadatas
        ]
