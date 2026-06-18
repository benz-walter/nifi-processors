import json
import pandas as pd

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class CheckDuplicates(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.1"
        description = (
            "Checks whether given flowfile content is already contained in database "
            "using SQL query and comparing provided columns. Uses a DBCPConnectionPool."
        )
        dependencies = ['pandas']

    DBCP_SERVICE = PropertyDescriptor(
        name="Database Connection Pool Service",
        description="The Controller Service that is used to obtain a connection to the database.",
        required=True,
        controller_service_definition="org.apache.nifi.dbcp.DBCPService"
    )

    SQL_QUERY = PropertyDescriptor(
        name="SQL query",
        description="Query executed against database",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    COLUMN_MAPPING = PropertyDescriptor(
        name="Column mapping",
        description="JSON mapping between flowfile fields and database columns",
        required=True,
        default_value="{}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [
        DBCP_SERVICE,
        SQL_QUERY,
        COLUMN_MAPPING
    ]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def getRelationships(self):
        return {
            Relationship("success", description="Flowfiles that are not duplicates are routed to this relationship"),
            Relationship("duplicated", description="Flowfiles that are duplicates are routed to this relationship"),
        }

    def transform(self, context, flowfile):

        contents_bytes = flowfile.getContentsAsBytes()
        contents = contents_bytes.decode('utf-8')
        flow_data = json.loads(contents)

        if isinstance(flow_data, dict):
            flow_data = [flow_data]

        sql = context.getProperty(self.SQL_QUERY).evaluateAttributeExpressions(flowfile).getValue()
        column_mapping = json.loads(
            context.getProperty(self.COLUMN_MAPPING)
            .evaluateAttributeExpressions(flowfile)
            .getValue()
        )

        dbcp_service = context.getProperty(self.DBCP_SERVICE).asControllerService()
        conn = dbcp_service.getConnection()
        try:
            stmt = conn.prepareStatement(sql)
            try:
                rs = stmt.executeQuery()
                try:
                    # Spaltennamen extrahieren
                    meta = rs.getMetaData()
                    col_count = meta.getColumnCount()
                    columns = [meta.getColumnLabel(i + 1) for i in range(col_count)]

                    # Daten extrahieren
                    rows = []
                    while rs.next():
                        row = []
                        for i in range(col_count):
                            row.append(rs.getObject(i + 1))
                        rows.append(row)

                    df = pd.DataFrame(rows, columns=columns)
                finally:
                    rs.close()
            finally:
                stmt.close()
        finally:
            conn.close()

        # db_rows = df.to_dict(orient='records')
        #
        # non_duplicates = []
        # duplicates = []
        #
        # for item in flow_data:
        #     is_duplicate = False
        #
        #     for db_row in db_rows:
        #         if all(
        #                 str(item.get(flow_col)) == str(db_row.get(db_col))
        #                 for flow_col, db_col in column_mapping.items()
        #         ):
        #             is_duplicate = True
        #             duplicates.append(item)
        #             break
        #
        #     if not is_duplicate:
        #         non_duplicates.append(item)
        flow_df = pd.DataFrame(flow_data)

        rename_map = {db_col: flow_col for flow_col, db_col in column_mapping.items()}
        db_df_renamed = df[list(column_mapping.values())].rename(columns=rename_map)

        merge_cols = list(column_mapping.keys())

        # Nur für den Vergleich: Kopien mit str-Cast
        flow_compare = flow_df[merge_cols].astype(str)
        db_compare = db_df_renamed[merge_cols].astype(str).drop_duplicates()

        merged = flow_compare.merge(db_compare, on=merge_cols, how='left', indicator=True)

        # Index nutzen, um originale Zeilen aus flow_df zu holen
        non_duplicate_idx = merged[merged['_merge'] == 'left_only'].index
        duplicate_idx = merged[merged['_merge'] == 'both'].index

        non_duplicates = flow_df.loc[non_duplicate_idx].to_dict(orient='records')
        duplicates = flow_df.loc[duplicate_idx].to_dict(orient='records')

        if non_duplicates:
            return FlowFileTransformResult(
                relationship="success",
                contents=json.dumps(non_duplicates)
            )

        if duplicates:
            return FlowFileTransformResult(
                relationship="duplicated",
                contents=json.dumps(duplicates)
            )

        return None
