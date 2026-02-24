import json
import pandas as pd

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from nifiapi.relationship import Relationship


class FrostDatastreamMap(FlowFileTransform):

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.1.0"
        description = (
            "Maps Datastream IDs from FROST server to given measurements using "
            "measurement type, sensor ID, thing ID and source name."
        )
        dependencies = ['pandas']

    DBCP_SERVICE = PropertyDescriptor(
        name="Database Connection Pool Service",
        description="The Controller Service that is used to obtain a connection to the database.",
        required=True,
        controller_service_definition="org.apache.nifi.dbcp.DBCPService"
    )

    MEASUREMENT_TYPE = PropertyDescriptor(
        name="Measurement type",
        description="Flowfile record key with information about measurement type",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    SENSOR_ID = PropertyDescriptor(
        name="Sensor ID",
        description="Flowfile record key with information about sensor ID",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    THING_ID = PropertyDescriptor(
        name="Thing ID",
        description="Flowfile record key with information about thing ID",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    SOURCE_NAME = PropertyDescriptor(
        name="Source name",
        description="Flowfile record key with information about source name",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    DESCRIPTIONS = PropertyDescriptor(
        name="Descriptions",
        description="Considered entries of description column in sensor table",
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    properties = [DBCP_SERVICE, MEASUREMENT_TYPE, SENSOR_ID, THING_ID, SOURCE_NAME, DESCRIPTIONS]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.properties

    def transform(self, context, flowfile):
        try:
            measurement_type = context.getProperty(self.MEASUREMENT_TYPE).evaluateAttributeExpressions(flowfile).getValue()
            sensor_id = context.getProperty(self.SENSOR_ID).evaluateAttributeExpressions(flowfile).getValue()
            thing_id = context.getProperty(self.THING_ID).evaluateAttributeExpressions(flowfile).getValue()
            source_name = context.getProperty(self.SOURCE_NAME).evaluateAttributeExpressions(flowfile).getValue()
            description = context.getProperty(self.DESCRIPTIONS).evaluateAttributeExpressions(flowfile).getValue()

            descriptions = ", ".join(f"'{desc}'" for desc in description.split(","))
            contents_bytes = flowfile.getContentsAsBytes()
            contents = contents_bytes.decode('utf-8')
            data = json.loads(contents)
            df = pd.DataFrame(data)
            sql = f"""
            select d.id as datastream_id,
                   json_extract_scalar(d.properties, '$.disabled')          as datastream_disabled,
                   json_extract_scalar(d.properties, '$.measurement_type')  as {measurement_type},
                   json_extract_scalar(s.properties, '$.id')                as {sensor_id},
                   json_extract_scalar(s.properties, '$.sourceName')        as {source_name},
                   json_extract_scalar(t.properties, '$.id')                as {thing_id}
            from frost.public.datastreams d
                     left join frost.public.sensors s on d.sensor_id = s.id
            left join frost.public.things t on d.thing_id = t.id
            where s.description in ({descriptions})
            order by datastream_id
            """


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

                        df2 = pd.DataFrame(rows, columns=columns)
                    finally:
                        rs.close()
                finally:
                    stmt.close()
            finally:
                conn.close()

            df = df.merge(df2, on=[measurement_type, sensor_id, thing_id, source_name], how='left')
            result = df.to_dict(orient='records')

            result_contents = json.dumps(result, ensure_ascii=False)

            return FlowFileTransformResult(
                relationship="success",
                contents=result_contents
            )

        except Exception as e:
            import traceback
            error_details = f"Error transforming GeoJSON to WKT: {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            self.logger.error(error_details)
            return FlowFileTransformResult(
                relationship="failure",
                contents=error_details
            )
