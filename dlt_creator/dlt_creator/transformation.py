from pyspark import pipelines as dp
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .utils.utils import *



def runner(spark):

    operation_name = spark.conf.get("operation_name", "tenerife_meteo_data")

    print(operation_name)

    # Extract metadata for the given operation name and active status
    metadata = (
        spark
        .table("meteo_open_data.operations.metadata")
        .where(f"operation_name == '{operation_name}'")
        .where("is_active == True")
    ).collect()

    # Can happend that, all the configuration has been deactivated, or
    # there is no configuration at all.
    if len(metadata) > 0:

        bronze_rows = [r for r in metadata if r.operation_type == "bronze"]
        silver_rows = [r for r in metadata if r.operation_type == "silver"]
        gold_rows = [r for r in metadata if r.operation_type == "gold"]

        # Execute bronze logic
        for r in bronze_rows:
            r_dict = r.asDict()
            parameters = json.loads(r_dict.get("parameters"))
            create_table(spark, parameters)
        
        # Execute silver logic
        for r in silver_rows:

            # Extract all paremeters from the silver
            r_dict = r.asDict()
            parameters = json.loads(r_dict.get("parameters"))

            cdc_parameters = parameters.get("cdc_parameters", {})
            if not cdc_parameters:
                raise Exception("CDC_PARAMETERS - missing configuration")

            cdc_type = cdc_parameters.get("cdc_type", "")
            if not cdc_type:
                raise Exception("CDC_TYPE - missing configuration")

            source_table = parameters.get("source_table", "")
            if source_table == "":
                raise ValueError("SOURCE_TABLE - missing configuration")

            target_table = parameters.get("target_table", "")
            if target_table == "":
                raise ValueError("TARGET_TABLE - missing configuration")

            business_keys = cdc_parameters.get("keys", [])
            if len(business_keys) == 0:
                raise ValueError("KEYS - missing configuration")


            scd_type = cdc_parameters.get("scd_type", 2)

            # Auto CDC flow for incremental or logical deletion sources.
            if cdc_type == "auto_cdc":
                print("Executing auto_cdc flow")

                sequence_by = cdc_parameters.get("sequence_by", [])
                if sequence_by == "":
                    raise ValueError("SEQUENCE_BY - missing configuration")
                
                create_auto_cdc_flow_local(
                    spark,
                    source_table=source_table,
                    target_table=target_table,
                    keys=business_keys,
                    sequence_by_col=sequence_by,
                    scd_type=scd_type
                )

            # Auto CDC with snapshot for Physical Deletion or bulk
            elif cdc_type == "auto_cdc_from_snapshot":
                print("Executing auto_cdc_from_snapshot flow")
                order_keys = cdc_parameters.get("order_keys", [])
                if len(order_keys) == 0:
                    raise ValueError("ORDER_KEYS - missing configuration")
                track_history_column_list = cdc_parameters.get("track_history_column_list", [])
                track_history_except_column_list = cdc_parameters.get("track_history_except_column_list", [])

                create_auto_cdc_from_snapshot_flow_local(
                    spark,
                    target_table,
                    source_table,
                    business_keys,
                    order_keys,
                    scd_type,
                    cdc_parameters.get("track_history_except_column_list", []),
                    cdc_parameters.get("track_history_column_list", [])
                )
        
        # Execute Gold Logic
        for r in gold_rows:
            r_dict = r.asDict()
            parameters = json.loads(r_dict.get("parameters"))
            sql_file_path = parameters.get("sql_file_path", "")
            if sql_file_path == "":
                raise ValueError("SQL_FILE_PATH - missing configuration")
            with open(sql_file_path, "r") as f:
                sql_content = f.read()
            generated_code = convert_sql_to_python_decorators(sql_content)
            exec(generated_code, {"spark": spark, "dp": dp, "F": F, "Window": Window})
