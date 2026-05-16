from pyspark import pipelines as dp

import json
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType
from pyspark.sql.window import Window

import json

from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)



from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)



def spark_type(type_str: str, *, default_decimal: tuple[int, int] = (38, 2)) -> DataType:
    """
    Map a metadata type string to a PySpark SQL type instance.

    Supports common primitive aliases (for example `int`, `integer`, `bool`)
    and decimal definitions in the form `decimal(p,s)`. If `decimal` is
    provided without precision/scale, `default_decimal` is used.
    """
    if type_str is None:
        raise ValueError("type_str cannot be None")

    t = str(type_str).strip().lower()

    # decimal(precision,scale) or decimal
    if t.startswith("decimal"):
        if "(" in t and ")" in t:
            inner = t[t.find("(") + 1 : t.rfind(")")]
            parts = [p.strip() for p in inner.split(",")]
            if len(parts) != 2:
                raise ValueError(f"Invalid decimal type: {type_str}")
            return DecimalType(int(parts[0]), int(parts[1]))
        return DecimalType(*default_decimal)

    mapping = {
        "string": StringType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "long": LongType(),
        "smallint": ShortType(),
        "short": ShortType(),
        "tinyint": ByteType(),
        "byte": ByteType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "bool": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "binary": BinaryType(),
    }
    if t in mapping:
        return mapping[t]


def parse_type(type_obj, *, default_decimal: tuple[int, int] = (38, 2)) -> DataType:
    """
    Recursively parse a type definition that can be:
    - A string (primitive type)
    - A dict with 'type': 'array' and 'elementType'
    - A dict with 'type': 'struct' and 'fields'
    """
    if isinstance(type_obj, str):
        # Primitive type
        return spark_type(type_obj, default_decimal=default_decimal)
    
    if not isinstance(type_obj, dict):
        raise ValueError(f"Type must be string or dict, got: {type(type_obj)}")
    
    type_kind = type_obj.get("type")
    
    if type_kind == "array":
        element_type = type_obj.get("elementType")
        if not element_type:
            raise ValueError(f"Array type missing 'elementType': {type_obj}")
        contains_null = type_obj.get("containsNull", True)
        element_dtype = parse_type(element_type, default_decimal=default_decimal)
        return ArrayType(element_dtype, contains_null)
    
    elif type_kind == "struct":
        fields = type_obj.get("fields", [])
        if not isinstance(fields, list):
            raise ValueError(f"Struct fields must be a list. Got: {type(fields)}")
        
        spark_fields = []
        for f in fields:
            if not isinstance(f, dict):
                raise ValueError(f"Each field must be a dict. Got: {f}")
            
            name = f.get("name")
            if not name:
                raise ValueError(f"Field missing 'name': {f}")
            
            nullable = bool(f.get("nullable", True))
            metadata = f.get("metadata") or {}
            
            field_type = f.get("type", "string")
            dtype = parse_type(field_type, default_decimal=default_decimal)
            
            spark_fields.append(StructField(name, dtype, nullable, metadata))
        
        return StructType(spark_fields)
    
    else:
        raise ValueError(f"Unknown type kind: {type_kind}")


def convert_struct_schema_string_to_struct_type(
    schema_json: str,
    *,
    default_decimal: tuple[int, int] = (38, 2),
) -> StructType | None:
    """
    Accepts schema in the wrapper format supporting nested arrays and structs:
      {
        "type": "struct",
        "fields": [
          {"name":"product_code","type":"string","nullable":true},
          {"name":"sensors","type":{"type":"array","elementType":{...}},"nullable":true},
          ...
        ]
      }

    Returns a PySpark StructType suitable for Auto Loader.
    """
    schema_obj = schema_json
    if not schema_obj or schema_obj.get("type") != "struct":
        raise ValueError(f"Expected schema with type 'struct'. Got: {schema_obj}")

    return parse_type(schema_obj, default_decimal=default_decimal)


def create_table(spark, params):
    """
    This functionality manages the creation of each of the tables
    in the pipeline.
    """
    @dp.table(
        name=params.get("target_table", ""),
        comment="<comment>"
    )
    def _():
        df = (
            spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", params.get("file_format", "csv"))
            .schema(convert_struct_schema_string_to_struct_type(params.get("schema", {})))
            .load(params.get("source_path", ""))
            .selectExpr("*", "_metadata as source_metadata", "current_timestamp as _ingestion_timestamp")
        )
        df = df.withColumn("file_date",
                      F.to_timestamp(
                          F.regexp_extract(F.col("source_metadata.file_name"), r"_(\d{8})_\d{6}\.json$", 1),
                          "yyyyMMdd"
                      ).alias("file_date")
                      )
        return df

def create_auto_cdc_from_snapshot_flow_local(
    spark,
    target_table: str,
    source_table: str,
    business_keys: list[str],
    order_keys: list[str],
    scd_type: int = 2,
    track_history_except_column_list: list[str] = None,
    track_history_column_list: list[str] = None,
):
    # Step 1: Create target streaming table
    dp.create_streaming_table(name=target_table)

    # Step 2: Create snapshot as temporary view (intermediate deduplicated snapshot, not persisted)
    # Extract just the table name for the view (temporary views cannot have multipart names)
    table_name_only = target_table.split(".")[-1]
    snapshot_table = f"{table_name_only}_snapshot"
    
    @dp.temporary_view(name=snapshot_table)
    def silver_snapshot():
        df = (
            spark.read.table(source_table)
            .withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy(*business_keys).orderBy(*order_keys)
                )
            )
        )
        return df.filter(F.col("row_num") == 1).drop("row_num")

    # Step 3: Create CDC flow at top level (NO decorator)
    # Only pass one of track_history_column_list or track_history_except_column_list, not both
    cdc_kwargs = {
        "target": target_table,
        "source": snapshot_table,
        "keys": business_keys,
        "stored_as_scd_type": scd_type,
    }
    
    # Add track_history parameters only if they are non-empty and non-None
    # These parameters are mutually exclusive - only one can be specified
    if track_history_column_list and len(track_history_column_list) > 0:
        cdc_kwargs["track_history_column_list"] = track_history_column_list
    elif track_history_except_column_list and len(track_history_except_column_list) > 0:
        cdc_kwargs["track_history_except_column_list"] = track_history_except_column_list
    
    dp.create_auto_cdc_from_snapshot_flow(**cdc_kwargs)

def create_auto_cdc_flow_local(
    spark,
    source_table: str,
    target_table: str,
    keys: list[str],
    sequence_by_col: str,
    scd_type: int = 2
):
    # Step 1: Create target streaming table
    dp.create_streaming_table(name=target_table)

    # Step 2: Create source as temporary view (not persisted)
    # Extract just the table name for the view (temporary views cannot have multipart names)
    table_name_only = target_table.split(".")[-1]
    source_view_name = f"{table_name_only}_source"
    
    @dp.temporary_view(name=source_view_name)
    def silver_source():
        return spark.readStream.table(source_table)

    # Step 3: Create CDC flow at top level (NO decorator)
    from pyspark.sql.functions import col
    dp.create_auto_cdc_flow(
        target=target_table,
        source=source_view_name,
        keys=keys,
        sequence_by=col(sequence_by_col),
        stored_as_scd_type=scd_type
    )
    
import re

def convert_sql_to_python_decorators(sql_content: str) -> str:
    """
    Convert SQL CREATE MATERIALIZED VIEW statements to Python decorator format.
    
    Input: SQL with CREATE OR REFRESH MATERIALIZED VIEW statements
    Output: Python code with @dp.materialized_view decorators
    """
    # Pattern to match CREATE MATERIALIZED VIEW statements
    # Captures: table_name and the SELECT query (handling nested parentheses)
    pattern = r'CREATE\s+OR\s+REFRESH\s+MATERIALIZED\s+VIEW\s+([^\s]+)\s+as\s+\((.*?)\);'
    
    matches = re.findall(pattern, sql_content, re.IGNORECASE | re.DOTALL)
    
    python_code_blocks = []
    
    for table_name, select_query in matches:
        # Generate function name from table name (use last part after dots)
        function_name = table_name.split('.')[-1]
        
        # Clean up the SELECT query - preserve internal structure but clean edges
        select_query = select_query.strip()
        
        # Generate Python code block
        code_block = f'''@dp.materialized_view(name="{table_name}")
def {function_name}():
    return spark.sql("""
{select_query}
    """)'''
        
        python_code_blocks.append(code_block)
    
    return '\n\n'.join(python_code_blocks)



