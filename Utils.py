# Refactored Spark helper library
# All function and variable names changed from the original version.

import os
import re
import random as _random
import string as _string
from decimal import Decimal as _Decimal
from datetime import timedelta, time, date, datetime, timezone as _timezone
from dateutil.parser import parse as _parse_date
from dateutil.relativedelta import relativedelta as _relativedelta
import pandas as pd
import numpy as np
import time as _time

from pyspark.sql import SparkSession as _SparkSession
from pyspark.sql import functions as SF
from pyspark.sql.functions import col as _col, when as _when, lit as _lit, to_date as _to_date, to_timestamp as _to_timestamp, regexp_replace as _regexp_replace, explode_outer as _explode_outer
from pyspark.sql.types import StringType as _StringType, ArrayType as _ArrayType, StructType as _StructType
from pyspark.sql.dataframe import DataFrame as _DataFrame

# Azure email client (kept same lib usage, but variable names differ)
from azure.communication.email import EmailClient as _EmailClient

# read secret using dbutils (name changed)
_conn_str = dbutils.secrets.get("KV-[Environment]", "KV-ConnectionString")
_email_client = _EmailClient.from_connection_string(_conn_str)

# small helpers
_letters = _string.ascii_lowercase
_session = _SparkSession.builder.getOrCreate()


class SparkHelpers:
    """A collection of helper utilities for Spark operations (renamed)."""

    @staticmethod
    def check_folder_and_compare(base_path: str, run_date: str):
        """Validate directory structure under DBFS and compare files between latest and previous run.
        If files are missing in latest run, send an alert email.
        """
        base_path = ("/dbfs" + base_path) if not base_path.startswith("/dbfs") else base_path
        base_path = base_path + "/" if not base_path.endswith("/") else base_path

        if os.path.exists(base_path + run_date):
            pos = 0
            full = base_path + run_date
            modified_full = full.replace("/dbfs", "", 1) + "/" if not full.endswith("/") else full

            # list and sort folders by modification time
            all_dirs = dbutils.fs.ls(base_path.replace("/dbfs", "", 1))
            ordered = sorted(all_dirs, key=lambda x: x.modificationTime, reverse=True)
            for idx in range(len(ordered)):
                if modified_full == ordered[idx].path.replace("dbfs:", ""):
                    pos = idx
                    break

            latest_items = dbutils.fs.ls(ordered[pos].path.replace("dbfs:", ""))
            previous_items = dbutils.fs.ls(ordered[pos + 1].path.replace("dbfs:", ""))
            prev_run_date = previous_items[0].path.split("/")[-2]

            latest_names = [p.path.split("/")[-1] for p in latest_items]
            prev_names = [p.path.split("/")[-1] for p in previous_items]

            if len(latest_names) >= len(prev_names):
                print("Latest folder has same or more files; proceeding")
            else:
                prev_prefixes = [n.split("_")[0] for n in prev_names]
                latest_prefixes = [n.split("_")[0] for n in latest_names]
                missing_prefixes = list(set(prev_prefixes).difference(latest_prefixes))
                missing_files = [f for f in prev_names if f.split("_")[0] in missing_prefixes]
                # adjust file names to point to current run
                missing_adjusted = [m.replace(prev_run_date, run_date) for m in missing_files]
                msg = "<br>" + ", ".join(missing_adjusted) + " not present in " + ordered[pos].path.replace("dbfs:", "") + "<br><br>"
                SparkHelpers._send_mail(msg, "Missing run artifacts")
        else:
            print(f"The expected path {base_path + run_date} does not exist - possibly not a run day")

    @staticmethod
    def save_table(df: _DataFrame, mode: str, table_full_name: str):
        df.write.mode(mode).saveAsTable(table_full_name)

    @staticmethod
    def is_table_without_rows(table_full_name: str, where_clause: str = "") -> bool:
        if where_clause == "":
            cnt = _session.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name}").collect()[0]["cnt"]
        else:
            cnt = _session.sql(f"SELECT COUNT(*) AS cnt FROM {table_full_name} WHERE {where_clause}").collect()[0]["cnt"]
        return cnt == 0

    @staticmethod
    def refresh_table_metadata(table: str, database: str):
        _session.sql(f"REFRESH TABLE {database}.{table}")

    @staticmethod
    def deduplicate_view(object_name: str, key_cols: list) -> _DataFrame:
        if key_cols and '' not in key_cols:
            part = ",".join(key_cols)
            order = ",".join(key_cols)
            stmt = (
                "WITH dedup_cte AS (SELECT ROW_NUMBER() OVER(PARTITION BY "
                + part
                + " ORDER BY "
                + order
                + ") AS rn, * FROM "
                + object_name
                + ") SELECT * FROM dedup_cte WHERE rn = 1"
            )
            return _session.sql(stmt).drop('rn')
        else:
            return _session.sql(f"SELECT * FROM {object_name}")

    @staticmethod
    def tidy_partition_and_move(folder_path: str, final_name: str):
        print(folder_path + "/_SUCCESS")
        items = list(dbutils.fs.ls(folder_path))
        for i in range(len(items)):
            p = items[i].path
            if "_SUCCESS" in p or "_started" in p or "_committed" in p:
                dbutils.fs.rm(p)
            if p.endswith('.csv') or p.endswith('.parquet'):
                dbutils.fs.mv(p, folder_path + "/" + final_name)

    @staticmethod
    def locate_table_storage(table_full_name: str) -> str:
        return _session.sql("DESCRIBE DETAIL " + table_full_name).select("location").collect()[0][0]

    @staticmethod
    def overwrite_delta(table_full_name: str, df: _DataFrame):
        path = SparkHelpers.locate_table_storage(table_full_name)
        df.write.format("delta").mode("overwrite").save(path)

    @staticmethod
    def quoted_column_list(table_full_name: str) -> str:
        cols = _session.table(table_full_name).schema.names
        return '"' + '","'.join(cols) + '"'

    @staticmethod
    def fill_na(df: _DataFrame, columns: list, defaults=None) -> _DataFrame:
        if defaults is None:
            defaults = {"StringType": "N/A", "IntegerType": 0, "DateType": "3099-12-31", "TimestampType": "3099-12-31 00:00:00", "LongType": 0, "DoubleType": 0.0, "BooleanType": True}
        for nm, typ in df.dtypes:
            if nm in columns:
                if typ == 'string':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), defaults.get("StringType")).otherwise(_col(nm)))
                elif typ == 'integer':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), defaults.get("IntegerType")).otherwise(_col(nm).cast('double')))
                elif typ == 'boolean':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), defaults.get("BooleanType")).otherwise(_col(nm)))
                elif typ == 'long':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), defaults.get("LongType")).otherwise(_col(nm)))
                elif typ in ['double', 'float']:
                    df = df.withColumn(nm, _when(_col(nm).isNull(), defaults.get("DoubleType")).otherwise(_col(nm)))
                elif typ == 'date':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), _to_date(defaults.get("DateType"), "yyyy-MM-dd")).otherwise(_col(nm)))
                elif typ == 'timestamp':
                    df = df.withColumn(nm, _when(_col(nm).isNull(), _to_timestamp(defaults.get("TimestampType"), "yyyy-MM-dd HH:mm:ss")).otherwise(_col(nm)))
        return df

    @staticmethod
    def normalise_empty_strings(df: _DataFrame, target_cols: list) -> _DataFrame:
        all_cols = df.columns
        other_cols = [SF.col(c) for c in all_cols if c not in target_cols]
        transformed = [ _regexp_replace(SF.col(c), "^$", "N/A").alias(c + "__tmp") for c in target_cols]
        renamed = [_col(c + "__tmp").alias(c) for c in target_cols]
        result = df.select(*(other_cols + transformed)).select(*(other_cols + renamed)).select(all_cols)
        return result

    @staticmethod
    def drop_columns(df: _DataFrame, cols_to_remove: list) -> _DataFrame:
        if cols_to_remove and len(cols_to_remove) > 0:
            keep = [SF.col(c) for c in df.columns if c not in cols_to_remove]
            return df.select(*keep)
        return df

    @staticmethod
    def ensure_database_exists(table_full_name: str):
        dbname = table_full_name.split('.')[0]
        return _session.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}")

    @staticmethod
    def build_delta_table(table_full_name: str, partition_expr: str, storage_base: str, df_src: _DataFrame, append_table_to_path: bool = True):
        SparkHelpers.ensure_database_exists(table_full_name)
        rand_view = ''.join(_random.choice(_letters) for _ in range(10))
        df_src.createOrReplaceTempView(rand_view)

        sql = f"CREATE TABLE {table_full_name} USING DELTA "
        if append_table_to_path:
            sql += f" LOCATION '{storage_base}/{SparkHelpers._extract_table_name(table_full_name)}' "
        else:
            sql += f" LOCATION '{storage_base}' "

        if partition_expr:
            sql += f" PARTITIONED BY ({partition_expr}) "

        sql += f" AS SELECT * FROM {rand_view}"
        _session.sql(sql)

    @staticmethod
    def _extract_table_name(full_name: str) -> str:
        return full_name.split('.')[1]

    @staticmethod
    def partition_filter(col_name: str, col_value: str) -> str:
        return f"{col_name} = '{col_value}'"

    @staticmethod
    def truncate_and_copy(src_table: str, dest_table: str, where_clause: str = ""):
        if where_clause and len(where_clause) > 1:
            stmt = f"INSERT OVERWRITE TABLE {dest_table} SELECT * FROM {src_table} WHERE {where_clause}"
        else:
            stmt = f"INSERT OVERWRITE TABLE {dest_table} SELECT * FROM {src_table}"
        print(stmt)
        _session.sql(stmt)

    @staticmethod
    def add_audit_columns(df: _DataFrame) -> _DataFrame:
        user = _session.sql("select substring_index(current_user(),'@',1)").collect()[0][0]
        today = _session.sql("select current_date()").collect()[0][0]
        defaults = {"updated_on": today, "updated_by": user, "created_on": today, "created_by": user, "is_active": 1}
        for cn, dv in defaults.items():
            df = df.withColumn(cn, _lit(dv))
        return df

    @staticmethod
    def remove_table_and_data(table_full_name: str, storage_base: str, append_table_to_path: bool = True):
        _session.sql(f"DROP TABLE IF EXISTS {table_full_name}")
        if storage_base:
            if append_table_to_path:
                dbutils.fs.rm(f"{storage_base}/{SparkHelpers._extract_table_name(table_full_name)}", True)
            else:
                dbutils.fs.rm(storage_base, True)

    @staticmethod
    def unnest_df(df: _DataFrame) -> _DataFrame:
        def _to_str(x):
            return str(x)
        cast_to_str = SF.udf(_to_str, _StringType())

        complex_fields = SparkHelpers._find_complex(df)
        while complex_fields:
            col_name = list(complex_fields.keys())[0]
            dtype = complex_fields[col_name]
            if isinstance(dtype, _StructType):
                expanded = []
                for fld in dtype:
                    fld_name = fld.name
                    fld_type = fld.dataType.__class__.__name__
                    if fld_type not in ("StringType", "ArrayType", "StructType"):
                        expanded.append(cast_to_str(_col(col_name + '.' + fld_name)).alias(fld_name))
                    else:
                        expanded.append(_col(col_name + '.' + fld_name).alias(fld_name))
                df = df.select("*", *expanded).drop(col_name)
            elif isinstance(dtype, _ArrayType):
                df = df.withColumn(col_name, _explode_outer(col_name))
            complex_fields = SparkHelpers._find_complex(df)
        return df

    @staticmethod
    def _find_complex(df: _DataFrame) -> dict:
        return dict([(f.name, f.dataType) for f in df.schema.fields if isinstance(f.dataType, (_ArrayType, _StructType))])

    @staticmethod
    def load_raw_as_table(format_name: str, table_full_name: str, source_path: str, run_date: str, filename: str = None):
        if format_name == 'Parquet':
            prefix = '/dbfs'
            modified = source_path.replace(prefix, '', 1)
            parquet_loc = f"{modified}/{run_date}/{filename}" if filename else f"{modified}/{run_date}"
            _session.sql(f"DROP TABLE IF EXISTS {table_full_name}")
            _session.sql(f"CREATE TABLE IF NOT EXISTS {table_full_name} USING PARQUET LOCATION '{parquet_loc}'")
            print(f"Table created at {parquet_loc}")
            return

        ext_map = {'Excel': 'xlsx', 'Json': 'json', 'Csv': 'csv'}
        ext = ext_map.get(format_name)
        if not filename:
            files = [f for f in os.listdir(source_path + run_date + "/") if f.endswith('.' + ext)]
            if not files:
                SparkHelpers._send_mail(f"No <b>{ext}</b> files found in <b>{source_path + run_date}</b>", "Process failed")
                raise ValueError(f"No {ext} files found in {source_path + run_date}")
            filename = files[0]

        file_path = os.path.join(source_path, run_date, filename) if filename and format_name in ['Csv', 'Excel'] else os.path.join(source_path, run_date, filename)

        if format_name == 'Excel':
            pdf = pd.read_excel(file_path)
            cleaned = {c: c.lower().replace(' ', '').replace('(', '').replace(')', '') for c in pdf.columns}
            pdf.rename(columns=cleaned, inplace=True)
            src_df = _session.createDataFrame(pdf)
            for c in src_df.columns:
                src_df = src_df.withColumn(c, SF.trim(_col(c)))

        elif format_name == 'Json':
            fp = file_path.replace('/dbfs', '', 1)
            jdf = _session.read.option("multiLine", True).json(fp)
            src_df = SparkHelpers.unnest_df(jdf)
            for c in src_df.columns:
                src_df = src_df.withColumnRenamed(c, c.lower().replace(' ', '').replace('(', '').replace(')', ''))
            for c in src_df.columns:
                src_df = src_df.withColumn(c, SF.trim(_col(c)))

        elif format_name == 'Csv':
            fp = file_path.replace('/dbfs', '', 1)
            cdf = _session.read.option("header", True).csv(fp)
            src_df = cdf
            for c in cdf.columns:
                src_df = src_df.withColumnRenamed(c, c.lower().replace(' ', '').replace('(', '').replace(')', ''))
            for c in src_df.columns:
                src_df = src_df.withColumn(c, SF.trim(_col(c)))

        # persist as table
        SparkHelpers.ensure_database_exists(table_full_name)
        temp_view = 'tmp_view'
        src_df.createOrReplaceTempView(temp_view)
        _session.sql(f"DROP TABLE IF EXISTS {table_full_name}")

        prefix = '/dbfs'
        modified_path = source_path.replace(prefix, '', 1)
        if not filename:
            dbutils.fs.rm(f"{modified_path}{run_date}/parquet", True)
            _session.sql(f"CREATE TABLE {table_full_name} USING PARQUET LOCATION '{modified_path}{run_date}/parquet' AS SELECT * FROM {temp_view}")
            _session.catalog.dropTempView(temp_view)
            print(f"Table created at {modified_path}{run_date}/parquet")
        elif filename and format_name in ['Csv', 'Excel']:
            src_df.write.format('delta').mode('overwrite').saveAsTable(table_full_name)
            _session.catalog.dropTempView(temp_view)
            print("Table created as delta table")

    @staticmethod
    def make_parquet_table(table_name_with_db: str, data_folder: str, run_date: str):
        _session.sql(f"DROP TABLE IF EXISTS {table_name_with_db}")
        _session.sql(f"CREATE TABLE IF NOT EXISTS {table_name_with_db} USING PARQUET LOCATION '{data_folder}/{run_date}'")

    @staticmethod
    def build_column_expression(col_list: list = None, src_alias: str = '', tgt_alias: str = '') -> str:
        if col_list is None:
            col_list = []
        alias = tgt_alias if src_alias == '' else src_alias
        expr = ''
        for c in col_list:
            expr += f"\n{alias}.{c.strip()},"
        return expr[:-1] if expr else ''

    @staticmethod
    def build_key_join(keys: list, left_alias: str = 'l', right_alias: str = 'r') -> str:
        jc = ''
        for k in keys:
            jc += f"{left_alias}.{k.strip()} = {right_alias}.{k.strip()} \n and "
        return jc[:-5]

    @staticmethod
    def build_tracking_pred(tracking_keys: list, left_alias: str = 'l', right_alias: str = 'r') -> str:
        parts = ''
        for k in tracking_keys:
            parts += f"{left_alias}.{k.strip()} != {right_alias}.{k.strip()} \n or "
        return "(" + parts[:-4] + ")"

    @staticmethod
    def build_concat_expression(col_list: list, alias: str = '') -> str:
        alias_to_use = alias if alias else ''
        expr = ''
        for c in col_list:
            expr += f"{alias_to_use}.{c.strip()},"
        return "concat_ws('~'," + expr[:-1] + ")"

    @staticmethod
    def compose_scd2_merge(src_table: str, tgt_table: str, all_columns: list, natural_keys: list, change_columns: list) -> str:
        src_pk = SparkHelpers.build_concat_expression(natural_keys, alias='src')
        tgt_pk = SparkHelpers.build_concat_expression(natural_keys, alias='tgt')
        col_expr = SparkHelpers.build_column_expression(all_columns, src_alias='src')
        join_clause = SparkHelpers.build_key_join(natural_keys, left_alias='src', right_alias='tgt')
        track_clause = SparkHelpers.build_tracking_pred(change_columns, left_alias='src', right_alias='tgt')

        merge_sql = f"""
        MERGE INTO {tgt_table} AS tgt
        USING (
            SELECT {src_pk} AS merge_key, src.* FROM {src_table} AS src
            UNION ALL
            SELECT NULL AS merge_key, src.* FROM {src_table} AS src
            INNER JOIN {tgt_table} AS tgt ON {join_clause} AND tgt.is_active = 1 AND {track_clause}
        ) src
        ON {tgt_pk} = src.merge_key
        WHEN MATCHED AND {track_clause} THEN
            UPDATE SET tgt.is_active = 0, tgt.updated_on = current_date(), tgt.created_on = current_date()
        WHEN NOT MATCHED THEN
            INSERT *
        """
        print(merge_sql)
        return merge_sql

    @staticmethod
    def list_df_columns(df: _DataFrame) -> list:
        return df.columns

    @staticmethod
    def delete_and_reinsert(tracking_keys: list, src_df: _DataFrame, dest_table: str):
        print("delete-reinsert process starting")
        cols_csv = ",".join(tracking_keys)
        src_df.createOrReplaceTempView("tmp_tracking")
        distinct_tracking = _session.sql(f"SELECT DISTINCT {cols_csv} FROM tmp_tracking")
        distinct_tracking.createOrReplaceTempView("tmp_tracking")

        if len(tracking_keys) > 1:
            del_stmt = f"DELETE FROM {dest_table} WHERE concat({cols_csv}) IN (SELECT concat({cols_csv}) FROM tmp_tracking)"
        else:
            del_stmt = f"DELETE FROM {dest_table} WHERE {cols_csv} IN (SELECT {cols_csv} FROM tmp_tracking)"
        print(del_stmt)
        _session.sql(del_stmt)

        src_df.createOrReplaceTempView('tmp_src')
        if len(tracking_keys) > 1:
            left_cols = ",".join([f"S.{c}" for c in tracking_keys])
            right_cols = ",".join([f"D.{c}" for c in tracking_keys])
            ins_stmt = f"INSERT INTO {dest_table} SELECT S.* FROM tmp_src AS S INNER JOIN tmp_tracking AS D ON concat({left_cols}) = concat({right_cols})"
        else:
            ins_stmt = f"INSERT INTO {dest_table} SELECT S.* FROM tmp_src AS S INNER JOIN tmp_tracking AS D ON S.{cols_csv} = D.{cols_csv}"
        print(ins_stmt)
        _session.sql(ins_stmt)
        print("delete-reinsert process completed")

    @staticmethod
    def vacuum_table(table_name: str, retain_hours: int):
        stmt = f"VACUUM {table_name} RETAIN {retain_hours} HOURS"
        _session.sql(stmt)
        print(f"Vacuum executed on {table_name} retaining {retain_hours} hours")

    @staticmethod
    def compose_scd1_merge(src_table: str, tgt_table: str, all_columns: list, natural_keys: list) -> str:
        join_clause = SparkHelpers.build_key_join(natural_keys, left_alias='src', right_alias='tgt')
        stmt = f"MERGE INTO {tgt_table} AS tgt USING {src_table} AS src ON {join_clause} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        print(stmt)
        return stmt

    @staticmethod
    def excel_to_table(table_full_name: str, source_path: str, run_date: str):
        if not os.path.exists(source_path + run_date):
            SparkHelpers._send_mail(f"The folder {source_path}{run_date} does not exist", "Process failed")
            raise ValueError("Source folder missing")
        files = [f for f in os.listdir(source_path + run_date) if f.endswith('.xlsx')]
        fp = os.path.join(source_path, run_date, files[0])
        pdf = pd.read_excel(fp)
        cleaned_cols = [c.replace(' ', '').replace('(', '').replace(')', '') for c in pdf.columns]
        pdf.columns = cleaned_cols
        SparkHelpers.ensure_database_exists(table_full_name)
        tmp = ''.join(_random.choice(_letters) for _ in range(10))
        src_df = _session.createDataFrame(pdf)
        src_df.createOrReplaceTempView(tmp)
        _session.sql(f"DROP TABLE IF EXISTS {table_full_name}")
        prefix = '/dbfs'
        modified = source_path.replace(prefix, '', 1)
        dbutils.fs.rm(modified + run_date + '/parquet', True)
        _session.sql(f"CREATE TABLE {table_full_name} USING PARQUET LOCATION '{modified}{run_date}/parquet' AS SELECT * FROM {tmp}")

    @staticmethod
    def snapshot_stage(format_name: str, raw_table: str, src_path: str, run_date: str, stage_table: str, filename: str = None):
        src_path = ("/dbfs" + src_path) if not src_path.startswith("/dbfs") else src_path
        src_path = src_path + "/" if not src_path.endswith('/') else src_path
        folder_check = src_path.split("/")[-1] in raw_table
        dir_to_check = src_path + run_date if not filename else src_path
        if not os.path.exists(dir_to_check):
            print(f"Path {dir_to_check} does not exist")
            return
        file_list = dbutils.fs.ls(dir_to_check.replace('/dbfs', '', 1))
        if folder_check and len(file_list) > 0:
            SparkHelpers.load_raw_as_table(format_name, raw_table, src_path, run_date, filename)
            final_df = _session.table(raw_table)
            snap_col = next((c for c in final_df.columns if 'snapshot' in c.lower().replace('_', '')), None)
            if snap_col and snap_col.lower() in map(str.lower, final_df.columns):
                final_df = final_df.withColumnRenamed(snap_col, 'SnapshotDate')
            else:
                final_df = final_df.withColumn('SnapshotDate', _lit(run_date))
            stage_df = SparkHelpers.add_audit_columns(final_df)
            filt = SparkHelpers.partition_filter('SnapshotDate', run_date)
            stage_df.write.format('delta').mode('overwrite').option('replaceWhere', filt).saveAsTable(stage_table)
            SparkHelpers.vacuum_table(stage_table, 180)

    @staticmethod
    def snapshot_to_gold(src_table: str, dest_table: str, etl_date: str):
        _session.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", False)
        df = _session.table(src_table).filter(_col('SnapshotDate') == etl_date)
        filt = SparkHelpers.partition_filter('SnapshotDate', etl_date)
        df.write.format('delta').mode('overwrite').option('replaceWhere', filt).saveAsTable(dest_table)