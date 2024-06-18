# Created By : Joe Antony
# Modified By : Kumara d
# Copyright (c) ibsplc.com
# Purpose : 11-2 Current Year + Same Date Of The
# Previous Year/Same Day Of The Previous Year Combined Processing
# =====================================================

# importing required modules
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime
from commons.dm_common_functions import *
from commons.redshift_common_functions import *

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])  # getting job parameter

sc = SparkContext()  # getting spark context
glueContext = GlueContext(sc)  # getting glue context
spark = glueContext.spark_session  # spark session to use spark sql
logger = glueContext.get_logger()  # logger obj getting from glue logger

job = Job(glueContext)  # it is specific to glue to get the job properties
job.init(args['JOB_NAME'], args)  # job initialization

# list will be used to record the messages and finally this
# will be written to log path
job_name = args['JOB_NAME']
log_msg = []
# calling get_json_config method to parse json data
job_config_json = get_json_config(l1_protected_bucket,
                                  dm_creation_jobs_config)

# getting config variables
locals().update(job_config_json[job_name])

# calling get_dwh_log_path_variable method to get log path
execution_log_path = get_dwh_log_path_variable(job_config_json,
                                               job_name, schema_name1)


def main():
    try:
        conn = create_redshift_connection()
        cur = conn.cursor()
    except Exception as e:
        logger_warning(logger, log_msg,
                       'Couldnt able to connect with Redshift')
        raise ValueError('Couldnt able to connect with Redshift')

    try:
        insert_table_query = \
            f" INSERT INTO {ouput_schema}.{target_table} " \
            " WITH CONTENTS AS ( " \
            " SELECT " \
            " A.key as key,	 " \
            " A.flt_date  as flt_date ,	 " \
            " SPLIT_PART(A.key, ':', 2) as ope_flt_no,	 " \
            " CASE " \
            " WHEN B.route is not Null  " \
            " THEN B.route  " \
            " ELSE A.route " \
            " END	as route, " \
            " SPLIT_PART(A.key, ':', 5) as cmp_cls, " \
            " SPLIT_PART(A.key, ':', 3) as dep_ap, " \
            " SPLIT_PART(A.key, ':', 4) as arr_ap, " \
            " A.act_seat as act_seat,	 " \
            " A.phy_seat  as phy_seat ,	 " \
            " A.sales_seat as sales_seat,	 " \
            " A.act_seat_ly_dt as act_seat_ly_dt,	 " \
            " A.phy_seat_ly_dt   as phy_seat_ly_dt  ,	 " \
            " A.sales_seat_ly_dt as sales_seat_ly_dt,	 " \
            " B.act_seat_ly_day as act_seat_ly_day,	 " \
            " B.phy_seat_ly_day as phy_seat_ly_day,	 " \
            " B.sales_seat_ly_day as sales_seat_ly_day,	 " \
            " A.seat_km as seat_km,	 " \
            " A.seat_km_ly_dt as seat_km_ly_dt,	 " \
            " B.seat_km_ly_day as seat_km_ly_day,	 " \
            " A.bkg_cls_cd as bkg_cls_cd,	 " \
            " A.sub_cls_cd as sub_cls_cd,	 " \
            " A.num_of_sub_reved   as num_of_sub_reved  ,	 " \
            " A.num_of_sub_reved_ly_dt as num_of_sub_reved_ly_dt,	 " \
            " B.num_of_sub_reved   as num_of_sub_reved_ly_day,	 " \
            " A.bod_dt as bod_dt,	 " \
            " A.ope_carr_cd as ope_carr_cd,	 " \
            " A.mkt_carr_cd as mkt_carr_cd,	 " \
            " A.mkt_flt_no as mkt_flt_no,	 " \
            " A.std as std,	 " \
            " A.std_ly_dt as std_ly_dt,	 " \
            " B.std_ly_day as std_ly_day,	 " \
            " A.sta as sta,	 " \
            " A.sta_ly_dt as sta_ly_dt,	 " \
            " B.sta_ly_day as sta_ly_day,	 " \
            " A.eqp_cd as eqp_cd,	 " \
            " A.eqp_cd_ly_dt as eqp_cd_ly_dt,	 " \
            " B.eqp_cd_ly_day as eqp_cd_ly_day,	 " \
            " A.max as max,	 " \
            " A.max_ly_dt as max_ly_dt,	 " \
            " B.max_ly_day as max_ly_day,	 " \
            " A.nop as nop,	 " \
            " A.nop_ly_dt as nop_ly_dt,	 " \
            " B.nop_ly_day as nop_ly_day,	 " \
            " A.mng_res_cnt as mng_res_cnt,	 " \
            " A.mng_res_cnt_ly_dt as mng_res_cnt_ly_dt,	 " \
            " B.mng_res_cnt_ly_day as mng_res_cnt_ly_day,	 " \
            " A.snap_shot_flg as snap_shot_flg,	 " \
            " A.sect_cd as sect_cd,	 " \
            " A.route_cd as route_cd	 " \
            f" FROM {ouput_schema}.{input_tbl1} A " \
            f" JOIN {ouput_schema}.{input_tbl2} B ON " \
            " A.bod_dt = B.bod_dt	AND " \
            " A.key = B.key AND " \
            " A.bkg_cls_cd = B.bkg_cls_cd AND " \
            " A.sub_cls_cd = B.sub_cls_cd	 " \
            " UNION " \
            " SELECT " \
            " A.key as key,	 " \
            " A.flt_date  as flt_date ,	 " \
            " SPLIT_PART(A.key, ':', 2) as ope_flt_no,	 " \
            " A.route	as route, " \
            " SPLIT_PART(A.key, ':', 5) as cmp_cls, " \
            " SPLIT_PART(A.key, ':', 3) as dep_ap, " \
            " SPLIT_PART(A.key, ':', 4) as arr_ap, " \
            " A.act_seat as act_seat,	 " \
            " A.phy_seat  as phy_seat ,	 " \
            " A.sales_seat as sales_seat,	 " \
            " A.act_seat_ly_dt as act_seat_ly_dt,	 " \
            " A.phy_seat_ly_dt   as phy_seat_ly_dt  ,	 " \
            " A.sales_seat_ly_dt as sales_seat_ly_dt,	 " \
            " B.act_seat_ly_day as act_seat_ly_day,	 " \
            " B.phy_seat_ly_day as phy_seat_ly_day,	 " \
            " B.sales_seat_ly_day as sales_seat_ly_day,	 " \
            " A.seat_km as seat_km,	 " \
            " A.seat_km_ly_dt as seat_km_ly_dt,	 " \
            " B.seat_km_ly_day as seat_km_ly_day,	 " \
            " A.bkg_cls_cd as bkg_cls_cd,	 " \
            " A.sub_cls_cd as sub_cls_cd,	 " \
            " A.num_of_sub_reved   as num_of_sub_reved  ,	 " \
            " A.num_of_sub_reved_ly_dt as num_of_sub_reved_ly_dt,	 " \
            " B.num_of_sub_reved   as num_of_sub_reved_ly_day,	 " \
            " A.bod_dt as bod_dt,	 " \
            " A.ope_carr_cd as ope_carr_cd,	 " \
            " A.mkt_carr_cd as mkt_carr_cd,	 " \
            " A.mkt_flt_no as mkt_flt_no,	 " \
            " A.std as std,	 " \
            " A.std_ly_dt as std_ly_dt,	 " \
            " B.std_ly_day as std_ly_day,	 " \
            " A.sta as sta,	 " \
            " A.sta_ly_dt as sta_ly_dt,	 " \
            " B.sta_ly_day as sta_ly_day,	 " \
            " A.eqp_cd as eqp_cd,	 " \
            " A.eqp_cd_ly_dt as eqp_cd_ly_dt,	 " \
            " B.eqp_cd_ly_day as eqp_cd_ly_day,	 " \
            " A.max as max,	 " \
            " A.max_ly_dt as max_ly_dt,	 " \
            " B.max_ly_day as max_ly_day,	 " \
            " A.nop as nop,	 " \
            " A.nop_ly_dt as nop_ly_dt,	 " \
            " B.nop_ly_day as nop_ly_day,	 " \
            " A.mng_res_cnt as mng_res_cnt,	 " \
            " A.mng_res_cnt_ly_dt as mng_res_cnt_ly_dt,	 " \
            " B.mng_res_cnt_ly_day as mng_res_cnt_ly_day,	 " \
            " A.snap_shot_flg as snap_shot_flg,	 " \
            " A.sect_cd as sect_cd,	 " \
            " A.route_cd as route_cd	 " \
            f" FROM {ouput_schema}.{input_tbl1} A " \
            f" LEFT JOIN {ouput_schema}.{input_tbl2} B ON " \
            " A.bod_dt = B.bod_dt	AND " \
            " A.key = B.key AND " \
            " A.bkg_cls_cd = B.bkg_cls_cd AND " \
            " A.sub_cls_cd = B.sub_cls_cd	 " \
            " WHERE " \
            " B.key is Null " \
            " UNION " \
            " SELECT " \
            " B.key as key,	 " \
            " B.flt_date  as flt_date ,	 " \
            " SPLIT_PART(B.key, ':', 2) as ope_flt_no,	 " \
            " B.route	as route, " \
            " SPLIT_PART(B.key, ':', 5) as cmp_cls, " \
            " SPLIT_PART(B.key, ':', 3) as dep_ap, " \
            " SPLIT_PART(B.key, ':', 4) as arr_ap, " \
            " A.act_seat as act_seat,	 " \
            " A.phy_seat  as phy_seat ,	 " \
            " A.sales_seat as sales_seat,	 " \
            " A.act_seat_ly_dt as act_seat_ly_dt,	 " \
            " A.phy_seat_ly_dt   as phy_seat_ly_dt  ,	 " \
            " A.sales_seat_ly_dt as sales_seat_ly_dt,	 " \
            " B.act_seat_ly_day as act_seat_ly_day,	 " \
            " B.phy_seat_ly_day as phy_seat_ly_day,	 " \
            " B.sales_seat_ly_day as sales_seat_ly_day,	 " \
            " A.seat_km as seat_km,	 " \
            " A.seat_km_ly_dt as seat_km_ly_dt,	 " \
            " B.seat_km_ly_day as seat_km_ly_day,	 " \
            " B.bkg_cls_cd as bkg_cls_cd,	 " \
            " B.sub_cls_cd as sub_cls_cd,	 " \
            " A.num_of_sub_reved   as num_of_sub_reved  ,	 " \
            " A.num_of_sub_reved_ly_dt as num_of_sub_reved_ly_dt,	 " \
            " B.num_of_sub_reved   as num_of_sub_reved_ly_day,	 " \
            " B.bod_dt as bod_dt,	 " \
            " A.ope_carr_cd as ope_carr_cd,	 " \
            " A.mkt_carr_cd as mkt_carr_cd,	 " \
            " A.mkt_flt_no as mkt_flt_no,	 " \
            " A.std as std,	 " \
            " A.std_ly_dt as std_ly_dt,	 " \
            " B.std_ly_day as std_ly_day,	 " \
            " A.sta as sta,	 " \
            " A.sta_ly_dt as sta_ly_dt,	 " \
            " B.sta_ly_day as sta_ly_day,	 " \
            " A.eqp_cd as eqp_cd,	 " \
            " A.eqp_cd_ly_dt as eqp_cd_ly_dt,	 " \
            " B.eqp_cd_ly_day as eqp_cd_ly_day,	 " \
            " A.max as max,	 " \
            " A.max_ly_dt as max_ly_dt,	 " \
            " B.max_ly_day as max_ly_day,	 " \
            " A.nop as nop,	 " \
            " A.nop_ly_dt as nop_ly_dt,	 " \
            " B.nop_ly_day as nop_ly_day,	 " \
            " A.mng_res_cnt as mng_res_cnt,	 " \
            " A.mng_res_cnt_ly_dt as mng_res_cnt_ly_dt,	 " \
            " B.mng_res_cnt_ly_day as mng_res_cnt_ly_day,	 " \
            " A.snap_shot_flg as snap_shot_flg,	 " \
            " A.sect_cd as sect_cd,	 " \
            " A.route_cd as route_cd	 " \
            f" FROM {ouput_schema}.{input_tbl2} B " \
            f" LEFT JOIN {ouput_schema}.{input_tbl1} A ON " \
            " A.bod_dt = B.bod_dt	AND " \
            " A.key = B.key AND " \
            " A.bkg_cls_cd = B.bkg_cls_cd AND " \
            " A.sub_cls_cd = B.sub_cls_cd	 " \
            " WHERE " \
            " A.key is Null " \
            " ) " \
            " SELECT * FROM CONTENTS ; "
        logger_warning(logger, log_msg,
                       f' insert query: {insert_table_query} ')
        cur.execute(insert_table_query)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        logger_warning(logger, log_msg,
                       f'Error in Inserting datas into {target_table} ')
        raise ValueError(f'Error in Inserting datas into {target_table} ')

try:
    main()
    # After successful operations, submitting the final result
    job.commit()
    logger_info(logger, log_msg, '*******"Job Commit Successful"*******')
except Exception as ex:
    logger_warning(logger, log_msg, '*******"Job Skipped"*******')
    raise ValueError('Job Skipped')
finally:
    s3.Object(l1_protected_bucket, execution_log_path + job_name +
              date_time + log_ext).put(Body="\n".join(log_msg))
