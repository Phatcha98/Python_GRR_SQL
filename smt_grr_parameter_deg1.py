from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import numpy as np
import pandas as pd
import psycopg2
import queue
import schedule
import sys

class Command(CustomBaseCommand):
    
    def query_by_job_id(self, df_job):
        
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                for index, row in df_job.iterrows():
                    job = row[0]
                    query = """
                        WITH avg_window AS (
                            SELECT 
                                t.mc_type,
                                t.mc_code,
                                t.sn,
                                t.dimension,
                                t.meas_time,
                                t.op_id,
                                t."time",
                                t.value,
                                avg(t.value) OVER (PARTITION BY t.sn, t.op_id) AS avg_1,
                                sum(t.value) OVER (PARTITION BY t.sn, t.op_id) AS sum,
                                (max(t.value) OVER (PARTITION BY t.sn, t.op_id)) - (min(t.value) OVER (PARTITION BY t.sn, t.op_id)) AS range,
                                t.lsl,
                                t.usl,
                                t.job_id
                            FROM 
                                smt_grr_job_datalake t
                            WHERE 
                                t.job_id = %s
                        ),
                        avg_window1 AS (
                            SELECT
                                mc_type,
                                mc_code,
                                sn,
                                dimension,
                                meas_time,
                                op_id,
                                "time",
                                "value",
                                AVG(avg_1) OVER (PARTITION BY sn, op_id) AS avg,
                                sum,
                                range,
                                (SELECT sum(power(t2.value::double precision, 2)) 
                                FROM smt_grr_job_datalake t2 
                                WHERE t2.job_id = %s) AS sum_sq_all,
                                lsl,
                                usl,
                                job_id
                            FROM 
                                avg_window
                        ),
                        avg_window2 AS (
                            SELECT
                                mc_type,
                                mc_code,
                                sn,
                                dimension,
                                meas_time,
                                avg,
                                sum,
                                range, 
                                "time",
                                "value",
                                AVG(avg) OVER (PARTITION BY op_id, job_id) AS x_bar,
                                AVG(range) OVER (PARTITION BY op_id, job_id) AS r_bar,
                                lsl,
                                usl,
                                job_id,
                                op_id,
                                sum_sq_all
                            FROM 
                                avg_window1
                            where job_id = %s
                        )
                        SELECT 
                            distinct 
                            mc_type,
                            mc_code,
                            sn,
                            avg,
                            sum,
                            range,
                            x_bar,
                            r_bar,
                            lsl,
                            usl,
                            SUM("value") OVER (PARTITION BY sn) AS total_vertical,
                            AVG(avg) OVER (PARTITION BY sn) AS x_bar_p,
                            sum_sq_all,
                            job_id,
                            op_id
                        FROM 
                            avg_window2
                        where job_id = %s;
                    """
                    cur.execute(query, (job, job, job, job))
                    result = cur.fetchall()
                    
                    df = pd.DataFrame(result)
                    df.replace({np.nan: None, 'nan': None}, inplace=True)
                    df[15] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    columns_to_round = list(range(3, 13))
                    df[columns_to_round] = df[columns_to_round].applymap(lambda x: round(x, 6) if isinstance(x, (int, float)) else x)
                    
                    try:
                        insert_query = """
                            INSERT INTO smt_grr_parameter_deg1 (mc_type, mc_code, sn, avg, sum, "range", x_bar, r_bar, lsl, usl, total_vertical, x_bar_p, sum_sq_all, job_id, op_id, update_date)
                            VALUES %s
                            ON CONFLICT (mc_type, mc_code, sn, job_id, op_id)
                            DO UPDATE
                            SET avg = EXCLUDED.avg,
                                sum = EXCLUDED.sum,
                                range = EXCLUDED.range, 
                                x_bar = EXCLUDED.x_bar,
                                r_bar = EXCLUDED.r_bar,
                                lsl = EXCLUDED.lsl, 
                                usl = EXCLUDED.usl,
                                total_vertical = EXCLUDED.total_vertical, 
                                x_bar_p = EXCLUDED.x_bar_p,
                                sum_sq_all = EXCLUDED.sum_sq_all,
                                update_date = EXCLUDED.update_date
                        """
                        
                        data_values = [tuple(row) for row in df.to_numpy()]
                        execute_values(cur, insert_query, data_values)
                        conn.commit()
                        cur.execute("""
                            UPDATE smt_grr_job_datalake 
                            SET is_check = TRUE WHERE job_id = %s
                        """, (row[0],))
                        
                    except Exception as ex:
                        logger.exception(ex)
        
    @logger.catch
    def run(self):
        logger.log("START", None)
        try:
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
                with conn.cursor() as cur:
                    job_id_query = (""" 
                        SELECT job_id, COUNT(job_id) as job_count
                        FROM smt_grr_job_datalake
                        WHERE is_check = false OR is_check IS NULL
                        GROUP BY job_id
                        HAVING COUNT(job_id) = 90;
                    """)
                    cur.execute(job_id_query)
                    result1 = cur.fetchall()
                    columns1 = [desc[0] for desc in cur.description]
                    df_job = pd.DataFrame(result1, columns=columns1)
                    self.query_by_job_id(df_job)
                
        except Exception as ex:
            logger.exception(ex)
                
        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)