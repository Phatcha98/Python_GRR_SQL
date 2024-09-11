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

    def getData(self, id):
        try:
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as connection:
                with connection.cursor() as cursor:
                    data = """
                    select distinct
                        ndc.job_id
                        , concat (
                                    substring (split_part(ndc.job_id, '+', 3) from 1 for 4), '-',
                                    substring (split_part(ndc.job_id, '+', 3) from 5 for 2), '-',
                                    substring (split_part(ndc.job_id, '+', 3) from 7 for 2), ' ',
                                    substring (split_part(ndc.job_id, '+', 3) from 9 for 2), ':',
                                    substring (split_part(ndc.job_id, '+', 3) from 11 for 2), ':',
                                    substring (split_part(ndc.job_id, '+', 3) from 13 for 2)
                                ) as test_date
                        , split_part (ndc.job_id, '+', 2) as tester_code
                        , ndc.ndc
                        , tol.tolerance
                        , judge.judgement
                        , judge.tv
                        , judge.tv_judge
                        , judge.tol_judge
                    from
                            (
                                select
                                    t.job_id
                                    , t.availability_resolution_ndc as ndc
                                from
                                    smt_grr_parameter_deg5_grr t
                                where
                                    t.job_id = %s
                            ) as ndc
                        inner join
                            (
                                select
                                    q.job_id
                                    , round (max (q.tolerance)::numeric, 3) as tolerance
                                from
                                    (
                                    select
                                        t.job_id
                                        , case
                                            when t.item = 'Total Gauge R&R' then t.tolerance
                                            else null
                                        end as tolerance
                                    from
                                        smt_grr_parameter_deg5_grr t
                                    where
                                        t.job_id = %s
                                    ) as q
                                group by
                                    q.job_id
                            ) as tol
                            on ndc.job_id = tol.job_id
                        inner join
                            (
                                select
                                    q1.job_id
                                    , q1.tv
                                    , q1.tol
                                    , q1.tv_judge
                                    , q1.tol_judge
                                    , case
                                        when q1.tv_judge = 'Excellent' or q1.tol_judge = 'Excellent' then 'Excellent'
                                        when q1.tv_judge = 'Accepted' or q1.tol_judge = 'Accepted' then 'Accepted'
                                        when q1.tv_judge = 'Must be improved' and q1.tol_judge = 'Must be improved' then 'Must be improved'
                                        else null
                                    end as judgement
                                from
                                    (
                                    select
                                        q.job_id
                                        , max (q.tv) as tv
                                        , max (q.tol) as tol
                                        , case
                                            when max (q.tv) < 0.1 then 'Excellent'
                                            when max (q.tv) between 0.1 and 0.3 then 'Accepted'    
                                            when max (q.tv) > 0.3 then 'Must be improved'
                                            else null
                                        end as tv_judge
                                        , case
                                            when max (q.tol) < 0.1 then 'Excellent'
                                            when max (q.tol) between 0.1 and 0.3 then 'Accepted'    
                                            when max (q.tol) > 0.3 then 'Must be improved'
                                            else null
                                        end as tol_judge
                                    from
                                        (
                                        select
                                            t.job_id
                                            , case
                                                when t.item = 'Total Gauge R&R' then t.tv::numeric / 100
                                                else null
                                            end as tv
                                            , case
                                                when t.item = 'Total Gauge R&R' then t.tolerance::numeric / 100
                                                else null
                                            end as tol
                                        from
                                            smt_grr_parameter_deg5_grr t
                                        where
                                            t.job_id = %s
                                        ) as q    
                                    group by
                                        q.job_id
                                    ) as q1
                            ) as judge
                            on ndc.job_id = judge.job_id;
                    """
                    cursor.execute(data, (id, id, id))
                    result = cursor.fetchone()
                    col_names = [desc[0] for desc in cursor.description]
                    col_names = [desc[0] for desc in cursor.description]
                    df_result = pd.DataFrame([result], columns=col_names)
                return df_result
            
        except Exception as ex:
            logger.exception(ex)
    
    def selectData(self):
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""   
                            select 
                                distinct 
                                t.job_id
                            from 
                                smt_grr_job_datalake t 
                            """)
                data = cur.fetchall()
                return data

    @logger.catch
    def run(self):

        logger.log("START", None)

        try:             
            id_list = self.selectData()
            id_list = pd.DataFrame(id_list)
            id_list.rename(columns={0: 'id'}, inplace=True)
            
            # where id from id_list
            df = []
            for index, row in id_list.iterrows():
                id = row['id']
                df_data = self.getData(id)
                if df_data is None or df_data.empty:
                    continue
                else:
                    df.append({
                        "job_id": df_data['job_id'],
                        "test_date": df_data['test_date'],
                        "tester_code": df_data['tester_code'],
                        "ndc": df_data['ndc'],
                        "tolerance": df_data['tolerance'],
                        "judgement": df_data['judgement'],
                        "update_date": datetime.now(),
                        "tv": df_data['tv'],
                        "tv_judge": df_data['tv_judge'],
                        "tol_judge": df_data['tol_judge']
                    })
            df = pd.DataFrame(df)
            df = df.applymap(lambda x: x[0] if isinstance(x, pd.Series) else x)
            
            tuple_insert = [tuple(x) for x in df.itertuples(index=False, name=None)]
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany("""
                        INSERT INTO smt.smt_grr_job_result (
                            job_id, test_date, tester_code, ndc, tolerance, judgement, update_date, tv, tv_judge, tol_judge) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_id)
                        DO UPDATE
                        SET test_date = EXCLUDED.test_date,
                            tester_code = EXCLUDED.tester_code,
                            ndc = EXCLUDED.ndc,
                            tolerance = EXCLUDED.tolerance,
                            judgement = EXCLUDED.judgement,
                            update_date = EXCLUDED.update_date,
                            tv = EXCLUDED.tv,
                            tv_judge = EXCLUDED.tv_judge,
                            tol_judge = EXCLUDED.tol_judge;
                    """, tuple_insert)
                    conn.commit()
                        
            logger.success(
                f"select data from smt_grr_parameter_deg5_grr(121) to smt_grr_job_result(121)",
                input_process=f"input smt_grr_parameter_deg5_grr(121)",
                output_process=f"database table smt_grr_job_result(121)"
            )
                        
        except Exception as ex:
            logger.exception(ex)

        logger.log("STOP", None)

    def handle(self, *args, **options):   
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)