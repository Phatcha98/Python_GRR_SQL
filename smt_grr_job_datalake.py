from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import psycopg2
import queue
import schedule
import sys

class Command(CustomBaseCommand):

    @logger.catch  
    def run(self):
        logger.log("START", None)
        
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        SELECT 
                            job_id, 
                            REPLACE(TRIM(op_id), ' ', '') AS op_id, 
                            REPLACE(TRIM(mc_type), ' ', '') AS mc_type, 
                            REPLACE(TRIM(tester_code), ' ', '') AS tester_code, 
                            REPLACE(TRIM(grr_sn), ' ', '') AS grr_sn, 
                            REPLACE(TRIM(dimension), ' ', '') AS dimension, 
                            id, 
                            REPLACE(TRIM(mc_code), ' ', '') AS mc_code, 
                            create_date
                        FROM 
                            smt_grr_job_record
                        WHERE 
                            is_check = False;
                    """)
                    job_record_list = cur.fetchall()
                    
                    for job_record in job_record_list:

                        cur.execute(f"""
                            SELECT grr_mc_type, grr_mc_code, grr_sn, grr_dimension, grr_value, grr_meas_time, grr_lsl, grr_usl, grr_pd_name
                            FROM smt_grr_datalake
                            WHERE grr_sn = '{job_record[4]}'
                            AND grr_dimension = '{job_record[5]}'
                            AND grr_mc_code = '{job_record[7]}'
                            AND grr_meas_time::date = '{job_record[8].date()}'
                            ORDER BY grr_meas_time
                        """)

                        grr_data_list = cur.fetchall()
                        
                        if grr_data_list and len(grr_data_list) >= 9:
                            output_data_list = []
                            op_id_split = [x for x in job_record[1].split('+')[:3] for _ in range(3)]
                            op_id_list = [(x, y) for x, y in zip(list(range(1, 4))*3, op_id_split)]
                            for op_id, grr_data in zip(op_id_list, grr_data_list[:9]):
                                    formatted_value = round(grr_data[4], 4)
                                    output_data_list.append((
                                        grr_data[0], job_record[3], grr_data[2], op_id[1], 
                                        grr_data[3], op_id[0], formatted_value, 
                                        grr_data[5], grr_data[6], grr_data[7], 
                                        job_record[0], grr_data[8]
                                    ))
                                                            
                            # insert data to smt_grr_job_datalake
                            insert_query = """
                                INSERT INTO smt_grr_job_datalake
                                (mc_type, mc_code, sn, op_id, dimension, "time", value, meas_time, lsl, usl, job_id, prd)
                                VALUES %s
                            """
                            
                            execute_values(cur, insert_query, output_data_list)
                            conn.commit()
                            
                            # update is_check from smt_grr_job_record
                            cur.execute(f"""
                                UPDATE smt_grr_job_record
                                SET is_check = true
                                WHERE id = {job_record[6]}
                            """)
                            conn.commit()
                        
                            logger.success(
                                f"Select data from smt_grr_job_record map with smt_grr_datalake and insert data to smt_grr_job_datalake",
                                input_process=f"data from smt_grr_job_record map with smt_grr_datalake",
                                output_process=f"smt_grr_job_datalake"
                            )
                    
                except Exception as ex:
                    logger.exception(ex)

        logger.log("STOP", None)

    def handle(self, *args, **options):
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)