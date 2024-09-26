from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import csv
import glob
import math
import numpy as np
import os
import pandas as pd
import psycopg2
import queue
import schedule
import shutil
import sys

INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/GRR/PSI"

class Command(CustomBaseCommand):

    def backup_error(self, path_back, csv_file):
        ERROR_PATH = os.path.join(path_back, "ERROR")
        if not os.path.exists(ERROR_PATH):
            os.makedirs(ERROR_PATH)
        if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(csv_file))):
            os.remove(os.path.join(ERROR_PATH, os.path.basename(csv_file)))
        shutil.move(csv_file, ERROR_PATH)  
        
    def backup(self, path_back, csv_file):
        BACKUP_PATH = os.path.join(path_back, "BACKUP")
        if not os.path.exists(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)
        if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(csv_file))):
            os.remove(os.path.join(BACKUP_PATH, os.path.basename(csv_file)))
        shutil.move(csv_file, BACKUP_PATH)
    
    def convert_to_datetime_object(self, date_time):
        try:
            input_datetime = datetime.strptime(date_time, '%m/%d/%Y %H:%M:%S %p')
        except ValueError:
            try:
                input_datetime = datetime.strptime(date_time, '%d/%m/%Y %H:%M:%S')
            except ValueError:
                try:
                    input_datetime = datetime.strptime(date_time, '%Y/%m/%d %H:%M:%S.%f')
                except ValueError:
                    try:
                        input_datetime = datetime.strptime(date_time, '%Y/%m/%d %H:%M:%S')
                    except ValueError:
                        try:
                            input_datetime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            raise ValueError("Invalid date format")

        output_date_string = input_datetime.strftime('%Y-%m-%d %H:%M:%S')
        return output_date_string
    
    def db_smt_grr_datalake(self, df, path_back, csv_file):
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                try:
                    insert_query = """
                        INSERT INTO smt_grr_datalake (grr_filename, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension, grr_value, grr_meas_time, grr_lsl, grr_usl, update_date) 
                        VALUES %s
                        ON CONFLICT (grr_filename, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension, grr_meas_time)
                        DO UPDATE
                        SET grr_value = EXCLUDED.grr_value,
                            grr_lsl = EXCLUDED.grr_lsl,
                            grr_usl = EXCLUDED.grr_usl, 
                            update_date = EXCLUDED.update_date
                    """
                    df = df.drop_duplicates(subset=['grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension', 'grr_meas_time'])
                    data_values = [tuple(row) for row in df.to_numpy()]
                    execute_values(cur, insert_query, data_values)
                    conn.commit()
                except Exception as ex:
                    logger.exception(ex)
                    self.backup_error(path_back, csv_file)

    @logger.catch
    def run(self):
        logger.log("START", None)
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT DISTINCT 
                    job_id, 
                    trim(mc_code) as mc_code,        
                    trim(dimension) as dimension,
                    trim(tester_code) as tester_code                               
                FROM smt.smt_grr_job_record
                WHERE mc_type = 'PSI'
                AND is_check = false 
                OR is_check IS null;
                """)
                # Fetch all records and convert to DataFrame
                job_record_list = cur.fetchall()
                df_cur = pd.DataFrame(job_record_list, columns=[ 'job_id', 'mc_code', 'dimension', 'tester_code'])
                if df_cur.empty:
                    pass
                for job in df_cur.itertuples():
                    dimensions = job.dimension
                    mc = job.mc_code
                    tester_code = job.tester_code
                    folder = glob.glob(f"{INPUT_PATH}/{mc}_{tester_code}/*")
                    path_back = f"{INPUT_PATH}/{mc}_{tester_code}"
                    df_list = []
                    for csv_file in folder:
                        filename = os.path.basename(csv_file)
                        if filename in ["ERROR", "BACKUP"]:
                            continue
                        else:
                            try:
                                df = pd.read_csv(csv_file)
                                matching_columns = [col for col in df.columns if any(col.startswith(dim) for dim in dimensions)]
                                df_filtered = df[matching_columns]
                                value_psi = df_filtered.iloc[0, 0]
                                grr_filename = os.path.basename(csv_file)
                                prd_name = grr_filename.split("_")[1].replace(' ', '')
                                prd = "-".join(prd_name.split("-")[0:2])
                                if prd in ['RGPZ-159MW', 'RGPZ-160MW']:
                                    lsl, usl, value = 0.75, 1.45, value_psi * 1_000_000
                                elif prd in ['RGPZ-086MW', 'RGPZ-127MW', 'RGPZ-143MW']:
                                    lsl, usl, value = 0, 5, value_psi * 0.01
                                else:
                                    continue 
                                for index, row in df.iterrows():  
                                    justment = row[4]
                                    if justment == 'OK':
                                        meas_time = row[1]   
                                        meas_time_psi = self.convert_to_datetime_object(meas_time)  
                                        sn = row[2]
                                        update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
                                        df_psi= pd.DataFrame({ 
                                            'grr_filename': [grr_filename],
                                            'grr_mc_type': ['PSI'],
                                            'grr_mc_code': [mc],
                                            'grr_pd_name': [prd],
                                            'grr_sn': [sn],
                                            'grr_dimension': [dimensions],
                                            'grr_value': [round(value, 4)],
                                            'grr_meas_time': [meas_time_psi],
                                            'grr_lsl': [lsl],
                                            'grr_usl': [usl],
                                            'update_date': [update_date]
                                        })
                                        df_list.append(df_psi)
                                        self.backup(path_back, csv_file)
                            except Exception as ex:
                                logger.exception(ex)
                                self.backup_error(path_back, csv_file)  
                                continue   
                df_out = pd.concat(df_list, ignore_index=True)
                self.db_smt_grr_datalake(df_out, path_back, csv_file)

        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)