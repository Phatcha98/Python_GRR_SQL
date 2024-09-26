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

INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/GRR/Hioki"

class Command(CustomBaseCommand):
    
    def backup_error(self, csv_file):
        ERROR_PATH = os.path.join(os.path.dirname(csv_file), "ERROR")
        if not os.path.exists(ERROR_PATH):
            os.makedirs(ERROR_PATH)
        if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(csv_file))):
            os.remove(os.path.join(ERROR_PATH, os.path.basename(csv_file)))
        shutil.move(csv_file, ERROR_PATH)  

    def db_smt_grr_datalake(self, df):
        df.loc[df['grr_pd_name'].isin(['RGPZ-299NL', 'RGPZ-300NL', 'RGPZ-299ML']), ['grr_lsl', 'grr_usl']] = [0, 100]
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

    @logger.catch
    def run(self):
        logger.log("START", None)
        
        folder = glob.glob(f"{INPUT_PATH}/*")
        for csv_file in folder:
            filename = os.path.basename(csv_file)
            if filename == "ERROR" or filename == "BACKUP"  or filename =="MASTER":
                continue
            else:
                try:
                    db = connections["10.17.66.121.iot.smt"].settings_dict
                    with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
                        with conn.cursor() as cur:
                            query = ("""
                                select *
                                from smt_grr_master_type
                                where (prd, type) in (
                                    select  prd, type
                                    from smt_grr_master_type
                                    group by prd, type
                                )
                                """)
                            cur.execute(query)
                            master = cur.fetchall()
                            columns = [desc[0] for desc in cur.description]
                            df = pd.DataFrame(master, columns=columns)

                    df_hioko = []
                    update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # else:
                    grr_filename_hioki = os.path.basename(csv_file)
                    df_input_hioko_1 = pd.read_csv(csv_file, header=None, nrows=1)
                    df_input_hioko_1 = pd.DataFrame(df_input_hioko_1)
                    df_input_hioko_2 = pd.read_csv(csv_file, header=None, skiprows=9)
                    df_input_hioko_2 = pd.DataFrame(df_input_hioko_2)
                    # if df_input_hioko_1.iloc[0, 1] == "OK":
                    try:
                        if df_input_hioko_1.iloc[0, 0]  == "[ Test Results ]":
                            df_input_hioko_3 = pd.read_csv(csv_file, header=None, skiprows=1, nrows=4)
                            df_input_hioko_3 = pd.DataFrame(df_input_hioko_3)
                            df_input_hioko_4 = pd.read_csv(csv_file, header=None, skiprows=5, nrows=1)
                            df_input_hioko_4 = pd.DataFrame(df_input_hioko_4)
                            grr_mc_code_hioko = df_input_hioko_3[1][0].split("_")[1].replace(' ', '')
                            grr_sn_hioko = df_input_hioko_3[1][3].replace(' ', '')
                            grr_meas_time_hioko_time = df_input_hioko_4[1][0]+" "+df_input_hioko_4[2][0]
                            grr_meas_time_hioko_time_dt = datetime.strptime(grr_meas_time_hioko_time.strip(), '%Y/%m/%d %H:%M:%S')
                            grr_meas_time_hioko = grr_meas_time_hioko_time_dt.strftime('%Y-%m-%d %H:%M:%S')

                        else:
                            df_input_hioko_5 = pd.read_csv(csv_file, header=None, skiprows=5, nrows=1)
                            df_input_hioko_5 = pd.DataFrame(df_input_hioko_5)
                            grr_mc_code_hioko = df_input_hioko_1[2][0].split("_")[1].replace(' ', '')
                            grr_sn_hioko = df_input_hioko_1[0][0].replace(' ', '')
                            grr_meas_time_hioko_time = df_input_hioko_5[1][0]+" "+df_input_hioko_5[2][0]
                            grr_meas_time_hioko_time_dt = datetime.strptime(grr_meas_time_hioko_time.strip(), '%Y/%m/%d %H:%M:%S')
                            grr_meas_time_hioko = grr_meas_time_hioko_time_dt.strftime('%Y-%m-%d %H:%M:%S')

                        # grr_mc_code_hioko = df_input_hioko_1[3][0].split("(")[1].split(")")[0]
                        grr_pd_name_hioko = grr_filename_hioki.split("+")[0]
                        
                        if grr_pd_name_hioko in df['prd'].values:
                            grr_mc_type_hioko = df.loc[df['prd'] == grr_pd_name_hioko, 'type'].values[0]
                        
                        for index, row in df_input_hioko_2.iterrows():
                            grr_dimension_hioki = row[2].replace(' ', '')
                            grr_dimension_hioki_c = row[2].replace(' ', '')[:3]
                            if grr_dimension_hioki_c == "SUS":
                                if isinstance(row[9], str):
                                    grr_lsl_hioko = float(row[9].strip())
                                else:
                                    grr_lsl_hioko = float(row[9])

                                if isinstance(row[8], str):
                                    grr_usl_hioko = float(row[8].strip())
                                else:
                                    grr_usl_hioko = float(row[8])

                                grr_value_hioko = round(row[12],5)
                            else:
                                continue
                            df_hioko.append([grr_filename_hioki, grr_mc_type_hioko, grr_mc_code_hioko, grr_pd_name_hioko, grr_sn_hioko, 
                                            grr_dimension_hioki, grr_value_hioko, grr_meas_time_hioko, grr_lsl_hioko, grr_usl_hioko, update_date])
                        columns = [
                            'grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension',
                            'grr_value', 'grr_meas_time', 'grr_lsl', 'grr_usl', 'update_date'
                        ]
                        df_hioko = pd.DataFrame(df_hioko, columns=columns)
                        df = df_hioko
                        if df.empty:
                            self.backup_error(csv_file)
                        else:
                            self.db_smt_grr_datalake(df)

                            BACKUP_PATH = os.path.join(INPUT_PATH, "BACKUP")
                            if not os.path.exists(BACKUP_PATH):
                                os.makedirs(BACKUP_PATH)
                            if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(csv_file))):
                                os.remove(os.path.join(BACKUP_PATH, os.path.basename(csv_file)))
                            shutil.move(csv_file, BACKUP_PATH)

                    except Exception as ex:
                        logger.exception(ex)
                        self.backup_error(csv_file)
                        pass
                except Exception as ex:
                    logger.exception(ex)
                    self.backup_error(csv_file)
                    pass

        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)