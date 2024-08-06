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
                    
                    # Convert DataFrame to list of tuples
                    data_values = [tuple(row) for row in df.to_numpy()]
                    
                    # Use execute_values for efficient bulk insert
                    execute_values(cur, insert_query, data_values)
                    
                    # Commit the transaction
                    conn.commit()
                except Exception as ex:
                    logger.exception(ex)

    # def check_csv_row(self, csv_file_path):
    #     with open(csv_file_path, 'r') as file:
    #         csv_reader = csv.reader(file)
    #         row_count = sum(1 for row in csv_reader)  # Counting the rows
    #         return row_count
        
    # def convert_to_float(value):
    #     try:
    #         return float(value.strip())
    #     except AttributeError:
    #         return value

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

                    # row_count = self.check_csv_row(csv_file)  
                    # df_fct = []
                    df_hioko = []
                    update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                    # elif row_count == 6:
                        
                    #     grr_filename_fct = os.path.basename(csv_file)
                    #     df_input_fct_1 = pd.read_csv(csv_file, header=None, nrows=1)
                    #     df_input_fct_2 = pd.read_csv(csv_file, header=None, skiprows= 1)
                    #     grr_mc_code_fct = grr_filename_fct.split("+")[2]
                    #     grr_pd_name_fct = grr_filename_fct.split("+")[0]
                    #     if grr_pd_name_fct in df['prd'].values:
                    #         grr_mc_type_fct = df.loc[df['prd'] == grr_pd_name_fct, 'type'].values[0]

                    #     grr_sn_fct = df_input_fct_1[0][0]
                    #     grr_meas_time_fct = df_input_fct_1[5][0]
                    #     grr_dimension_fct = df_input_fct_2.iloc[0, [16, 17, 18, 19, 27, 28]].T
                    #     grr_value_fct = df_input_fct_2.iloc[4, [16, 17, 18, 19, 27, 28]].T
                    #     grr_lsl_fct = df_input_fct_2.iloc[2, [16, 17, 18, 19, 27, 28]].T
                    #     grr_usl_fct = df_input_fct_2.iloc[1, [16, 17, 18, 19, 27, 28]].T
                    #     df_fct = pd.DataFrame({ 
                    #         'grr_filename': grr_filename_fct, 
                    #         'grr_mc_type': grr_mc_type_fct, 
                    #         'grr_mc_code': grr_mc_code_fct, 
                    #         'grr_pd_name': grr_pd_name_fct, 
                    #         'grr_sn': grr_sn_fct, 
                    #         'grr_dimension': grr_dimension_fct, 
                    #         'grr_value': grr_value_fct, 
                    #         'grr_meas_time': grr_meas_time_fct, 
                    #         'grr_lsl': grr_lsl_fct, 
                    #         'grr_usl': grr_usl_fct,
                    #         'update_date': update_date
                    #     })
                    #     df = df_fct
                    #     self.db_smt_grr_datalake(df)
                        
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
        self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)