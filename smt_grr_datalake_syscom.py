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

INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/GRR/Syscom"

class Command(CustomBaseCommand):
    
    def backup_error(self, csv_file):
        ERROR_PATH = os.path.join(os.path.dirname(csv_file), "ERROR")
        if not os.path.exists(ERROR_PATH):
            os.makedirs(ERROR_PATH)
        if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(csv_file))):
            os.remove(os.path.join(ERROR_PATH, os.path.basename(csv_file)))
        shutil.move(csv_file, ERROR_PATH)  
        
    def backup(self, csv_file):
        BACKUP_PATH = os.path.join(INPUT_PATH, "BACKUP")
        if not os.path.exists(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)
        if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(csv_file))):
            os.remove(os.path.join(BACKUP_PATH, os.path.basename(csv_file)))
        shutil.move(csv_file, BACKUP_PATH)

    def db_smt_grr_datalake(self, df, csv_file):
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
        self.backup(csv_file)
                    
    def data_rgpz_030(self, grr_filename_syscom, grr_mc_type, df_input_syscom, grr_mc_code, grr_pd_name):
        df_syscom = []
        update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        grr_sn = df_input_syscom[0][0]
        grr_dimension = df_input_syscom[18][0]
        grr_dimension_030 = grr_dimension.replace(' ', '')[:3]
        if grr_dimension_030 == "SUS":
            grr_value = round(df_input_syscom[19][0],5)
            date_str = df_input_syscom[5][0]
            try:
                date_obj = datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
                grr_meas_time_syscom = date_obj.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError as e:
                print(f"Error parsing date string: {e}")
            grr_lsl = -10
            grr_usl = 10
            df_syscom.append([grr_filename_syscom, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension,
                    grr_value, grr_meas_time_syscom, grr_lsl, grr_usl, update_date])
            columns = ['grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension',
                    'grr_value', 'grr_meas_time', 'grr_lsl', 'grr_usl', 'update_date']
            df_syscom = pd.DataFrame(df_syscom, columns=columns)
            return df_syscom
        else:
            pass

    def data_rgpz_086(self, grr_filename_syscom, grr_mc_type,  df_input_syscom, grr_mc_code, grr_pd_name):
        df_syscom = []
        update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        grr_sn = df_input_syscom[0][0]
        grr_dimension = df_input_syscom[7][0]
        grr_dimension_086 = grr_dimension.replace(' ', '')[:3]
        if grr_dimension_086 == "SUS":
            grr_value = df_input_syscom[8][0]
            grr_meas_time_syscom = df_input_syscom[5][0]
            grr_lsl = 0
            grr_usl = 5
            df_syscom.append([grr_filename_syscom, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension,
                    grr_value, grr_meas_time_syscom, grr_lsl, grr_usl, update_date])
            columns = ['grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension',
                    'grr_value', 'grr_meas_time', 'grr_lsl', 'grr_usl', 'update_date']
            df_syscom = pd.DataFrame(df_syscom, columns=columns)
            return df_syscom
        else:
            pass

    def data_rgpz_127(self, grr_filename_syscom, grr_mc_type,  df_input_syscom, grr_mc_code, grr_pd_name):
        df_syscom = []
        update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        grr_sn = df_input_syscom[0][0]
        grr_dimension = df_input_syscom[7][0]
        grr_dimension_127 = grr_dimension.replace(' ', '')[:3]
        if grr_dimension_127 == "SUS":
            grr_value = df_input_syscom[8][0]
            grr_meas_time_syscom = df_input_syscom[5][0]
            grr_lsl = 0
            grr_usl = 5
            df_syscom.append([grr_filename_syscom, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension,
                    grr_value, grr_meas_time_syscom, grr_lsl, grr_usl, update_date])
            columns = ['grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension',
                    'grr_value', 'grr_meas_time', 'grr_lsl', 'grr_usl', 'update_date']
            df_syscom = pd.DataFrame(df_syscom, columns=columns)
            return df_syscom
        else:
            pass
        
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
                            df_master = pd.DataFrame(master, columns=columns)
                    try:                       
                        grr_filename_syscom = os.path.basename(csv_file)
                        df_input_syscom = pd.read_csv(csv_file, header=None)
                        grr_pd_name = grr_filename_syscom.split("+")[0].replace(' ', '')
                        if grr_pd_name in df_master['prd'].values:
                            grr_mc_type = df_master.loc[df_master['prd'] == grr_pd_name, 'type'].values[0]
                            if grr_pd_name == "RGPZ-030MW":
                                grr_mc_code = df_input_syscom[2][0].split("_")[1].replace(' ', '')
                                df = self.data_rgpz_030(grr_filename_syscom, grr_mc_type, df_input_syscom, grr_mc_code, grr_pd_name)
                                if df.empty:
                                        self.backup_error(csv_file)
                                else:
                                    self.db_smt_grr_datalake(df, csv_file)
                            elif grr_pd_name == "RGPZ-086MW":
                                grr_mc_code = df_input_syscom[2][0].split("_")[1].replace(' ', '')
                                df = self.data_rgpz_086(grr_filename_syscom, grr_mc_type, df_input_syscom, grr_mc_code, grr_pd_name)
                                if df.empty:
                                        self.backup_error(csv_file)
                                else:
                                    self.db_smt_grr_datalake(df, csv_file)

                            elif grr_pd_name == "RGPZ-127MW":
                                grr_mc_code = df_input_syscom[2][0].split("_")[1].replace(' ', '')
                                df = self.data_rgpz_127(grr_filename_syscom, grr_mc_type,  df_input_syscom, grr_mc_code, grr_pd_name)         
                                if df.empty:
                                        self.backup_error(csv_file)
                                else:
                                    self.db_smt_grr_datalake(df, csv_file)
                            else:               
                                df_syscom = []
                                grr_mc_code = df_input_syscom[2][0].split("_")[1].replace(' ', '')
                                update_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
                                grr_sn = df_input_syscom[0][0]
                                grr_dimension = df_input_syscom[7][0]
                                grr_value = df_input_syscom[8][0]
                                grr_meas_time = df_input_syscom[5][0]
                                date_obj = datetime.strptime(grr_meas_time, '%d/%m/%Y %H:%M:%S')
                                grr_meas_time_syscom = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                                grr_lsl = 0
                                grr_usl = 5
                                df_syscom.append([grr_filename_syscom, grr_mc_type, grr_mc_code, grr_pd_name, grr_sn, grr_dimension,
                                        grr_value, grr_meas_time_syscom, grr_lsl, grr_usl, update_date])
                                columns = ['grr_filename', 'grr_mc_type', 'grr_mc_code', 'grr_pd_name', 'grr_sn', 'grr_dimension',
                                        'grr_value', 'grr_meas_time', 'grr_lsl', 'grr_usl', 'update_date']
                                df_syscom = pd.DataFrame(df_syscom, columns=columns)
                                df = df_syscom

                                if df.empty:
                                    self.backup_error(csv_file)
                                else:
                                    self.db_smt_grr_datalake(df, csv_file)
                                    # BACKUP_PATH = os.path.join(INPUT_PATH, "BACKUP")
                                    # if not os.path.exists(BACKUP_PATH):
                                    #     os.makedirs(BACKUP_PATH)
                                    # if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(csv_file))):
                                    #     os.remove(os.path.join(BACKUP_PATH, os.path.basename(csv_file)))
                                    # shutil.move(csv_file, BACKUP_PATH)
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