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

INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/GRR/MASTER"

class Command(CustomBaseCommand):

    @logger.catch
    def run(self):
        logger.log("START", None)
        
        folder = glob.glob(f"{INPUT_PATH}/*")
        for csv_file in folder:
            
            filename = os.path.basename(csv_file)
            
            if filename == "ERROR" or filename == "BACKUP":
                continue
            
            try:
                df_input = pd.read_csv(csv_file)
                df_input['update_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                db = connections["10.17.66.121.iot.smt"].settings_dict
                with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
                    with conn.cursor() as cur:
                        try:
                            insert_query = """
                                INSERT INTO smt_grr_master_type (prd, type, update_date)
                                VALUES %s
                                ON CONFLICT (prd, type)
                                DO UPDATE
                                SET update_date = EXCLUDED.update_date
                            """
                            data_values = [tuple(row) for row in df_input.to_numpy()]
                            execute_values(cur, insert_query, data_values)
                            conn.commit()

                        except Exception as ex:
                            logger.exception(ex)
                            ERROR_PATH = os.path.join(os.path.dirname(csv_file), "ERROR")
                            if not os.path.exists(ERROR_PATH):
                                os.makedirs(ERROR_PATH)
                            if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(csv_file))):
                                os.remove(os.path.join(ERROR_PATH, os.path.basename(csv_file)))
                            shutil.move(csv_file, ERROR_PATH)  

                BACKUP_PATH = os.path.join(INPUT_PATH, "BACKUP")
                if not os.path.exists(BACKUP_PATH):
                    os.makedirs(BACKUP_PATH)
                if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(csv_file))):
                    os.remove(os.path.join(BACKUP_PATH, os.path.basename(csv_file)))
                shutil.move(csv_file, BACKUP_PATH)

            except Exception as ex:
                logger.exception(ex)
                ERROR_PATH = os.path.join(os.path.dirname(csv_file), "ERROR")
                if not os.path.exists(ERROR_PATH):
                    os.makedirs(ERROR_PATH)
                if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(csv_file))):
                    os.remove(os.path.join(ERROR_PATH, os.path.basename(csv_file)))
                shutil.move(csv_file, ERROR_PATH)  

        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)