from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import psycopg2
import glob
import numpy as np
import pandas as pd
import os
import shutil
import queue
import schedule
import sys

INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/GRR/Nidec"

class Command(CustomBaseCommand):

    def backup_error(self, path_back, folder):
        for file in folder:
            filename = os.path.basename(file)
            if filename in ["ERROR", "BACKUP", "combine"]:
                continue
            ERROR_PATH = os.path.join(path_back, "ERROR")
            if not os.path.exists(ERROR_PATH):
                os.makedirs(ERROR_PATH)
            if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(file))):
                os.remove(os.path.join(ERROR_PATH, os.path.basename(file)))
            shutil.move(file, ERROR_PATH)  
        
    def backup(self,path_back, folder):
        for file in folder:
            filename = os.path.basename(file)
            if filename in ["ERROR", "BACKUP", "combine"]:
                continue
            BACKUP_PATH = os.path.join(path_back, "BACKUP")
            if not os.path.exists(BACKUP_PATH):
                os.makedirs(BACKUP_PATH)
            if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(file))):
                os.remove(os.path.join(BACKUP_PATH, os.path.basename(file)))
            shutil.move(file, BACKUP_PATH)

    def insert_to_db121(self, df_out, df_result, path_back, folder):
        db = connections["10.17.66.121.iot.fpc"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                try:
                    data_tuples = [
                        (row['mc_type'], row['mc_code'], str(row['sn']), str(row['op_id']), row['dimension'], int(row['time']),
                            float(row['value']), pd.to_datetime(row['meas_time']).to_pydatetime(), float(row['lsl']), float(row['usl']),
                            row['job_id'], row['prd'])
                        for _, row in df_out.iterrows()
                    ]
                    cur.executemany("""
                        INSERT INTO fpc_grr_job_datalake
                        (mc_type, mc_code, sn, op_id, dimension, "time", value, meas_time, lsl, usl, job_id, is_check, prd, update_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, %s, now()) 
                        ON CONFLICT (mc_type, mc_code, sn, op_id, meas_time, job_id, prd)
                        DO UPDATE SET 
                            dimension = EXCLUDED.dimension,
                            "time" = EXCLUDED."time",
                            value = EXCLUDED.value,
                            lsl = EXCLUDED.lsl,
                            usl = EXCLUDED.usl,
                            is_check = EXCLUDED.is_check,
                            update_date = EXCLUDED.update_date;
                    """, data_tuples)
                    conn.commit()

                    ids_to_update = df_result['id'].tolist()
                    update_query = """
                        UPDATE fpc_grr_job_record
                        SET is_check = true
                        WHERE id = ANY(%s);
                    """
                    cur.execute(update_query, (ids_to_update,))
                    conn.commit()
                    self.backup(path_back, folder)
                except Exception as ex:
                    logger.exception(ex)
                    self.backup_error(path_back, folder)  

    def coverd_file_with_db121(self, job_id, df_nidec, filename, path_back, folder):
        db = connections["10.17.66.121.iot.fpc"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                try:
                    query = """
                        SELECT *
                        FROM fpc.fpc_grr_job_record
                        WHERE (is_check = false OR is_check IS NULL)
                        AND job_id = %s;
                    """
                    # Pass job_id as a tuple
                    cur.execute(query, (job_id,))
                    results = cur.fetchall()
                    df_result = pd.DataFrame(results, columns=[desc[0] for desc in cur.description])
                    pc_indices = df_result['pc_index'].drop_duplicates()
                    net_dim_columns = df_result['net_dim'].tolist()
                    df_nidec['PieceIndex'] = df_nidec['PieceIndex'].astype(str).str.strip()
                    filtered_df_nidec = df_nidec[
                                        df_nidec['PieceIndex'].isin(pc_indices) &
                                        df_nidec['Judge'].str.upper().isin(["PASS"])
                                    ]
                    unique_net_dims = pd.Series(net_dim_columns).drop_duplicates()
                    # Initialize an empty list to store all DataFrames
                    data_lake = []
                    if filtered_df_nidec['Judge'].eq('PASS').all() or filtered_df_nidec['Judge'].eq('Pass').all():
                        for net_dim in unique_net_dims:
                            if net_dim in filtered_df_nidec.columns:
                                result_df = filtered_df_nidec[['PieceIndex', net_dim]]
                                grouped = result_df.groupby('PieceIndex')
                                date_time = pd.to_datetime(filtered_df_nidec['Date'] + ' ' + filtered_df_nidec['Time'], format='%Y/%m/%d %H:%M:%S')
                                for piece_index, group in grouped:
                                    values = group[net_dim].values
                                    group_index = group.index
                                    meas_time = date_time.loc[group_index].dt.strftime('%Y-%m-%d %H:%M:%S').values  # Correctly apply strftime
                                    temp_df = pd.DataFrame({
                                        'mc_type': [df_result['fixture_type'].iloc[0].replace(' ', '')] * len(values),
                                        'mc_code': [df_result['tester_code'].iloc[0].replace(' ', '')] * len(values),
                                        'sn': [int(piece_index)] * len(values),
                                        'dimension': [net_dim] * len(values),
                                        'value': np.round(values/1000, 4),
                                        'meas_time': meas_time,
                                        'lsl': [df_result['lsl'].iloc[0]] * len(values),
                                        'usl': [df_result['usl'].iloc[0]] * len(values),
                                        'prd': [df_result['equipment_no'].iloc[0].replace(' ', '')] * len(values)
                                    })
                                    data_lake.append(temp_df)
                    final_output = pd.concat(data_lake, ignore_index=True)
                    final_output['sn'] = final_output['sn'].astype(int)
                    final_output = final_output.sort_values(by=['meas_time', 'sn'], ascending=[True, True])
                    if len(final_output) == 90:
                        df_unique_op_id = df_result.drop_duplicates(subset=['op_id'])
                        unique_op_ids = sorted(df_unique_op_id['op_id'].tolist())
                        output_data = []  
                        op_id_split = [op_id for op_id in unique_op_ids for _ in range(30)] 
                        # Prepare op_id_numbers with repeating pattern [1, 1, 1, 2, 2, 2, 3, 3, 3] * 10
                        op_id_numbers = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3] * 3
                        # Pair op_id_numbers with the corresponding op_id_split values
                        op_id_list = [(x, y) for x, y in zip(op_id_numbers, op_id_split)]
                        # Ensure final_output is sliced to 90 items
                        for (num, op_id), grr_data in zip(op_id_list, final_output.iloc[:90].itertuples(index=False)):
                            grr_data_list = list(grr_data)
                            output_data.append((
                                grr_data_list[0],  
                                grr_data_list[1],  
                                grr_data_list[2], 
                                op_id, 
                                grr_data_list[3],
                                num,  
                                grr_data_list[4],  
                                grr_data_list[5],  
                                grr_data_list[6],   
                                grr_data_list[7],
                                job_id,
                                grr_data_list[8]
                            ))

                        df_out = pd.DataFrame(output_data, columns=['mc_type', 'mc_code', 'sn', 'op_id', 'dimension', 'time', 'value', 'meas_time', 'lsl', 'usl', 'job_id', 'prd'])
                        self.insert_to_db121(df_out, df_result, path_back, folder)
                    else:
                        self.backup_error(path_back, folder) 
                except Exception as ex:
                    logger.exception(ex)
                    self.backup_error(path_back, folder)  

    @logger.catch  
    def run(self):
        logger.log("START", None)
        db = connections["10.17.66.121.iot.fpc"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        SELECT DISTINCT 
                        job_id, 
                        trim(equipment_no) as equipment_no, 
                        trim(tester_code) as tester_code, 
                        is_check
                        FROM fpc.fpc_grr_job_record
                        WHERE is_check = false OR is_check IS NULL;
                    """)
                    # Fetch all records and convert to DataFrame
                    job_record_list = cur.fetchall()
                    df_cur = pd.DataFrame(job_record_list, columns=[ 'job_id','equipment_no', 'tester_code', 'is_check'])
                    # duplicates = df_cur.drop_duplicates(subset=['equipment_no', 'tester_code'])

                    for index, row in df_cur.iterrows():
                        job_id = row['job_id']
                        equipment_no = row['equipment_no']
                        tester_code = row['tester_code']
                        folder = glob.glob(f"{INPUT_PATH}/{equipment_no}_{tester_code}/*")
                        path_back = f"{INPUT_PATH}/{equipment_no}_{tester_code}"
                        df_nidec = []
                        for csv_file in folder:
                            filename = os.path.basename(csv_file)
                            if filename in ["ERROR", "BACKUP", "combine"]:
                                continue
                            else:
                                try:
                                    df = pd.read_csv(csv_file, skiprows=1)
                                    df_nidec.append(df)    
                                except Exception as ex:
                                    logger.exception(ex)
                                    self.backup_error(path_back, folder)  
                        if df_nidec:
                            df_nidec = pd.concat(df_nidec, ignore_index=True)
                            self.coverd_file_with_db121(job_id, df_nidec, filename, path_back, folder)
                except Exception as ex:
                    logger.exception(ex)
                    self.backup_error(path_back, folder)

                logger.success(
                    f"Select data from fpc_grr_job_record map with fpc_grr_datalake and insert data to fpc_grr_job_datalake",
                    input_process=f"data from fpc_grr_job_record map with fpc_grr_datalake",
                    output_process=f"fpc_grr_job_datalake"
                )

        logger.log("STOP", None)

    def handle(self, *args, **options):
        # self.run()
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)