from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from scipy.stats import f
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import math
import numpy as np
import pandas as pd
import psycopg2
import queue
import schedule
import scipy.stats as stats
import sys

class Command(CustomBaseCommand):

    @logger.catch
    def run(self):

        logger.log("START", None)
        
        try:
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
                with conn.cursor() as cur:
                    
                    job_id_query = (""" 
                        SELECT DISTINCT job_id
                        FROM smt.smt_grr_parameter_deg2
                        WHERE is_check IS NULL OR is_check = false;
                    """)
                    cur.execute(job_id_query)
                    result1 = cur.fetchall()
                    columns1 = [desc[0] for desc in cur.description]
                    df_job = pd.DataFrame(result1, columns=columns1)
                    
                    for index, row in df_job.iterrows():
                        
                        job = row[0]
                        query = ("""
                            SELECT t1.*
                            FROM smt_grr_parameter_deg2 AS t1
                            JOIN (
                                SELECT mc_type, mc_code, job_id
                                FROM smt_grr_parameter_deg2
                                GROUP BY mc_type, mc_code, job_id
                            ) AS t2
                            ON t1.mc_type = t2.mc_type
                            AND t1.mc_code = t2.mc_code
                            AND t1.job_id = t2.job_id
                            WHERE t1.job_id = %s
                        """)
                        cur.execute(query, (job,))
                        result1 = cur.fetchall()
                        columns1 = [desc[0] for desc in cur.description]
                        df1 = pd.DataFrame(result1, columns=columns1)
                        # print(df1)

                        row_anova_df_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Part"), "df"].iloc[0]
                        row_grr_df_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Part"), "df"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            part_df = row_anova_df_part
                        else:
                            part_df = row_grr_df_part

                        row_anova_df_operators = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators"), "df"].iloc[0]
                        row_grr_df_operators = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators"), "df"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operators_df = row_anova_df_operators
                        else:
                            operators_df = row_grr_df_operators

                        row_anova_df_o_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "df"].iloc[0]
                        row_grr_df_o_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "df"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            o_part_df = row_anova_df_o_part
                        else:
                            o_part_df = row_grr_df_o_part

                        row_anova_df_equipment = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "df"].iloc[0]
                        row_grr_df_equipment = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "df"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            equipment_df = row_anova_df_equipment
                        else:
                            equipment_df = row_grr_df_equipment

                        row_anova_df_total = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Total"), "df"].iloc[0]
                        row_grr_df_total = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Total"), "df"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            total_df = row_anova_df_total
                        else:
                            total_df = row_grr_df_total

                        degree_of_freedom = [part_df, operators_df, o_part_df, equipment_df, total_df]
                        # print(degree_of_freedom)
                        
                        row_anova_ss_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Part"), "ss"].iloc[0]
                        row_grr_ss_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Part"), "ss"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            part_ss = row_anova_ss_part
                        else:
                            part_ss = row_grr_ss_part

                        row_anova_ss_operators = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators"), "ss"].iloc[0]
                        row_grr_ss_operators = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators"), "ss"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operators_ss = row_anova_ss_operators
                        else:
                            operators_ss = row_grr_ss_operators

                        row_anova_ss_o_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "ss"].iloc[0]
                        row_grr_ss_o_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "ss"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            o_part_ss = row_anova_ss_o_part
                        else:
                            o_part_ss = row_grr_ss_o_part

                        row_anova_ss_equipment = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "ss"].iloc[0]
                        row_grr_ss_equipment = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "ss"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            equipment_ss = row_anova_ss_equipment
                        else:
                            equipment_ss = row_grr_ss_equipment

                        row_anova_ss_total = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Total"), "ss"].iloc[0]
                        row_grr_ss_total = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Total"), "ss"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            total_ss = row_anova_ss_total
                        else:
                            total_ss = row_grr_ss_total

                        sum_of_squares = [part_ss, operators_ss, o_part_ss, equipment_ss, total_ss]
                        # print(sum_of_squares)

                        row_anova_ms_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Part"), "ms"].iloc[0]
                        row_grr_ms_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Part"), "ms"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            part_ms = row_anova_ms_part
                        else:
                            part_ms = row_grr_ms_part

                        row_anova_ms_operators = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators"), "ms"].iloc[0]
                        row_grr_ms_operators = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators"), "ms"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operators_ms = row_anova_ms_operators
                        else:
                            operators_ms = row_grr_ms_operators

                        row_anova_ms_o_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "ms"].iloc[0]
                        row_grr_ms_o_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "ms"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            o_part_ms = row_anova_ms_o_part
                        else:
                            o_part_ms = row_grr_ms_o_part

                        row_anova_ms_equipment = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "ms"].iloc[0]
                        row_grr_ms_equipment = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "ms"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            equipment_ms = row_anova_ms_equipment
                        else:
                            equipment_ms = row_grr_ms_equipment

                        row_anova_ms_total = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Total"), "ms"].iloc[0]
                        row_grr_ms_total = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Total"), "ms"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            total_ms = row_anova_ms_total
                        else:
                            total_ms = row_grr_ms_total

                        average_square = [part_ms, operators_ms, o_part_ms, equipment_ms, total_ms]
                        # print(average_square)

                        row_anova_f_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Part"), "f"].iloc[0]
                        row_grr_f_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Part"), "f"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            part_f = row_anova_f_part
                        else:
                            part_f = row_grr_f_part

                        row_anova_f_operators = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators"), "f"].iloc[0]
                        row_grr_f_operators = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators"), "f"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operators_f = row_anova_f_operators
                        else:
                            operators_f = row_grr_f_operators

                        row_anova_f_o_part = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "f"].iloc[0]
                        row_grr_f_o_part = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Operators*Part"), "f"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            o_part_f = row_anova_f_o_part
                        else:
                            o_part_f = row_grr_f_o_part

                        row_anova_f_equipment = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "f"].iloc[0]
                        row_grr_f_equipment = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "f"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            equipment_f = row_anova_f_equipment
                        else:
                            equipment_f = row_grr_f_equipment

                        row_anova_f_total = df1.loc[(df1["type"] == "ANOVA Table With Operator*Part Interaction") & (df1["item"] == "Total"), "f"].iloc[0]
                        row_grr_f_total = df1.loc[(df1["type"] == "ANOVA Table Without Operator*Part Interaction") & (df1["item"] == "Total"), "f"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            total_f = row_anova_f_total
                        else:
                            total_f = row_grr_f_total

                        f_ratio = [part_f, operators_f, o_part_f, equipment_f, total_f]
                        item = ["Part", "Operators", "Operators*Part", "Equipment", "Total"]
                        # print(f_ratio)

                        df_out1 = pd.DataFrame({ 
                                'mc_type': df1['mc_type'].iloc[0], 
                                'mc_code': df1['mc_code'].iloc[0], 
                                'type': "ANOVA Table", 
                                'item': item, 
                                'degree_of_freedom': degree_of_freedom, 
                                'sum_of_squares': sum_of_squares, 
                                'average_square': average_square, 
                                'f_ratio': f_ratio, 
                                'job_id': df1['job_id'].iloc[0],
                                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })

                        df_anova = df_out1.applymap(lambda x: round(x, 4) if isinstance(x, (int, float)) else x)
                        
                        try:
                            
                            insert_query = """
                                INSERT INTO smt_grr_parameter_deg4_anova (mc_type, mc_code, "type", item, degree_of_freedom, sum_of_squares, average_square, f_ratio, job_id, update_date)
                                VALUES %s
                                ON CONFLICT (mc_type, mc_code, type, item, job_id)
                                DO UPDATE
                                SET degree_of_freedom = EXCLUDED.degree_of_freedom,
                                    sum_of_squares = EXCLUDED.sum_of_squares,
                                    average_square = EXCLUDED.average_square, 
                                    f_ratio = EXCLUDED.f_ratio,
                                    update_date = EXCLUDED.update_date
                            """
                            # df_anova.replace({"None": None, "NaN": None, "nan": None, " ": None}, inplace=True)
                            df_anova = df_anova.replace({np.nan: None})
                            data_values = [tuple(row) for row in df_anova.to_numpy()]
                            execute_values(cur, insert_query, data_values)
                            conn.commit()
                            cur.execute("UPDATE smt_grr_parameter_deg2 SET is_check = TRUE WHERE job_id = %s", (row[0],))
                            pass
                        
                        except Exception as ex:
                            logger.exception(ex)

        except Exception as ex:
            logger.exception(ex)     
        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):      
        # self.run()     
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)
