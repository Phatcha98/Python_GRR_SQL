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
                        FROM smt.smt_grr_parameter_deg3
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
                            FROM smt_grr_parameter_deg3 AS t1
                            JOIN (
                                SELECT mc_type, mc_code, job_id
                                FROM smt_grr_parameter_deg3
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
                        df1.replace({np.nan: None})

                        row_anova_varcomp_totalgrr = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "varcomp"].iloc[0]
                        row_grr_varcomp_totalgrr = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalgrr_varcomp = row_anova_varcomp_totalgrr
                        else:
                            totalgrr_varcomp = row_grr_varcomp_totalgrr

                        row_anova_varcomp_repe = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "varcomp"].iloc[0]
                        row_grr_varcomp_repe = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repe_varcomp = row_anova_varcomp_repe
                        else:
                            repe_varcomp = row_grr_varcomp_repe

                        row_anova_varcomp_repro = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "varcomp"].iloc[0]
                        row_grr_varcomp_repro = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repro_varcomp = row_anova_varcomp_repro
                        else:
                            repro_varcomp = row_grr_varcomp_repro

                        row_anova_varcomp_operator = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Operator"), "varcomp"].iloc[0]
                        row_grr_varcomp_operator = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Operator"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operator_varcomp = row_anova_varcomp_operator
                        else:
                            operator_varcomp = row_grr_varcomp_operator

                        interac_varcomp = 0

                        row_anova_varcomp_ptp = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "varcomp"].iloc[0]
                        row_grr_varcomp_ptp = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            ptp_varcomp = row_anova_varcomp_ptp
                        else:
                            ptp_varcomp = row_grr_varcomp_ptp

                        row_anova_varcomp_totalvar = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Variation"), "varcomp"].iloc[0]
                        row_grr_varcomp_totalvar = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Variation"), "varcomp"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalvar_varcomp = row_anova_varcomp_totalvar
                        else:
                            totalvar_varcomp = row_grr_varcomp_totalvar

                        estimate_of_vaiance = [totalgrr_varcomp, repe_varcomp, repro_varcomp, operator_varcomp, interac_varcomp, ptp_varcomp, totalvar_varcomp]
                        # print(len(estimate_of_vaiance))
                        # print(estimate_of_vaiance)

                        row_anova_stdev_totalgrr = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "stdev"].iloc[0]
                        row_grr_stdev_totalgrr = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalgrr_stdev = row_anova_stdev_totalgrr
                        else:
                            totalgrr_stdev = row_grr_stdev_totalgrr

                        row_anova_stdev_repe = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "stdev"].iloc[0]
                        row_grr_stdev_repe = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repe_stdev = row_anova_stdev_repe
                        else:
                            repe_stdev = row_grr_stdev_repe

                        row_anova_stdev_repro = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "stdev"].iloc[0]
                        row_grr_stdev_repro = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repro_stdev = row_anova_stdev_repro
                        else:
                            repro_stdev = row_grr_stdev_repro

                        row_anova_stdev_operator = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Operator"), "stdev"].iloc[0]
                        row_grr_stdev_operator = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Operator"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operator_stdev = row_anova_stdev_operator
                        else:
                            operator_stdev = row_grr_stdev_operator

                        interac_stdev = 0

                        row_anova_stdev_ptp = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "stdev"].iloc[0]
                        row_grr_stdev_ptp = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            ptp_stdev = row_anova_stdev_ptp
                        else:
                            ptp_stdev = row_grr_stdev_ptp

                        row_anova_stdev_totalvar = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Variation"), "stdev"].iloc[0]
                        row_grr_stdev_totalvar = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Variation"), "stdev"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalvar_stdev = row_anova_stdev_totalvar
                        else:
                            totalvar_stdev = row_grr_stdev_totalvar

                        stdev = [totalgrr_stdev, repe_stdev, repro_stdev, operator_stdev, interac_stdev, ptp_stdev, totalvar_stdev]
                        # print(len(stdev))
                        # print(stdev)

                        row_anova_contribution_totalgrr = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "contribution"].iloc[0]
                        row_grr_contribution_totalgrr = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalgrr_contribution = row_anova_contribution_totalgrr*100
                        else:
                            totalgrr_contribution = row_grr_contribution_totalgrr*100

                        row_anova_contribution_repe = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "contribution"].iloc[0]
                        row_grr_contribution_repe = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repe_contribution = row_anova_contribution_repe*100
                        else:
                            repe_contribution = row_grr_contribution_repe*100

                        row_anova_contribution_repro = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "contribution"].iloc[0]
                        row_grr_contribution_repro = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repro_contribution = row_anova_contribution_repro*100
                        else:
                            repro_contribution = row_grr_contribution_repro*100

                        row_anova_contribution_operator = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Operator"), "contribution"].iloc[0]
                        row_grr_contribution_operator = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Operator"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operator_contribution = row_anova_contribution_operator*100
                        else:
                            operator_contribution = row_grr_contribution_operator*100

                        interac_contribution = 0

                        row_anova_contribution_ptp = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "contribution"].iloc[0]
                        row_grr_contribution_ptp = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            ptp_contribution = row_anova_contribution_ptp*100
                        else:
                            ptp_contribution = row_grr_contribution_ptp*100

                        row_anova_contribution_totalvar = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Variation"), "contribution"].iloc[0]
                        row_grr_contribution_totalvar = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Variation"), "contribution"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalvar_contribution = row_anova_contribution_totalvar*100
                        else:
                            totalvar_contribution = row_grr_contribution_totalvar*100

                        contribution = [totalgrr_contribution, repe_contribution, repro_contribution, operator_contribution, interac_contribution, ptp_contribution, totalvar_contribution]
                        # print(len(contribution))
                        # print(contribution)

                        row_anova_tv_totalgrr = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "tv"].iloc[0]
                        row_grr_tv_totalgrr = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalgrr_tv = row_anova_tv_totalgrr*100
                        else:
                            totalgrr_tv = row_grr_tv_totalgrr*100

                        row_anova_tv_repe = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "tv"].iloc[0]
                        row_grr_tv_repe = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repe_tv = row_anova_tv_repe*100
                        else:
                            repe_tv = row_grr_tv_repe*100

                        row_anova_tv_repro = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "tv"].iloc[0]
                        row_grr_tv_repro = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repro_tv = row_anova_tv_repro*100
                        else:
                            repro_tv = row_grr_tv_repro*100

                        row_anova_tv_operator = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Operator"), "tv"].iloc[0]
                        row_grr_tv_operator = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Operator"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operator_tv = row_anova_tv_operator*100
                        else:
                            operator_tv = row_grr_tv_operator*100

                        interac_tv = 0

                        row_anova_tv_ptp = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "tv"].iloc[0]
                        row_grr_tv_ptp = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            ptp_tv = row_anova_tv_ptp*100
                        else:
                            ptp_tv = row_grr_tv_ptp*100

                        row_anova_tv_totalvar = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Variation"), "tv"].iloc[0]
                        row_grr_tv_totalvar = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Variation"), "tv"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalvar_tv = row_anova_tv_totalvar*100
                        else:
                            totalvar_tv = row_grr_tv_totalvar*100

                        tv = [totalgrr_tv, repe_tv, repro_tv, operator_tv, interac_tv, ptp_tv, totalvar_tv]
                        # print(len(tv))
                        # print(tv)

                        row_anova_tolerance_totalgrr = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "tolerance"].iloc[0]
                        row_grr_tolerance_totalgrr = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Gauge R&R"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalgrr_tolerance = row_anova_tolerance_totalgrr*100
                        else:
                            totalgrr_tolerance = row_grr_tolerance_totalgrr*100

                        row_anova_tolerance_repe = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Repeatability"), "tolerance"].iloc[0]
                        row_grr_tolerance_repe = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Repeatability"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repe_tolerance = row_anova_tolerance_repe*100
                        else:
                            repe_tolerance = row_grr_tolerance_repe*100

                        row_anova_tolerance_repro = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "tolerance"].iloc[0]
                        row_grr_tolerance_repro = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Reproducibility"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            repro_tolerance = row_anova_tolerance_repro*100
                        else:
                            repro_tolerance = row_grr_tolerance_repro*100

                        row_anova_tolerance_operator = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Operator"), "tolerance"].iloc[0]
                        row_grr_tolerance_operator = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Operator"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            operator_tolerance = row_anova_tolerance_operator*100
                        else:
                            operator_tolerance = row_grr_tolerance_operator*100

                        interac_tolerance = 0

                        row_anova_tolerance_ptp = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "tolerance"].iloc[0]
                        row_grr_tolerance_ptp = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Part - To - Part"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            ptp_tolerance = row_anova_tolerance_ptp*100
                        else:
                            ptp_tolerance = row_grr_tolerance_ptp*100

                        row_anova_tolerance_totalvar = df1.loc[(df1["type"] == "Gauge R&R With Operator*Part Interaction") & (df1["item"] == "Total Variation"), "tolerance"].iloc[0]
                        row_grr_tolerance_totalvar = df1.loc[(df1["type"] == "Gauge R&R Without Operator*Part Interaction") & (df1["item"] == "Total Variation"), "tolerance"].iloc[0]
                        if df1["p_value"].iloc[0] > df1["actual_p_value"].iloc[0]:
                            totalvar_tolerance = row_anova_tolerance_totalvar*100
                        else:
                            totalvar_tolerance = row_grr_tolerance_totalvar*100

                        tolerance = [totalgrr_tolerance, repe_tolerance, repro_tolerance, operator_tolerance, interac_tolerance, ptp_tolerance, totalvar_tolerance]
                        # print(len(tolerance))
                        # print(tolerance)

                        # if totalgrr_stdev == 0:
                        #     availability = float(0) 
                        # else:
                        if totalgrr_stdev == 0:
                                availability = 0  # Set to 0 if division by zero
                        else:
                            availability = 1.41 * round(ptp_stdev,4) / round(totalgrr_stdev,4)

                        availability_rounded_down = math.floor(availability)

                        item = ["Total Gauge R&R", "Repeatability", "Reproducibility", "Operator", "InteractionINT=0", "Part", "Total Variation"]

                        df_out = pd.DataFrame({ 
                                'mc_type': df1['mc_type'].iloc[0], 
                                'mc_code': df1['mc_code'].iloc[0], 
                                'type': "Gauge R&R", 
                                'item': item, 
                                'estimate_of_vaiance': estimate_of_vaiance, 
                                'stdev': stdev, 
                                'contribution': contribution, 
                                'tv': tv,
                                'tolerance': tolerance,
                                'availability_resolution_ndc': availability_rounded_down,
                                'job_id': df1['job_id'].iloc[0],
                                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })

                        df_grr = df_out.applymap(lambda x: round(x, 4) if isinstance(x, (int, float)) else x)
                        try:
                            insert_query = """
                                INSERT INTO smt_grr_parameter_deg5_grr (mc_type, mc_code, "type", item, estimate_of_vaiance, stdev, contribution, tv, tolerance, availability_resolution_ndc, job_id, update_date)
                                VALUES %s
                                ON CONFLICT (mc_type, mc_code, type, item, job_id)
                                DO UPDATE
                                SET estimate_of_vaiance = EXCLUDED.estimate_of_vaiance,
                                    stdev = EXCLUDED.stdev,
                                    contribution = EXCLUDED.contribution, 
                                    tv = EXCLUDED.tv,
                                    tolerance = EXCLUDED.tolerance,
                                    availability_resolution_ndc = EXCLUDED.availability_resolution_ndc,
                                    update_date = EXCLUDED.update_date
                            """
                            df_grr = df_grr.replace({np.nan: None})
                            data_values = [tuple(row) for row in df_grr.to_numpy()]
                            execute_values(cur, insert_query, data_values)
                            conn.commit()
                            cur.execute("UPDATE smt_grr_parameter_deg3 SET is_check = TRUE WHERE job_id = %s", (row[0],))
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

