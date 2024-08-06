from datetime import datetime
from decimal import Decimal
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from scipy.stats import f
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import cmath
import math
import numpy as np
import pandas as pd
import psycopg2
import queue
import schedule
import scipy.stats as stats
import sys

class Command(CustomBaseCommand):
    
    def fomula_contribution_tv(self, input_1, inpui_2):
        try:
            result = (input_1 / inpui_2)
            return result
        except:
            return 0
    
    @logger.catch
    def run(self):

        logger.log("START", None)
        
        try:
            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db["OPTIONS"]["options"]) as conn:
                with conn.cursor() as cur:
                    
                    job_id_query = (""" 
                        SELECT job_id, COUNT(job_id) as job_count
                        FROM smt_grr_parameter_deg1
                        WHERE is_check = false OR is_check IS NULL
                        GROUP BY job_id
                        HAVING COUNT(job_id) = 30;
                    """)
                    cur.execute(job_id_query)
                    result1 = cur.fetchall()
                    columns1 = [desc[0] for desc in cur.description]
                    df_job = pd.DataFrame(result1, columns=columns1)
                    
                    for index, row in df_job.iterrows():
                        job = row[0]
                        query1 = ("""
                            WITH x_bar_cte AS (
                                SELECT
                                    sn,
                                    x_bar_p AS for_x_bar_bar,
                                    job_id
                                FROM
                                    smt.smt_grr_parameter_deg1
                                where job_id = %s
                            )
                            , x_bar_avg_cte AS (
                                select
                                    sn, 
                                    AVG(for_x_bar_bar) over () as x_bar_bar,
                                    MAX(for_x_bar_bar) over () as x_bar_max,
                                    MIN(for_x_bar_bar) over () as x_bar_min
                                FROM
                                    x_bar_cte  
                            ),
                            r_bar_cte AS (
                                SELECT
                                    sn,
                                    SUM(r_bar) / 3 AS r_bar_bar
                                FROM
                                    smt.smt_grr_parameter_deg1
                                where job_id = %s
                                GROUP BY
                                    sn
                            ),
                            x_diff_cte AS (
                                SELECT
                                    sn,
                                    MAX(x_bar) - MIN(x_bar) AS x_diff
                                FROM
                                    smt.smt_grr_parameter_deg1
                                where job_id = %s
                                GROUP BY
                                    sn
                            )
                            SELECT
                                deg1.mc_type,
                                deg1.mc_code,
                                r.sn AS sn,
                                x_bar_avg_cte.x_bar_bar,
                                x_bar_avg_cte.x_bar_max - x_bar_avg_cte.x_bar_min AS rp,
                                0 AS d3,
                                2.575 AS d4,
                                1.023 AS a2,
                                3 AS appraisers,
                                3 AS trials,
                                10 AS samples,
                                r.r_bar_bar,
                                xd.x_diff AS x_diff,
                                x_bar_avg_cte.x_bar_bar + 1.023 * r.r_bar_bar AS uclx,
                                x_bar_avg_cte.x_bar_bar - 1.023 * r.r_bar_bar AS lclx,
                                2.575 * r.r_bar_bar AS uclr,
                                0 AS lclr,
                                deg1.job_id
                            FROM
                                x_bar_avg_cte
                            JOIN
                                r_bar_cte r ON x_bar_avg_cte.sn = r.sn
                            JOIN
                                x_diff_cte xd ON r.sn = xd.sn
                            JOIN
                                smt.smt_grr_parameter_deg1 deg1 ON deg1.sn = r.sn
                            WHERE  
                                deg1.job_id = %s
                            GROUP BY
                                deg1.mc_type, deg1.mc_code, r.sn, x_bar_avg_cte.x_bar_bar, r.r_bar_bar, x_bar_avg_cte.x_bar_max, x_bar_avg_cte.x_bar_min, xd.x_diff, deg1.job_id;
                            """)
                        cur.execute(query1, (job, job, job, job))
                        result1 = cur.fetchall()
                        
                        columns1 = [desc[0] for desc in cur.description]
                        df1 = pd.DataFrame(result1, columns=columns1)
                        # df1.replace({np.nan: None, 'nan': None}, inplace=True)
                        df1.drop_duplicates(subset=['mc_type','mc_code','sn'], inplace=True)
                        # print(df1)
                        
                        try:
                            query2 = """
                                SELECT
                                    mc_type,
                                    mc_code,
                                    op_id,
                                    total_horizontal,
                                    sum_sq_all,
                                    lsl,
                                    usl,
                                    sum_r,
                                    sum_total_vertical  
                                FROM (
                                    SELECT
                                        mc_type,
                                        mc_code,
                                        op_id,
                                        SUM("sum") OVER(PARTITION BY op_id, job_id) AS total_horizontal, 
                                        sum_sq_all,
                                        lsl,
                                        usl,
                                        SUM("sum"*"sum") OVER(PARTITION BY job_id) AS sum_r,
                                        SUM(total_vertical * total_vertical) OVER(PARTITION BY mc_type, mc_code, op_id, job_id) AS sum_total_vertical,
                                        job_id
                                    FROM
                                        smt.smt_grr_parameter_deg1
                                where job_id = %s
                                ) AS subquery
                                WHERE  
                                    job_id = %s
                                GROUP BY
                                    mc_type, mc_code, op_id, total_horizontal, sum_sq_all, lsl, usl, sum_r, sum_total_vertical;
                            """
                            cur.execute(query2, (job, job))
                            result2 = cur.fetchall()
                            columns2 = [desc[0] for desc in cur.description]
                            df2 = pd.DataFrame(result2, columns=columns2)
                            
                            # df2.replace({np.nan: None, 'nan': None}, inplace=True)
                            df2.drop_duplicates(subset=['mc_type','mc_code','op_id'], inplace=True)

                            nkr = (sum(df2['total_horizontal']) ** 2) / (df1['appraisers'] * df1['trials'] * df1['samples'])
                            nkr = nkr.iloc[0]
                            nr = sum(df2['total_horizontal'] ** 2) / df1['trials'] / df1['samples']
                            nr = nr.iloc[0]
                            kr = df2['sum_total_vertical'] / df1['appraisers'] / df1['trials']
                            kr = kr.iloc[0]
                            r = df2['sum_r'] / df1['trials']
                            r = r.iloc[0]

                            type = ['ANOVA Table With Operator*Part Interaction','ANOVA Table With Operator*Part Interaction', 'ANOVA Table With Operator*Part Interaction',
                            'ANOVA Table With Operator*Part Interaction','ANOVA Table With Operator*Part Interaction','ANOVA Table Without Operator*Part Interaction',
                            'ANOVA Table Without Operator*Part Interaction','ANOVA Table Without Operator*Part Interaction','ANOVA Table Without Operator*Part Interaction',
                            'ANOVA Table Without Operator*Part Interaction']

                            item = ['Part', 'Operators', 'Operators*Part', 'Repeatability', 'Total', 'Part', 'Operators', 'Operators*Part','Repeatability', 'Total']

                            wi_df_part = df1['samples'].iloc[0]-1
                            wi_df_ooperators = df1['appraisers'].iloc[0]-1
                            wi_df_operators_part = wi_df_part * wi_df_ooperators
                            wi_df_repeatability =  df1['appraisers'].iloc[0] * df1['samples'].iloc[0] * (df1['trials'].iloc[0]-1)
                            wi_df_total = df1['appraisers'].iloc[0] * df1['trials'].iloc[0] * df1['samples'].iloc[0]-1
                            wio_df_part =df1['samples'].iloc[0]-1
                            wio_df_ooperators = df1['appraisers'].iloc[0]-1
                            wio_df_operators_part = None
                            wio_df_repeatability = df1['appraisers'].iloc[0] * df1['samples'].iloc[0] * (df1['trials'].iloc[0]-1) + (df1['samples'].iloc[0]-1) * (df1['appraisers'].iloc[0]-1)
                            wio_df_total = df1['appraisers'].iloc[0] * df1['trials'].iloc[0] * df1['samples'].iloc[0]-1
                            df_anova1 = [wi_df_part, wi_df_ooperators, wi_df_operators_part, wi_df_repeatability, wi_df_total, wio_df_part, wio_df_ooperators, wio_df_operators_part, wio_df_repeatability, wio_df_total]

                            wi_ss_part = kr - nkr
                            wi_ss_ooperators = nr - nkr
                            wi_ss_operators_part = r - kr - nr + nkr
                            wi_ss_repeatability = df2['sum_sq_all'].iloc[0] - r
                            wi_ss_total = df2['sum_sq_all'].iloc[0] - nkr
                            wio_ss_part = kr - nkr
                            wio_ss_ooperators = nr - nkr
                            wio_ss_operators_part = None
                            wio_ss_repeatability = df2['sum_sq_all'].iloc[0] - kr - nr + nkr
                            wio_ss_total = df2['sum_sq_all'].iloc[0] - nkr
                            ss = [wi_ss_part, wi_ss_ooperators, wi_ss_operators_part, wi_ss_repeatability, wi_ss_total, wio_ss_part, wio_ss_ooperators, wio_ss_operators_part, wio_ss_repeatability, wio_ss_total]

                            wi_ms_part = self.fomula_contribution_tv(wi_ss_part, wi_df_part)
                            wi_ms_ooperators = self.fomula_contribution_tv(wi_ss_ooperators, wio_df_ooperators)
                            wi_ms_operators_part = self.fomula_contribution_tv(wi_ss_operators_part, wi_df_operators_part)
                            wi_ms_repeatability = self.fomula_contribution_tv(wi_ss_repeatability, wi_df_repeatability)
                            # wi_ms_total = self.fomula_contribution_tv(wi_ss_total, wi_df_total)
                            wi_ms_total = None
                            wio_ms_part = self.fomula_contribution_tv(wio_ss_part, wio_df_part)
                            wio_ms_operators = self.fomula_contribution_tv(wio_ss_ooperators, wio_df_ooperators)
                            wio_ms_operators_part = None
                            wio_ms_repeatability =  self.fomula_contribution_tv(wio_ss_repeatability, wio_df_repeatability)
                            # wio_ms_total = self.fomula_contribution_tv(wio_ss_total, wio_df_total)
                            wio_ms_total = None
                            ms = [wi_ms_part, wi_ms_ooperators, wi_ms_operators_part, wi_ms_repeatability, wi_ms_total, wio_ms_part, wio_ms_operators, wio_ms_operators_part, wio_ms_repeatability, wio_ms_total]
                        
                            wi_f_part = self.fomula_contribution_tv(wi_ms_part, wi_ms_operators_part)
                            wi_f_ooperators = self.fomula_contribution_tv(wi_ms_ooperators, wi_ms_operators_part)
                            wi_f_operators_part = self.fomula_contribution_tv(wi_ms_operators_part, wi_ms_repeatability)
                            wi_f_repeatability = None
                            wi_f_total = None
                            wio_f_part = self.fomula_contribution_tv(wio_ms_part, wio_ms_repeatability)
                            wio_f_ooperators = self.fomula_contribution_tv(wio_ms_operators, wio_ms_repeatability)
                            wio_f_operators_part = None
                            wio_f_repeatability = None
                            wio_f_total = None
                            f = [wi_f_part, wi_f_ooperators, wi_f_operators_part, wi_f_repeatability, wi_f_total, wio_f_part, wio_f_ooperators, wio_f_operators_part,  wio_f_repeatability, wio_f_total]

                            wi_p_part = float(stats.f.sf(wi_f_part, wi_df_part, wi_df_operators_part))
                            wi_p_operators = float(stats.f.sf(wi_f_ooperators, wi_df_ooperators, wi_df_operators_part))
                            wi_p_operators_part = float(stats.f.sf(wi_f_operators_part, wi_df_operators_part, wi_df_repeatability))
                            wi_p_repeatability = None
                            wi_p_total = None
                            wio_p_part = float(stats.f.sf(wio_f_part, wio_df_part, wio_df_repeatability))
                            wio_p_operators = float(stats.f.sf(wio_f_ooperators, wio_df_ooperators, wio_df_repeatability))
                            wio_p_operators_part = None
                            wio_p_repeatability = None
                            wio_p_total = None

                            wi_p_part_scientific = round(wi_p_part, 5)
                            wi_p_ooperators_scientific = round(wi_p_operators, 5)
                            wi_p_operators_part_scientific = round(wi_p_operators_part, 5)
                            wi_p_repeatability_scientific = 'None' if wi_p_repeatability is None or " "  else "{:0.50f}".format(wi_p_repeatability)
                            wi_p_total_scientific = 'None' if wi_p_total is None or " " else "{:0.5f}".format(wi_p_total)
                            wio_p_part_scientific = round(wio_p_part, 5)
                            wio_p_ooperators_scientific = round(wio_p_operators, 5)
                            wio_p_ptp_scientific = 'None' if wio_p_operators_part is None or " " else "{:0.50f}".format(wio_p_operators_part)
                            wio_p_repeatability_scientific = 'None' if wio_p_repeatability is None or " " else "{:0.50f}".format(wio_p_repeatability)
                            wio_p_total_scientific = 'None' if wio_p_total is None or " " else "{:0.5f}".format(wio_p_total)

                            p = [wi_p_part_scientific, wi_p_ooperators_scientific, wi_p_operators_part_scientific, wi_p_repeatability_scientific, wi_p_total_scientific,
                                wio_p_part_scientific, wio_p_ooperators_scientific, wio_p_ptp_scientific, wio_p_repeatability_scientific, wio_p_total_scientific]
                             
                            df_anova = pd.DataFrame({ 
                                'mc_type': df1['mc_type'].iloc[0], 
                                'mc_code': df1['mc_code'].iloc[0], 
                                'type': type, 
                                'item': item, 
                                'df': df_anova1, 
                                'ss': ss, 
                                'ms': ms, 
                                'f': f, 
                                'p': p, 
                                'p_value': 0.25,
                                'actual_p_value': wi_p_operators_part_scientific,
                                'job_id': df1['job_id'].iloc[0],
                                'r_bar_bar': df1['r_bar_bar'].iloc[0],
                                'x_diff': df1['x_diff'].iloc[0],
                                'uclx': df1['uclx'].iloc[0],
                                'lclx': df1['lclx'].iloc[0],
                                'uclr': df1['uclr'].iloc[0],
                                'lclr': df1['lclr'].iloc[0],
                                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })
                            df_out1 = df_anova.replace({"None": None, "NaN": None, "nan": None, " ": None})
                            df_out1 = df_out1.applymap(lambda x: round(x, 9) if isinstance(x, (int, float)) else x)
                            
                            try:
                                insert_query = """
                                    INSERT INTO smt_grr_parameter_deg2 (mc_type, mc_code, "type", item, df, ss, ms, f, p, p_value, actual_p_value, job_id, r_bar_bar, x_diff, uclx, lclx, uclr, lclr, update_date)
                                    VALUES %s
                                    ON CONFLICT (mc_type, mc_code, type, item, job_id)
                                    DO UPDATE
                                    SET df = EXCLUDED.df,
                                        ss = EXCLUDED.ss,
                                        ms = EXCLUDED.ms, 
                                        f = EXCLUDED.f,
                                        p = EXCLUDED.p,
                                        p_value = EXCLUDED.p_value, 
                                        actual_p_value = EXCLUDED.actual_p_value,
                                        r_bar_bar = EXCLUDED.r_bar_bar,
                                        x_diff = EXCLUDED.x_diff, 
                                        uclx = EXCLUDED.uclx,
                                        lclx = EXCLUDED.lclx,
                                        uclr = EXCLUDED.uclr, 
                                        lclr = EXCLUDED.lclr,
                                        update_date = EXCLUDED.update_date
                                """
                                df_out1 = df_out1.replace({np.nan: None})
                                data_values = [tuple(row) for row in df_out1.to_numpy()]
                                execute_values(cur, insert_query, data_values)
                                conn.commit()

                            except Exception as ex:
                                logger.exception(ex)

                            grr_type = ['Gauge R&R With Operator*Part Interaction', 'Gauge R&R With Operator*Part Interaction', 'Gauge R&R With Operator*Part Interaction', 'Gauge R&R With Operator*Part Interaction',
                                        'Gauge R&R With Operator*Part Interaction', 'Gauge R&R With Operator*Part Interaction', 'Gauge R&R With Operator*Part Interaction', 'Gauge R&R Without Operator*Part Interaction',
                                        'Gauge R&R Without Operator*Part Interaction', 'Gauge R&R Without Operator*Part Interaction', 'Gauge R&R Without Operator*Part Interaction', 'Gauge R&R Without Operator*Part Interaction',
                                        'Gauge R&R Without Operator*Part Interaction', 'Gauge R&R Without Operator*Part Interaction']
                            grr_item = ['Total Gauge R&R', 'Repeatability', 'Reproducibility', 'Operator', 'Operators*Part', 'Part - To - Part', 'Total Variation', 'Total Gauge R&R', 'Repeatability', 'Reproducibility', 'Operator',
                                        'Operators*Part', 'Part - To - Part', 'Total Variation']

                            grr_var_wi_repeatability = wi_ms_repeatability
                            grr_var_wi_operator = (wi_ms_ooperators-wi_ms_operators_part) / (df1['samples'].iloc[0] * df1['trials'].iloc[0]) 
                            grr_var_wi_operators_part = (wi_ms_operators_part - wi_ms_repeatability)/df1['trials'].iloc[0]
                            grr_var_wi_part_to_part = (wi_ms_part - wi_ms_operators_part) / (df1['appraisers'].iloc[0] * df1['trials'].iloc[0])
                            grr_var_wi_reproducibility = (grr_var_wi_operator + grr_var_wi_operators_part)
                            grr_var_wi_total = (grr_var_wi_repeatability + grr_var_wi_reproducibility)
                            grr_var_wi_total_variation = (grr_var_wi_total + grr_var_wi_part_to_part)
                            grr_var_wio_repeatability = (wio_ss_repeatability/ wio_df_repeatability)
                            grr_var_wio_operator = (wio_ms_operators - wio_ms_repeatability) / (df1['samples'].iloc[0] * df1['trials'].iloc[0]) 
                            grr_var_wio_operators_part = " "
                            grr_var_wio_part_to_part = (wio_ms_part - wio_ms_repeatability) /(df1['appraisers'].iloc[0] * df1['trials'].iloc[0])
                            grr_var_wio_reproducibility = grr_var_wio_operator
                            grr_var_wio_total = grr_var_wio_repeatability + grr_var_wio_reproducibility
                            grr_var_wio_total_variation = grr_var_wio_total + grr_var_wio_part_to_part
                            varcomp = [grr_var_wi_total, grr_var_wi_repeatability,grr_var_wi_reproducibility, grr_var_wi_operator, grr_var_wi_operators_part, grr_var_wi_part_to_part, grr_var_wi_total_variation,
                                    grr_var_wio_total, grr_var_wio_repeatability, grr_var_wio_reproducibility, grr_var_wio_operator, grr_var_wio_operators_part, grr_var_wio_part_to_part, grr_var_wio_total_variation]

                            grr_stdev_wi_repeatability = round(cmath.sqrt(grr_var_wi_repeatability).real, 9)
                            grr_stdev_wi_operators_part = round(cmath.sqrt(grr_var_wi_operators_part).real, 9)
                            grr_stdev_wi_part_to_part = round(cmath.sqrt(grr_var_wi_part_to_part).real, 9)
                            grr_stdev_wi_reproducibility = round(cmath.sqrt(grr_var_wi_reproducibility).real, 9)
                            grr_stdev_wi_operator = round(cmath.sqrt(grr_var_wi_operator).real, 9)
                            grr_stdev_wi_total = round(cmath.sqrt(grr_var_wi_total).real, 9)
                            grr_stdev_wi_total_variation = round(cmath.sqrt(grr_var_wi_total_variation).real, 9)
                            grr_stdev_wio_repeatability = round(cmath.sqrt(grr_var_wio_repeatability).real, 9)
                            grr_stdev_wio_operators_part = " "
                            grr_stdev_wio_part_to_part = round(cmath.sqrt(grr_var_wio_part_to_part).real, 9)
                            grr_stdev_wio_reproducibility = round(cmath.sqrt(grr_var_wio_reproducibility).real, 9)
                            grr_stdev_wio_operator = round(cmath.sqrt(grr_var_wio_operator).real, 9)
                            grr_stdev_wio_total = round(cmath.sqrt(grr_var_wio_total).real, 9)
                            grr_stdev_wio_total_variation  = round(cmath.sqrt(grr_var_wio_total_variation).real, 9)
                            stdev = [grr_stdev_wi_total, grr_stdev_wi_repeatability,grr_stdev_wi_reproducibility, grr_stdev_wi_operator, grr_stdev_wi_operators_part, grr_stdev_wi_part_to_part, grr_stdev_wi_total_variation,
                                    grr_stdev_wio_total, grr_stdev_wio_repeatability, grr_stdev_wio_reproducibility, grr_stdev_wio_operator, grr_stdev_wio_operators_part, grr_stdev_wio_part_to_part, grr_stdev_wio_total_variation]

                            grr_con_wi_repeatability = self.fomula_contribution_tv(grr_var_wi_repeatability, grr_var_wi_total_variation)
                            grr_con_wi_operators_part = self.fomula_contribution_tv(grr_var_wi_operators_part, grr_var_wi_total_variation)
                            grr_con_wi_part_to_part = self.fomula_contribution_tv(grr_var_wi_part_to_part, grr_var_wi_total_variation)
                            grr_con_wi_reproducibility = self.fomula_contribution_tv(grr_var_wi_reproducibility, grr_var_wi_total_variation)
                            grr_con_wi_operator = self.fomula_contribution_tv(grr_var_wi_operator, grr_var_wi_total_variation)
                            grr_con_wi_total = self.fomula_contribution_tv(grr_var_wi_total, grr_var_wi_total_variation)
                            grr_con_wi_total_variation = self.fomula_contribution_tv(grr_var_wi_total_variation, grr_var_wi_total_variation)
                            grr_con_wio_repeatability = self.fomula_contribution_tv(grr_var_wio_repeatability, grr_stdev_wio_total_variation)
                            grr_con_wio_operators_part = " "
                            grr_con_wio_part_to_part = self.fomula_contribution_tv(grr_var_wio_part_to_part, grr_stdev_wio_total_variation)
                            grr_con_wio_reproducibility = self.fomula_contribution_tv(grr_var_wio_reproducibility, grr_stdev_wio_total_variation)
                            grr_con_wio_operator = self.fomula_contribution_tv(grr_var_wio_operator, grr_stdev_wio_total_variation)
                            grr_con_wio_total = self.fomula_contribution_tv(grr_var_wio_total, grr_stdev_wio_total_variation)
                            grr_con_wio_total_variation  = self.fomula_contribution_tv(grr_stdev_wio_total_variation, grr_stdev_wio_total_variation)
                            contribution = [grr_con_wi_total, grr_con_wi_repeatability,grr_con_wi_reproducibility, grr_con_wi_operator, grr_con_wi_operators_part, grr_con_wi_part_to_part, grr_con_wi_total_variation,
                                    grr_con_wio_total, grr_con_wio_repeatability, grr_con_wio_reproducibility, grr_con_wio_operator, grr_con_wio_operators_part, grr_con_wio_part_to_part, grr_con_wio_total_variation]

                            grr_tv_wi_repeatability = self.fomula_contribution_tv(grr_stdev_wi_repeatability, grr_stdev_wi_total_variation)
                            grr_tv_wi_operators_part = self.fomula_contribution_tv(grr_stdev_wi_operators_part, grr_stdev_wi_total_variation)
                            grr_tv_wi_part_to_part = self.fomula_contribution_tv(grr_stdev_wi_part_to_part, grr_stdev_wi_total_variation)
                            grr_tv_wi_reproducibility = self.fomula_contribution_tv(grr_stdev_wi_reproducibility, grr_stdev_wi_total_variation)
                            grr_tv_wi_operator = self.fomula_contribution_tv(grr_stdev_wi_operator, grr_stdev_wi_total_variation)
                            grr_tv_wi_total = self.fomula_contribution_tv(grr_stdev_wi_total, grr_stdev_wi_total_variation)
                            grr_tv_wi_total_variation = self.fomula_contribution_tv(grr_stdev_wi_total_variation, grr_stdev_wi_total_variation)

                            grr_tv_wio_repeatability = self.fomula_contribution_tv(grr_stdev_wio_repeatability, grr_stdev_wio_total_variation)
                            grr_tv_wio_operators_part = " "
                            grr_tv_wio_part_to_part = self.fomula_contribution_tv(grr_stdev_wio_part_to_part, grr_stdev_wio_total_variation)
                            grr_tv_wio_reproducibility = self.fomula_contribution_tv(grr_stdev_wio_reproducibility, grr_stdev_wio_total_variation)
                            grr_tv_wio_operator = self.fomula_contribution_tv(grr_stdev_wio_operator, grr_stdev_wio_total_variation)
                            grr_tv_wio_total = self.fomula_contribution_tv(grr_stdev_wio_total, grr_stdev_wio_total_variation)
                            grr_tv_wio_total_variation  = self.fomula_contribution_tv(grr_stdev_wio_total_variation, grr_stdev_wio_total_variation)
                            tv = [grr_tv_wi_total, grr_tv_wi_repeatability,grr_tv_wi_reproducibility, grr_tv_wi_operator, grr_tv_wi_operators_part, grr_tv_wi_part_to_part, grr_tv_wi_total_variation,
                                    grr_tv_wio_total, grr_tv_wio_repeatability, grr_tv_wio_reproducibility, grr_tv_wio_operator, grr_tv_wio_operators_part, grr_tv_wio_part_to_part, grr_tv_wio_total_variation]

                            grr_tol_wi_repeatability = 6*(grr_stdev_wi_repeatability / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_operators_part = 6*(grr_stdev_wi_operators_part / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_part_to_part = 6*(grr_stdev_wi_part_to_part / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_reproducibility = 6*(grr_stdev_wi_reproducibility / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_operator = 6*(grr_stdev_wi_operator / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_total = 6*(grr_stdev_wi_total / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wi_total_variation = 6*(grr_stdev_wi_total_variation / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_repeatability = 6*(grr_stdev_wio_repeatability / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_operators_part = " "
                            grr_tol_wio_part_to_part = 6*(grr_stdev_wio_part_to_part / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_reproducibility = 6*(grr_stdev_wio_reproducibility / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_operator = 6*(grr_stdev_wio_operator / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_total = 6*(grr_stdev_wio_total / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            grr_tol_wio_total_variation  = 6*(grr_stdev_wio_total_variation / (df2['usl'].iloc[0] - df2['lsl'].iloc[0]))
                            tolerance = [grr_tol_wi_total, grr_tol_wi_repeatability,grr_tol_wi_reproducibility, grr_tol_wi_operator, grr_tol_wi_operators_part, grr_tol_wi_part_to_part, grr_tol_wi_total_variation,
                                    grr_tol_wio_total, grr_tol_wio_repeatability, grr_tol_wio_reproducibility, grr_tol_wio_operator, grr_tol_wio_operators_part, grr_tol_wio_part_to_part, grr_tol_wio_total_variation]

                            # stdev_rounded = [round(t, 4) for t in stdev]
                            df_grr = pd.DataFrame({ 
                                'mc_type': df1['mc_type'].iloc[0], 
                                'mc_code': df1['mc_code'].iloc[0], 
                                'type': grr_type, 
                                'item': grr_item, 
                                'varcomp': varcomp, 
                                'stdev': stdev, 
                                'contribution': contribution, 
                                'tv': tv, 
                                'tolerance': tolerance, 
                                'p_value': 0.25,
                                'actual_p_value': wi_p_operators_part_scientific,
                                'job_id': df1['job_id'].iloc[0],
                                'update_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })                           
                            df_out2 = df_grr.replace({"None": None, "NaN": None, "nan": None, " ": None})
                            df_out2 = df_out2.applymap(lambda x: round(x, 9) if isinstance(x, (int, float)) else x)
                            try:
                                insert_query = """
                                    INSERT INTO smt_grr_parameter_deg3 (mc_type, mc_code, type, item, varcomp, stdev, contribution, tv, tolerance, p_value, actual_p_value, job_id, update_date)
                                    VALUES %s
                                    ON CONFLICT (mc_type, mc_code, type, item, job_id)
                                    DO UPDATE
                                    SET varcomp = EXCLUDED.varcomp,
                                        contribution = EXCLUDED.contribution,
                                        tv = EXCLUDED.tv, 
                                        tolerance = EXCLUDED.tolerance,
                                        p_value = EXCLUDED.p_value,
                                        actual_p_value = EXCLUDED.actual_p_value, 
                                        update_date = EXCLUDED.update_date
                                """
                                df_out2.replace({'None': None, 'NaN': None, "nan": None, " ": None}, inplace=True)
                                df_out2 = df_out2.replace({np.nan: None})
                                data_values = [tuple(row) for row in df_out2.to_numpy()]
                                execute_values(cur, insert_query, data_values)
                                conn.commit()
                                cur.execute("UPDATE smt_grr_parameter_deg1 SET is_check = TRUE WHERE job_id = %s", (row[0],))
                                pass
                            except Exception as ex:
                                logger.exception(ex)
                                
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