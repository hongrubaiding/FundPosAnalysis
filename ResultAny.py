# -*-coding:utf-8-*-
# Author:ZouHao
# email:1084848158@qq.com
import numpy as np

import mylog as mylog_demo
import os
from GetDataPack.GetFundData import GetFundData
import pandas as pd
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False
import matplotlib.pyplot as plt


class ResultAny:
    def __init__(self):
        self.logger = mylog_demo.set_log('result_analy_logfile')
        self.get_fund_data_demo = GetFundData(self.logger)

    def calc_error_rate(self, calc_poc_result_df, true_poc_result_df):
        dic_result = {}
        dic_true = {}
        for first_industry_code, temp_df in true_poc_result_df.groupby("FirstIndustryCode"):
            dic_true[first_industry_code] = {}
            dic_true[first_industry_code]["FirstIndustryCode"] = first_industry_code
            dic_true[first_industry_code]["FirstIndustryName"] = temp_df["FirstIndustryName"].unique()[0]
            dic_true[first_industry_code]["TrueWeight"] = temp_df["RatioInNV"].sum()
            dic_true[first_industry_code]["Standard"] = temp_df["Standard"].unique()[0]

        true_poc_industry_df = pd.DataFrame(dic_true).T
        true_poc_lack_code = list(set(calc_poc_result_df["Classify_Industry_Code"].tolist()).difference(
            set(true_poc_industry_df["FirstIndustryCode"].tolist())))
        lack_se = pd.Series([0.] * len(true_poc_lack_code), index=true_poc_lack_code)
        true_poc_se = true_poc_industry_df["TrueWeight"].copy().append(lack_se)
        true_poc_se.name = "TrueWeight"

        calc_poc_se = calc_poc_result_df["Weight"]
        calc_poc_se.name = "CalcWeight"
        total_weight_df = pd.concat([true_poc_se, calc_poc_se], axis=1, sort=True)
        total_weight_df = total_weight_df.apply(lambda x: x.astype(float))
        total_weight_df["Classify_FirstIndustryName"] = calc_poc_result_df.loc[total_weight_df.index.tolist()][
            "Classify_FirstIndustryName"]

        error_abs_se = np.abs(total_weight_df["CalcWeight"] - total_weight_df["TrueWeight"])
        error_square_se = np.square(total_weight_df["CalcWeight"] - total_weight_df["TrueWeight"])
        dic_result["corr_coefficient"] = total_weight_df[["CalcWeight", "TrueWeight"]].corr().iloc[0][1]
        dic_result["error_abs_se"] = error_abs_se
        dic_result["error_square_se"] = error_square_se
        dic_result["total_weight"] = total_weight_df
        return dic_result

    def get_calc_result(self, code):
        dic_total_result = {}
        file_path = os.getcwd() + r"\\CalcResult\\"
        total_file_path = file_path + "%s\\" % code
        total_file = os.listdir(total_file_path)
        total_file_list = [file_name for file_name in total_file if file_name.find('计算结果.xlsx') != -1]
        df_fund_detail_poc_info = self.get_fund_data_demo.get_fund_total_poc_info(secu_code=code)
        for file_name in total_file_list:
            report_date = file_name[:10]
            if report_date[5:10] not in ['06-30', '12-31']:
                continue

            calc_poc_result_df = pd.read_excel(total_file_path + file_name, converters={"Classify_Industry_Code": str})
            calc_poc_result_df.set_index("Classify_Industry_Code", drop=False, inplace=True)
            true_poc_result_df = df_fund_detail_poc_info[df_fund_detail_poc_info["ReportDate"] == report_date]
            dic_temp_result = self.calc_error_rate(calc_poc_result_df, true_poc_result_df)
            dic_total_result[report_date] = dic_temp_result
        return dic_total_result

    def get_group_weight_result(self, dic_total_result):
        dic_result = {}
        for report_date, dic_temp in dic_total_result.items():
            report_year = report_date[:4]
            dic_result[report_year] = dic_result.get(report_year, {})
            dic_result[report_year][report_date] = dic_temp['total_weight']
        return dic_result

    def analysis_result(self, dic_total_result,code):
        dic_error = {}
        for report_date, calc_result in dic_total_result.items():
            dic_error[report_date] = {}
            dic_error[report_date]['error_abs'] = '%.4f' % calc_result["error_abs_se"].sum()
            dic_error[report_date]['error_square'] = '%.4f' % calc_result["error_square_se"].sum()
            dic_error[report_date]['accuracy_rate'] = '%.2f%%' % (100 * (2 - calc_result["error_abs_se"].sum()) / 2)
        total_error_df = pd.DataFrame(dic_error).T

        corr_coefficient_se = pd.Series(
            {temp_date: temp_result["corr_coefficient"] for temp_date, temp_result in dic_total_result.items()})
        corr_coefficient_se.name = "CorrCoefficient"
        fig = plt.figure(figsize=(16, 9))
        ax = fig.add_subplot(111)
        corr_coefficient_se.plot(ax=ax)
        plt.show()

        dic_group_df = self.get_group_weight_result(dic_total_result)
        file_path = os.getcwd() + r"\\CalcResult\\%s\\"%code
        for report_year, dic_df in dic_group_df.items():
            fig = plt.figure(figsize=(16, 9))

            report_date1 = (report_year + "-06-30")
            ax_num = fig.add_subplot(211)
            total_weight_df = dic_df[report_date1].copy()
            total_weight_df.set_index("Classify_FirstIndustryName", inplace=True)
            total_weight_df[["CalcWeight", "TrueWeight"]].plot.bar(ax=ax_num, legend=True)
            ax_num.set_title("报告期：%s;误差平方和：%s;误差绝对值和：%s;准确率%s;" % (
            report_date1, total_error_df.loc[report_date1]['error_square'], total_error_df.loc[report_date1]['error_abs'],
            total_error_df.loc[report_date1]['accuracy_rate']))
            x_axis = ax_num.axes.get_xaxis()
            x_axis.set_label_text('')

            report_date2 = (report_year + "-12-31")
            ax_num1 = fig.add_subplot(212)
            total_weight_df1 = dic_df[report_date2].copy()
            total_weight_df1.set_index("Classify_FirstIndustryName", inplace=True)
            total_weight_df1[["CalcWeight", "TrueWeight"]].plot.bar(ax=ax_num1, legend=True)
            ax_num1.set_title("报告期：%s;误差平方和：%s;误差绝对值和：%s;准确率%s;" % (
            report_date2, total_error_df.loc[report_date2]['error_square'], total_error_df.loc[report_date2]['error_abs'],
            total_error_df.loc[report_date2]['accuracy_rate']))
            x_axis = ax_num1.axes.get_xaxis()
            x_axis.set_label_text('')

            plt.tight_layout(pad=0.8, w_pad=1.5, h_pad=1.5)
            plt.savefig(file_path+"%s年持仓穿透对比.png" % report_year)
        plt.show()

    def get_main(self, code):
        dic_total_result = self.get_calc_result(code=code)
        self.analysis_result(dic_total_result,code)


if __name__ == "__main__":
    result_any_demo = ResultAny()
    result_any_demo.get_main(code='110011')
