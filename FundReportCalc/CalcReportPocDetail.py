# -*-coding:utf-8-*-
# Author:ZouHao
# email:1084848158@qq.com
'''
计算定期公告发布时，持仓穿透结果
'''
import numpy as np
import pandas as pd

class CalcReportPocDetail():
    def __init__(self, calc_main_demo):
        self.calc_main_demo = calc_main_demo

    def calc_process_detail(self, report_date):
        self.calc_main_demo.logger.info("calc date %s ..." % report_date.strftime("%Y-%m-%d"))
        dic_data = self.calc_main_demo.dic_data
        temp_df = dic_data['df_fund_poc_info'][dic_data['df_fund_poc_info']["ReportDate"] == report_date]
        before_trade_date, after_trade_date = self.calc_main_demo.get_nearest_trade(
            temp_df["InfoPublDate"].unique()[0])
        info_pub_date = temp_df['InfoPublDate'].unique()[0]

        # 前十大重仓股
        df_current_heav_poc = dic_data['df_fund_poc_info'][
            dic_data['df_fund_poc_info']['ReportDate'] == report_date].copy()

        heav_poc_lack = [code for code in self.calc_main_demo.lack_classify_list if
                         code in df_current_heav_poc['StockInnerCode'].tolist()]
        if heav_poc_lack:
            self.calc_main_demo.logger.error("current poc have lack classify： %s"%heav_poc_lack)

        # 季报披露的行业比例
        df_current_industry = dic_data['df_pub_indu_info'][
            dic_data['df_pub_indu_info']['ReportDate'] == report_date].copy()

        self.calc_main_demo.logger.info(
            "%s industy total ratio: %s" % (report_date.strftime("%Y-%m-%d"), df_current_industry['RatioInNV'].sum()))

        # 股票池数据
        df_total_stock_info,lack_code = self.calc_main_demo.get_fund_data_demo.get_stock_bool_info_new(before_trade_date,
                                                                                             info_pub_date, heav_code=
                                                                                             df_current_heav_poc[
                                                                                                 'StockInnerCode'].tolist())
        df_curret_stock = self.calc_main_demo.calc_stock_bool(df_total_stock_info,
                                                              df_current_heav_poc['StockInnerCode'].tolist())

        lack_df = pd.DataFrame()
        if lack_code:
            lack_df =df_curret_stock.loc[lack_code]

        df_stock_quote = self.calc_main_demo.get_fund_data_demo.get_stock_change_pct(before_trade_date,
                                                                                     after_trade_date,lack_df,
                                                                                     inner_code_list=df_curret_stock.index.tolist(),)
        df_fund_quote = self.calc_main_demo.get_fund_data_demo.get_fund_change_pct(before_trade_date,
                                                                                   after_trade_date,
                                                                                   inner_code=self.calc_main_demo.fund_inner_code)

        weight_df = self.calc_main_demo.fund_poc_optimize_demo.calc_stock_weight(before_trade_date,
                                                                                 after_trade_date,
                                                                                 df_current_heav_poc,
                                                                                 df_current_industry,
                                                                                 df_stock_quote,
                                                                                 df_fund_quote, df_curret_stock)
        if weight_df.empty:
            self.calc_main_demo.logger.error(
                "%s calc fail,please check the reason!" % report_date.strftime("%Y-%m-%d"))
        return weight_df

    def calc_and_save_result(self, weight_df, neww_folder, df_curret_stock, report_date):
        df = self.calc_main_demo.calc_result_detail(weight_df, df_curret_stock)
        df["Classify_FirstIndustryName"] = [error_str.encode('latin1').decode('gbk') for error_str in
                                            df["Classify_FirstIndustryName"].tolist()]
        weight_df["SecuAbbr"] = [error_str.encode('latin1').decode('gbk') for error_str in
                                 weight_df["SecuAbbr"].tolist()]
        if neww_folder:
            df.to_excel(neww_folder + "%s计算结果.xlsx" % report_date.strftime("%Y-%m-%d"))
            weight_df.to_excel(neww_folder + "%s详细股票池.xlsx" % report_date.strftime("%Y-%m-%d"))

    def calc_process_main(self,error_dic={}):
        dic_data = self.calc_main_demo.dic_data
        df_list = []
        error_date_list = []
        for report_date, temp_df in dic_data['df_fund_poc_info'].groupby("ReportDate"):
            if report_date.strftime("%Y-%m-%d")[-5:] not in ['03-31', '06-30', '09-30', '12-31']:
                self.calc_main_demo.logger.error(
                    "%s not in reportdate,continue!" % report_date.strftime("%Y-%m-%d"))
                continue

            if error_dic:
                if report_date.strftime("%Y-%m-%d") not in list(error_dic.keys()):
                    continue

            weight_df = self.calc_process_detail(report_date)
            if weight_df.empty:
                error_date_list.append(report_date)
            df_list.append({'date_str': report_date, "stook_bool": weight_df})
        if error_date_list:
            self.calc_main_demo.logger.error("calc error date :%s" % error_date_list)
        return df_list
