# -*-coding:utf-8-*-
# Author:ZouHao
# email:1084848158@qq.com


import pandas as pd
from datetime import timedelta, datetime

import mylog as mylog_demo
from FundReportCalc.GetDataPack.GetFundData import GetFundData
from FundReportCalc.FundPocOptimize import FundPocOptimize
from FundReportCalc.CalcReportPocDetail import CalcReportPocDetail


class CalcMain:
    def __init__(self):
        self.logger = mylog_demo.set_log('logfile')
        self.logger.info("calc_main init finish!")
        self.get_fund_data_demo = GetFundData(self.logger)
        self.fund_poc_optimize_demo = FundPocOptimize(self.logger)

    def get_fund_data(self, inner_code, calc_flag='Report'):
        """
        获取所需所有数据,不包含备选股票池
        :param code:
        :return:
        """
        self.calc_continue = True
        dic_data = {}
        self.get_fund_data_demo = GetFundData(self.logger)
        df_fund_asset, base_info = self.get_fund_data_demo.get_fund_base_info(inner_code)
        InvestmentType = base_info.iloc[0]['InvestmentType']
        if df_fund_asset.empty:
            self.logger.error("current fund have no df_fund_asset")
            self.calc_continue = False
            return

        dic_baseinfo = base_info.iloc[0].to_dict()
        self.logger.info("fund base info : %s" % str(dic_baseinfo))
        # 股票仓位占比
        df_fund_asset['total_stock_national'] = df_fund_asset[['RINOfStock', 'RINOfNational']].sum(axis=1)
        self.logger.info("get fund asset data success!")

        # 交易日信息
        start_date = df_fund_asset['InfoPublDate'].min() - timedelta(days=15)
        end_date = df_fund_asset['InfoPublDate'].max() + timedelta(days=15)

        df_trade_info = self.get_fund_data_demo.get_trade_date(start_date, end_date)
        self.logger.info("get trade info data success!")

        self.fund_inner_code = inner_code

        # 季报发布的基金投资行业占比
        df_pub_indu_info = self.get_fund_data_demo.get_fund_pub_indus_info(InvestmentType,inner_code=self.fund_inner_code,)

        self.logger.info("get fund publish industry data success!")

        # 季报发布的基金重仓股数据
        df_fund_poc_info, lack_classify_list = self.get_fund_data_demo.get_fund_poc_info(InvestmentType,self.fund_inner_code)
        self.logger.info("get fund poc_info  data success!")

        if calc_flag != 'Report' or dic_baseinfo['Type'] == 1:
            total_report_date = list(df_pub_indu_info['ReportDate'].unique())
            total_report_date = [report_date.strftime("%Y-%m-%d") for report_date in
                                 pd.to_datetime(total_report_date).tolist() if
                                 report_date.strftime("%Y-%m-%d")[-5:] in ['03-31', '06-30', '09-30', '12-13']]
            # 基金的行情数据
            df_fund_daily_quote = self.get_fund_data_demo.get_fund_daily_change_pct(inner_code=self.fund_inner_code,
                                                                                    start_date=min(total_report_date))
            dic_data['df_fund_daily_quote'] = df_fund_daily_quote

        dic_data['df_fund_asset'] = df_fund_asset
        dic_data['df_trade_info'] = df_trade_info
        dic_data['df_pub_indu_info'] = df_pub_indu_info
        dic_data['df_fund_poc_info'] = df_fund_poc_info
        dic_data['fund_type'] = dic_baseinfo['Type']
        self.dic_data = dic_data
        self.lack_classify_list = lack_classify_list
        return dic_data

    def calc_stock_bool(self, df_total_stock_info, heav_poc_list=[]):
        """
        获取备选股票池，以中信一级行业前60% 与当前基金十大重仓并集为准
        :param df_total_stock_info:
        :param df_current_heav_poc:
        :return:
        """
        df_total_stock_info = df_total_stock_info.sort_values(by='TotalMV', ascending=False)

        # 筛选股票池
        maret_value_rate = 0.6
        df_list = []
        for FirstIndustryCode, temp_df in df_total_stock_info.groupby("ZX_FirstIndustryCode"):
            df_list = df_list + temp_df.index[:int(temp_df.shape[0] * maret_value_rate)].tolist()

        total_list = list(set(df_list).union(set(heav_poc_list)))
        df = df_total_stock_info.loc[total_list]
        return df

    def get_nearest_trade(self, info_pub_date):
        fund_type = self.dic_data['fund_type']
        if fund_type != 1:
            ChaTrade = info_pub_date - self.dic_data['df_trade_info']["TradingDate"]
            TradeInt = [datestr.days for datestr in ChaTrade.tolist()]
            TradeInt_Se = pd.Series(TradeInt, index=self.dic_data['df_trade_info']["TradingDate"])
        else:
            self.logger.info("current fund is enclosed fund!")
            ChaTrade = info_pub_date - self.dic_data['df_fund_daily_quote']["TradingDay"]
            TradeInt = [datestr.days for datestr in ChaTrade.tolist()]
            TradeInt_Se = pd.Series(TradeInt, index=self.dic_data['df_fund_daily_quote']["TradingDay"])

        # 季报披露前后最近的交易日
        before_trade_date = TradeInt_Se[TradeInt_Se > 0].idxmin()
        after_trade_date = TradeInt_Se[TradeInt_Se < 0].idxmax()
        return before_trade_date, after_trade_date

    def calc_result_detail(self, weight_df, df_curret_stock):
        useful_se = weight_df['stock_weight'].copy()
        df_useful_stock = df_curret_stock.loc[useful_se.index.tolist()]
        df_final_stock = pd.concat([df_useful_stock, pd.DataFrame(useful_se)], axis=1, sort=True)
        dic_df = {}
        for ZX_FirstIndustryCode, temp_df in df_final_stock.groupby("ZX_FirstIndustryCode"):
            dic_df[ZX_FirstIndustryCode] = {}
            dic_df[ZX_FirstIndustryCode]['Weight'] = temp_df['stock_weight'].sum()
            dic_df[ZX_FirstIndustryCode]['Classify_FirstIndustryName'] = temp_df['ZX_FirstIndustryName'].unique()[0]
            dic_df[ZX_FirstIndustryCode]['Classify_Standard'] = temp_df['ZX_STandard3'].unique()[0]
        df = pd.DataFrame(dic_df).T
        df['Classify_Industry_Code'] = df.index.tolist()
        return df

    def get_calc_main(self, inner_code,error_dic={}):
        self.logger.info("curretn inner_code %s" % inner_code)
        self.get_fund_data(inner_code)
        if not self.calc_continue:
            return
        calc_report_poc_demo = CalcReportPocDetail(self)
        calc_report_poc_demo.calc_process_main(error_dic)
        self.logger.info("calc finish!!!\n\n\n")

    def get_total_fund_innercode(self):
        df_fund_innercode = self.get_fund_data_demo.get_total_fund()
        return df_fund_innercode


if __name__ == '__main__':
    calc_main_demo = CalcMain()
    total_error_list = []
    total_fund_innercode = calc_main_demo.get_total_fund_innercode()['InnerCode'].tolist()[1:]
    for inner_code in total_fund_innercode:
        calc_main_demo.get_calc_main(inner_code=inner_code)
            # calc_main_demo.logger.error("currend innercode %s calc error !!"%inner_code)
            # total_error_list.append(inner_code)
    # calc_main_demo.logger.error("total error innercode %s"%total_error_list)
    calc_main_demo.get_calc_main(inner_code=16207,error_dic={"2021-03-31":[]})
