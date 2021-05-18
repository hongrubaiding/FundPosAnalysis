# -- coding: utf-8 --
# Author:ZouHao
# email:1084848158@qq.com

from GetDataPack.ConnectDataBase import ConnectDataBase
import pandas as pd
import numpy as np

import warnings

warnings.filterwarnings("ignore")


class GetFundData:
    def __init__(self, logger):
        connect_database_demo = ConnectDataBase()
        self.conn = connect_database_demo.connect_database(flag="JYDB-Formal")
        self.logger = logger
        self.logger.info("GetFundData init finish!")

    def get_fund_base_info(self, inner_code):
        """
        获取季报披露仓位信息
        :param code:
        :return:
        """
        sql_str = "SELECT InnerCode,InfoPublDate,ReportDate,RINOfStock,RINOfNational FROM MF_AssetAllocationNew" \
                  " WHERE InnerCode=%s and ReportDate>'2012-12-31'" % inner_code
        df = pd.read_sql(sql_str, self.conn)

        sql1_str = "select EstablishmentDate,MainCode,Type,InvestmentType from MF_FundArchives where InnerCode=%s;" % inner_code
        df1 = pd.read_sql(sql1_str, self.conn)
        return df, df1

    def get_trade_date(self, start_date, end_date):
        """
        后去指数日期，用于计算交易日
        :param start_date:
        :param end_date:
        :return:
        """
        sql_str = "select TradingDate from QT_TradingDayNew where SecuMarket=83 and" \
                  " TradingDate between '%s' and '%s' and IfTradingDay=1" % (start_date, end_date)
        df = pd.read_sql(sql_str, self.conn)
        return df

    def adjust_ratioinnv(self, df):
        df_list = []
        for report_date, temp_df in df.groupby("ReportDate"):
            if temp_df[temp_df['RatioInNV'] == 0].empty:
                df_list.append(temp_df)
            else:
                max_index = temp_df['RatioInNV'].idxmax()
                temp_list = []
                for index in temp_df[temp_df['RatioInNV'] == 0].index:
                    temp_df1 = temp_df.loc[index, :].copy()
                    temp_df1['RatioInNV'] = temp_df.loc[index]['MarketValue'] * temp_df.loc[
                        max_index]['RatioInNV'] / temp_df.loc[max_index]['MarketValue']
                    temp_df1 = pd.DataFrame(temp_df1).T
                    temp_list.append(temp_df1)
                new_temp_df = pd.concat([temp_df[temp_df['RatioInNV'] != 0], pd.concat(temp_list, axis=0, sort=True)],
                                        axis=0, sort=True)
                df_list.append(new_temp_df)
        df_result = pd.concat(df_list, axis=0, sort=True)
        return df_result

    def get_fund_pub_indus_info(self, InvestmentType, inner_code):
        """
        获取季报披露的行业投资比例
        :param inner_code:
        :return:
        """

        sql_str = "select InnerCode,InfoPublDate,ReportDate,IndustryName,InvestType,RatioInNV,InduStandard,IndustryCode," \
                  "InduDiscCode,MarketValue  from MF_InvestIndustry" \
                  " where InnerCode=%s and InduDiscCode is not null and InduStandard=22 order by  ReportDate desc;" % inner_code
        df = pd.read_sql(sql_str, self.conn)
        df = self.adjust_ratioinnv(df)

        if 3 in df['InvestType'].tolist():
            # 当前为指数基金，同行业有主动投资和被动投资
            df_list = []
            for report_date, temp_df in df.groupby("ReportDate"):
                if len(temp_df['InduDiscCode'].unique()) == temp_df.shape[0]:
                    df_list.append(temp_df)
                    continue

                for indudis_code, temp_df1 in temp_df.groupby("InduDiscCode"):
                    if temp_df1.shape[0] == 1:
                        df_list.append(temp_df1)
                        continue
                    new_temp = temp_df1[temp_df1["InduDiscCode"] == indudis_code].copy()
                    new_temp["RatioInNV"] = temp_df1[temp_df1["InduDiscCode"] == indudis_code]["RatioInNV"].sum()
                    new_temp = new_temp[new_temp["InvestType"] == 3]
                    df_list.append(new_temp)
            df = pd.concat(df_list, axis=0, sort=True)
        return df

    def get_fund_net_value(self, inner_code):
        """
        获取复权单位净值
        :param inner_code:
        :return:
        """
        sql_str = "select InnerCode,TradingDay,UnitNVRestored from MF_FundNetValueRe " \
                  "where InnerCode=%s order by TradingDay;" % inner_code
        df = pd.read_sql(sql_str, self.conn)
        return df

    def handle_industry_change(self, df, df1):
        df_list = []

        for InfoPublDate, temp_df in df.groupby("InfoPublDate"):
            temp_df1 = temp_df.copy().set_index('StockInnerCode', drop=True)
            have_list = list(set(temp_df1.index).intersection(df1.index))
            temp_df2 = df1.loc[have_list, :]

            if len(temp_df1.index.unique()) == len(temp_df2.index.unique()) and temp_df1.shape[0] == temp_df2.shape[0]:
                # 不存在股票行业分类有过变动
                stock_indus_df = pd.concat([temp_df1, temp_df2], axis=1, sort=True)
            else:
                df_not_change_industry = temp_df2[np.isnan(temp_df2['CancelDate'])]
                df_change_industry = temp_df2[~ np.isnan(temp_df2['CancelDate'])].sort_values("CancelDate",
                                                                                              ascending=True)

                much_classify_commany = {}
                for CompanyCode, temp_df_industry in df_change_industry.groupby("CompanyCode"):
                    # 取距离季报发布最近，取消日期大于季报发布日期的分类
                    total_cancel_date = temp_df_industry['CancelDate'].tolist()
                    cancel_date = ''

                    for cancel_date_num in range(0, len(total_cancel_date)):
                        if (total_cancel_date[cancel_date_num] - InfoPublDate).days >= 0:
                            cancel_date = total_cancel_date[cancel_date_num]
                            break

                    if cancel_date:
                        use_df = temp_df_industry[temp_df_industry['CancelDate'] == cancel_date]
                        # use_df.loc['CompanyCode'] = CompanyCode
                        use_df['CompanyCode'] = CompanyCode
                        much_classify_commany[CompanyCode] = use_df

                much_classify_df = pd.DataFrame()
                if much_classify_commany.values():
                    much_classify_df = pd.concat(much_classify_commany.values(), axis=0, sort=True)

                not_chage_list = list(set(df_not_change_industry['CompanyCode']).difference(set(much_classify_commany)))
                abselute_not_change_industry = df_not_change_industry.set_index('CompanyCode', drop=False).loc[
                                               not_chage_list, :]

                if not much_classify_df.empty:
                    df_not_change_industry_new = pd.concat([abselute_not_change_industry, much_classify_df], axis=0,
                                                           sort=True)
                else:
                    df_not_change_industry_new = abselute_not_change_industry

                df_not_change_industry_new = df_not_change_industry_new.set_index('StockInnerCode', drop=False)
                row_loc = list(set(df_not_change_industry_new['StockInnerCode'].index).intersection(temp_df1.index))
                stock_indus_df = pd.concat([df_not_change_industry_new.loc[row_loc, :], temp_df1.loc[row_loc, :]],
                                           axis=1,

                                           sort=True)
            stock_indus_df['InfoPublDate'] = InfoPublDate
            stock_indus_df.drop('CompanyCode', axis=1, inplace=True)
            df_list.append(stock_indus_df.reset_index(drop=True))
        df = pd.concat(df_list, axis=0, sort=True, ignore_index=True)
        return df

    def combine_industy_classify(self, df):
        """
        根据披露的重仓股数据，获取对应的行业分类结果
        :param df:
        :return:
        """
        sql_str1 = "select  InnerCode,CompanyCode from SecuMain where InnerCode in %s;" % str(
            tuple(df['StockInnerCode'].tolist()))
        df1 = pd.read_sql(sql_str1, self.conn).set_index('CompanyCode', drop=True)

        sql_str2 = "select  CompanyCode,FirstIndustryName,FirstIndustryCode,CancelDate,Standard from DZ_ExgIndustry" \
                   " where CompanyCode in %s and Standard=22 and (InfoSource is null or InfoSource!='临时公告')" % str(
            tuple(df1.index.tolist()))
        df2 = pd.read_sql(sql_str2, self.conn).set_index('CompanyCode', drop=False)

        lack_classify_list = df1.loc[list(set(df1.index).difference(set(df2.index))), :].index.tolist()
        if lack_classify_list:
            self.logger.error("heavy poc lack SEC classify CompanyCode,maybe KCB!!!")
            self.logger.error("%s" % lack_classify_list)

        use_code = [code for code in df1.index.tolist() if code not in lack_classify_list]
        use_df = pd.concat([df2, df1.loc[use_code, :]], sort=True, axis=1)
        use_df.rename(columns={"InnerCode": "StockInnerCode"}, inplace=True)
        use_df = use_df.set_index('StockInnerCode', drop=False)

        df_result = self.handle_industry_change(df, use_df)
        return df_result, lack_classify_list

    def get_fund_poc_info(self, InvestmentType, inner_code):
        """
        获取季报披露的重仓股数据
        :param inner_code:
        :return:
        """
        if not InvestmentType in [7, 8]:
            sql_str = "select A.InfoPublDate,A.ReportDate,A.StockInnerCode,A.RatioInNV,A.MarketValue,B.CompanyCode,B.SecuCode,B.SecuAbbr,B.ListedSector" \
                      " from MF_KeyStockPortfolio A inner join SecuMain B on A.StockInnerCode=B.InnerCode " \
                      "where A.InnerCode=%s and A.ReportDate>'2012-12-31' order by InfoPublDate desc;" % inner_code
        else:
            sql_str = "select A.InfoPublDate,A.ReportDate,A.StockInnerCode,A.RatioInNV,A.MarketValue,B.CompanyCode,B.SecuCode,B.SecuAbbr,B.ListedSector" \
                      " from MF_KeyStockPortfolio A inner join SecuMain B on A.StockInnerCode=B.InnerCode " \
                      "where A.InnerCode=%s and A.ReportDate>'2012-12-31' and A.InvestType=3 order by InfoPublDate desc;" % inner_code
        df = pd.read_sql(sql_str, self.conn)
        df_result, lack_classify_list = self.combine_industy_classify(df)

        return df_result, lack_classify_list

    def get_fund_total_poc_info(self, secu_code, ):
        """
        获取年报半年报详细持仓数据
        :param inner_code:
        :return:
        """
        sql_str = "select A.InnerCode,A.InfoPublDate,A.ReportDate,A.StockInnerCode,A.RatioInNV,B.SecuAbbr," \
                  "B.SecuCode,B.CompanyCode from MF_StockPortfolioDetail A inner join SecuMain B on A.StockInnerCode=B.InnerCode" \
                  " where A.InnerCode in (select InnerCode from SecuMain where SecuCode='%s' and" \
                  " SecuCategory=8 and ListedState=1);" % secu_code
        df_detail_info = pd.read_sql(sql_str, self.conn)

        total_companycode = df_detail_info['CompanyCode'].tolist()
        sql_str = "select CompanyCode,InfoPublDate,Standard,CancelDate,FirstIndustryCode,FirstIndustryName" \
                  " from LC_ExgIndustry where Standard=3 and  CompanyCode in %s;" % str(tuple(total_companycode))
        df_detail_industry_info = pd.read_sql(sql_str, self.conn).set_index("CompanyCode", )

        industry_classify_num_se = df_detail_industry_info.index.value_counts()
        have_not_change_code = industry_classify_num_se[industry_classify_num_se == 1].index.tolist()
        have_change_code = industry_classify_num_se[industry_classify_num_se != 1].index.tolist()
        havenot_industry_change_df = df_detail_industry_info.loc[have_not_change_code][
            ["FirstIndustryCode", "FirstIndustryName", "Standard"]]
        have_industry_change_df = df_detail_industry_info.loc[have_change_code][
            ["FirstIndustryCode", "FirstIndustryName", "CancelDate", "Standard"]]

        total_df_list = []
        for report_date, temp_df in df_detail_info.groupby("ReportDate"):
            temp_industry_df = temp_df.copy().set_index("CompanyCode")
            temp_not_change_code = list(set(have_not_change_code).intersection(temp_industry_df.index.tolist()))
            temp_not_change_df = pd.DataFrame()
            if temp_not_change_code:
                temp_not_change_df = pd.concat(
                    [temp_industry_df.loc[temp_not_change_code], havenot_industry_change_df.loc[temp_not_change_code]],
                    axis=1, sort=True)

            temp_change_code = list(set(have_change_code).intersection(temp_industry_df.index.tolist()))
            temp_change_df = pd.DataFrame()
            if temp_change_code:
                change_df = have_industry_change_df.loc[temp_change_code].copy()
                change_df["StockCompanyCode"] = change_df.index.tolist()
                df_list = []
                for company_code, temp_classify_df in change_df.groupby("StockCompanyCode"):
                    temp_df1 = temp_classify_df[~np.isnan(temp_classify_df['CancelDate'])]
                    target_date = self.get_target_date(date_list=temp_df1["CancelDate"].tolist(),
                                                       current_date=report_date)

                    use_df = temp_classify_df[temp_classify_df["CancelDate"] == target_date].drop("StockCompanyCode",
                                                                                                  axis=1)
                    df_list.append(use_df)
                total_use_df = pd.concat(df_list, axis=0, sort=True)
                temp_change_df = pd.concat([temp_industry_df.loc[temp_change_code], total_use_df], axis=1, sort=True)
            temp_total_industry_df = pd.concat([temp_not_change_df, temp_change_df], axis=0, sort=True)
            temp_total_industry_df["CompanyCode"] = temp_total_industry_df.index.tolist()
            total_df_list.append(temp_total_industry_df)
        total_industry_df = pd.concat(total_df_list, axis=0, ignore_index=True, sort=True)
        return total_industry_df

    def get_target_date(self, date_list, current_date):
        """
        获取日期列表date_list中，距离当前日期最近，但小于当前日期的日期
        :param date_list:
        :param current_date:
        :return:
        """
        target_loc = np.nan
        if len(date_list) == 1:
            target_loc = date_list[0]
            return target_loc
        date_list_temp = date_list.copy()
        date_list_temp.sort()
        date_se = current_date - pd.Series(date_list)
        date_int_se = pd.Series([cha_day.days for cha_day in date_se])
        if not date_int_se[date_int_se < 0].empty:
            target_loc_index = date_int_se[date_int_se < 0].idxmax()

            target_loc = date_list[target_loc_index]
        return target_loc

    def get_stock_bool_info_new(self, before_trade_date, info_pub_date, heav_code=[]):
        sql_str = "select A.InnerCode, A.CompanyCode, A.SecuCode, A.SecuAbbr,B.Standard," \
                  "B.FirstIndustryCode,B.FirstIndustryName,B.CancelDate from SecuMain A inner" \
                  " join DZ_ExgIndustry B on A.CompanyCode=B.CompanyCode where A.SecuMarket" \
                  " in (83, 90) and A.SecuCategory = 1  and A.ListedDate<='%s'" \
                  "and B.Standard in (22,37);" % before_trade_date.strftime("%Y-%m-%d")
        df_total_stock = pd.read_sql(sql_str, self.conn)
        df_total_stock['StockInnerCode'] = df_total_stock["InnerCode"]
        df_total_stock.set_index("StockInnerCode", inplace=True)

        classify_se = df_total_stock["InnerCode"].value_counts()
        # 过滤只有证监会、中信行业一个分类的股票
        df_total_stock1 = df_total_stock.loc[classify_se[classify_se > 1].index]
        # 过滤取消日期小于当前发布日期的分类
        df_total_stock2 = df_total_stock1[
            (df_total_stock1['CancelDate'] > pd.to_datetime(info_pub_date).strftime("%Y-%m-%d")) | np.isnan(
                df_total_stock1['CancelDate'])]
        df_classify_list = []
        for Standard, temp_df in df_total_stock2.groupby("Standard"):
            temp_classify = temp_df['InnerCode'].value_counts()
            single_df = temp_df.loc[temp_classify[temp_classify == 1].index]
            much_classify_df = temp_df.loc[temp_classify[temp_classify > 1].index]
            if not much_classify_df.empty:
                much_df_list = [stock_df[stock_df['CancelDate'] == stock_df['CancelDate'].min()] for innner_code, stock_df
                                in much_classify_df.groupby("InnerCode")]
                temp_classify_df = pd.concat([single_df,pd.concat(much_df_list,sort=True,axis=0)],axis=0,sort=True)
            else:
                temp_classify_df = single_df
            df_classify_list.append(temp_classify_df)
        df_result = pd.concat(df_classify_list, axis=0, sort=True)
        self.logger.info("handle industry classify finish!")

        industry_df = df_result[df_result["Standard"] == 37].copy()
        industry_df = industry_df.rename(
            columns={"FirstIndustryCode": "ZX_FirstIndustryCode",
                     "FirstIndustryName": "ZX_FirstIndustryName", "Standard": "ZX_STandard3"})
        industry_df.set_index("InnerCode", inplace=True, drop=True)

        industry_df1 = df_result[df_result["Standard"] != 37][
            ["InnerCode", "FirstIndustryCode", "FirstIndustryName", "Standard"]].copy()

        industry_df1 = industry_df1.rename(
            columns={"FirstIndustryCode": "SEC_FirstIndustryCode",
                     "FirstIndustryName": "SEC_FirstIndustryName", "Standard": "SEC_STandard22"})
        industry_df1.set_index("InnerCode", inplace=True, drop=True)
        df_final = pd.concat([industry_df, industry_df1], axis=1, sort=True, )

        sql_str1 = "select InnerCode,TotalMV from DZ_Performance where TradingDay='%s' " \
                   "and InnerCode in %s" % (
                       before_trade_date, tuple(df_final.index.tolist()))
        df_stock_quote = pd.read_sql(sql_str1, self.conn)
        df_stock_quote.set_index("InnerCode", inplace=True)
        df_stock_quote, lack_code = self.get_suspend_mv(df_stock_quote, heav_code, before_trade_date)

        total_stock_innercode = list(set(df_stock_quote.index.tolist()).intersection(set(df_final.index.tolist())))
        df_total_stock_info = pd.concat(
            [df_stock_quote.loc[total_stock_innercode], df_final.loc[total_stock_innercode]], axis=1, sort=True)
        return df_total_stock_info, lack_code

    def get_suspend_mv(self, df_stock_quote, heav_code, before_trade_date):
        "停牌的重仓股数据处理"
        lack_code = [code for code in heav_code if code not in df_stock_quote.index]
        if lack_code:
            self.logger.error(
                "heav poc have suspend or othern condition stock: %s,date:%s" % (lack_code, before_trade_date))
            df_list = []
            for code in lack_code:
                sql_str = "select top 1 InnerCode,TotalMV from DZ_Performance where TradingDay<'%s' " \
                          "and InnerCode = %s" % (before_trade_date, code)
                temp_stock_quote = pd.read_sql(sql_str, self.conn)
                df_list.append(temp_stock_quote)
            temp_df = pd.concat(df_list, axis=0, sort=True).set_index("InnerCode")
            df_stock_quote = pd.concat([df_stock_quote, temp_df], axis=0, sort=True)
        return df_stock_quote, lack_code

    def get_stock_bool_info(self, before_trade_date):
        """
        获取指定日期内，全市场股票及所属中信一级行业（standard=3），证监会行业（standard=22），以及指定日期的规模数据
        :param before_trade_date:
        :return:
        """
        sql_str = "select A.InnerCode, A.CompanyCode, A.SecuCode, A.SecuAbbr,B.Standard,B.FirstIndustryCode," \
                  "B.FirstIndustryName from SecuMain A inner join LC_ExgIndustry B on " \
                  "A.CompanyCode=B.CompanyCode where A.SecuMarket in (83, 90) and A.SecuCategory = 1 " \
                  "and A.ListedState = 1 and B.Standard=3 and B.CancelDate is null;"
        df_total_stock = pd.read_sql(sql_str, self.conn).set_index("CompanyCode", drop=False)
        df_total_stock.rename(columns={"Standard": "ZX_STandard3", "FirstIndustryCode": "ZX_FirstIndustryCode",
                                       "FirstIndustryName": "ZX_FirstIndustryName"}, inplace=True)

        sql_sec_standard = "select Standard,FirstIndustryCode,FirstIndustryName,CompanyCode from LC_ExgIndustry where " \
                           "CompanyCode in %s and Standard=22 and CancelDate is null;" % str(
            tuple(df_total_stock["CompanyCode"].tolist()))
        df_total_sec_stock = pd.read_sql(sql_sec_standard, self.conn).set_index("CompanyCode", drop=False)
        df_total_sec_stock.rename(columns={"Standard": "Sec_STandard22", "FirstIndustryCode": "SEC_FirstIndustryCode",
                                           "FirstIndustryName": "SEC_FirstIndustryName"}, inplace=True)

        total_companycode = set(df_total_stock["CompanyCode"].tolist()).intersection(
            df_total_sec_stock["CompanyCode"].tolist())
        df_total_stock = df_total_stock.loc[total_companycode]
        df_total_sec_stock = df_total_sec_stock.loc[total_companycode].drop("CompanyCode", axis=1)
        df_total_stock = pd.concat([df_total_stock, df_total_sec_stock], axis=1, sort=True)

        sql_str1 = "select InnerCode,TotalMV from QT_StockPerformance where TradingDay='%s' " \
                   "and InnerCode in %s" % (
                       before_trade_date, tuple(df_total_stock['InnerCode'].tolist()))
        df_stock_quote = pd.read_sql(sql_str1, self.conn)

        df_total_stock.set_index("InnerCode", inplace=True)
        df_stock_quote.set_index("InnerCode", inplace=True)
        df_total_stock_info = pd.concat([df_stock_quote, df_total_stock], axis=1, sort=True)
        return df_total_stock_info

    def get_stock_change_pct(self, before_trade_date, after_trade_date, lack_df, inner_code_list=[]):
        """
        获取当前股票池在指定日期的涨跌幅，注意：inner_code_list中若有公司在当前日期未上市，取值为空
        :param before_trade_date:
        :param after_trade_date:
        :param inner_code_list:
        :return:
        """
        sql_str = "select InnerCode, ChangePCT,TradingDay from QT_StockPerformance where InnerCode " \
                  "in %s and TradingDay in %s;" % (tuple(inner_code_list),
                                                   (before_trade_date.strftime("%Y-%m-%d"),
                                                    after_trade_date.strftime("%Y-%m-%d")))
        df_stock_quote = pd.read_sql(sql_str, self.conn).set_index("InnerCode", drop=False)
        if not lack_df.empty:
            not_suspend_stock = list(set(df_stock_quote.index).difference(set(lack_df.index)))
            lack_indsutry_code = lack_df['ZX_FirstIndustryCode'].unique()
            self.logger.info("heav poc stock exist suspend:%s,indusrty return replace!!" % lack_df.index.tolist())
            if len(lack_indsutry_code) > 1:
                sql_index_str = "select A.IndexCode,B.IndustryCode from LC_CorrIndexIndustry A inner join" \
                                " CT_IndustryType B on A.IndustryCode=B.IndustryNum where B.IndustryCode in" \
                                " %s and A.IndustryStandard=37 and A.IndexState!=3;" % str(tuple(lack_indsutry_code))
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = "select InnerCode,ChangePCT,TradingDay from QT_IndexQuote where InnerCode in %s and  TradingDay in %s;" \
                                 % (str(tuple(df_index['IndexCode'].unique())), (before_trade_date.strftime("%Y-%m-%d"),
                                                                                 after_trade_date.strftime("%Y-%m-%d")))
            else:
                sql_index_str = "select A.IndexCode,B.IndustryCode from LC_CorrIndexIndustry A inner join" \
                                " CT_IndustryType B on A.IndustryCode=B.IndustryNum where B.IndustryCode =" \
                                " '%s' and A.IndustryStandard=37 and A.IndexState!=3;" % lack_indsutry_code[0]
                df_index = pd.read_sql(sql_index_str, self.conn)
                sql_index_str1 = "select InnerCode,ChangePCT,TradingDay from QT_IndexQuote where InnerCode = %s and  TradingDay in %s;" \
                                 % (df_index['IndexCode'].unique()[0], (before_trade_date.strftime("%Y-%m-%d"),
                                                                        after_trade_date.strftime("%Y-%m-%d")))

            df_index_quote = pd.read_sql(sql_index_str1, self.conn)
            df_lack_quote = []
            for lack_code in lack_df.index:
                ZX_IndustyrCode = lack_df.loc[lack_code]['ZX_FirstIndustryCode']
                index_code = df_index[df_index['IndustryCode'] == ZX_IndustyrCode]['IndexCode'].iloc[0]
                temp_index_quote = df_index_quote[df_index_quote['InnerCode'] == index_code]
                temp_index_quote.loc[:, 'InnerCode'] = lack_code
                df_lack_quote.append(temp_index_quote)
            lack_quote_df = pd.concat(df_lack_quote, axis=0, sort=True).set_index("InnerCode", drop=False)
            df_stock_quote = pd.concat([df_stock_quote.loc[not_suspend_stock, :], lack_quote_df], axis=0, sort=True)
        return df_stock_quote

    def get_fund_change_pct(self, before_trade_date, after_trade_date, inner_code):
        sql_str = "select TradingDay,NVRDailyGrowthRate from MF_FundNetValueRe where " \
                  "TradingDay in %s and InnerCode=%s;" % ((before_trade_date.strftime("%Y-%m-%d")
                                                           , after_trade_date.strftime("%Y-%m-%d")), inner_code)
        df_fund_quote = pd.read_sql(sql_str, self.conn)
        return df_fund_quote

    def get_fund_daily_change_pct(self, inner_code, start_date):
        sql_str = "select TradingDay,NVRDailyGrowthRate from MF_FundNetValueRe where InnerCode=%s and TradingDay>='%s';" % (
            inner_code, start_date)

        df_fund_daily_quote = pd.read_sql(sql_str, self.conn)
        return df_fund_daily_quote

    def get_test_sqlsever(self):
        sql_str = "select A.InnerCode, A.CompanyCode, A.SecuCode, A.SecuAbbr,B.Standard,B.FirstIndustryCode," \
                  "B.FirstIndustryName from SecuMain A inner join LC_ExgIndustry B on " \
                  "A.CompanyCode=B.CompanyCode where A.SecuMarket in (83, 90) and A.SecuCategory = 1 " \
                  "and A.ListedState = 1 and B.Standard=3 and B.CancelDate is null;"
        df_total_stock = pd.read_sql(sql_str, self.conn).set_index("CompanyCode", drop=False)
        self.logger.info(df_total_stock.head())

    def get_stock_quote(self, inner_code_list, start_date, end_date):
        sql_str = "select InnerCode,TradingDay,ChangePCT from QT_StockPerformance where InnerCode in %s and " \
                  "TradingDay>='%s' and TradingDay<='%s';" % (tuple(inner_code_list), start_date, end_date)

        df_stock_quote = pd.read_sql(sql_str, self.conn)
        return df_stock_quote

    def get_total_fund(self):
        sql_str = "select A.InnerCode,B.ListedState from MF_FundArchives A inner join SecuMain B " \
                  "on  A.InnerCode=B.InnerCode  where A.FundNature=1 and A.FundTypeCode" \
                  " in (1101,1103) and A.EnClearingDate is null and B.ListedState!=5;"
        df_fund_innercode = pd.read_sql(sql_str, self.conn)
        return df_fund_innercode


if __name__ == "__main__":
    import mylog as mylog_demo

    logger = mylog_demo.set_log()
    GetFundDataDemo = GetFundData(logger=logger)
    GetFundDataDemo.get_test_sqlsever()
