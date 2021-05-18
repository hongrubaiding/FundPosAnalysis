# -*-coding:utf-8-*-
# Author:ZouHao
# email:1084848158@qq.com


import numpy as np
import cvxpy as cp
import pandas as pd
import string


class FundPocOptimize:
    def __init__(self, logger):
        self.logger = logger
        self.logger.info("FundPocOptimize init finish !")

    def calc_stock_weight(self, before_trade_date, after_trade_date, df_current_heav_poc, df_current_industry,
                          df_stock_quote, df_fund_quote, df_curret_stock):
        df_before_stock_pct = df_stock_quote[df_stock_quote["TradingDay"] == before_trade_date].set_index(
            "InnerCode").drop("TradingDay", axis=1)
        df_after_stock_pct = df_stock_quote[df_stock_quote["TradingDay"] == after_trade_date].set_index(
            "InnerCode").drop("TradingDay", axis=1)

        total_stock_code = list(set(df_after_stock_pct.index.tolist()).intersection(df_before_stock_pct.index.tolist()))
        if df_before_stock_pct.shape[0] != df_after_stock_pct.shape[0]:
            self.logger.info(
                "trade afere and before exist stock lack:%s" % [code for code in df_before_stock_pct.index if
                                                                code not in df_after_stock_pct.index])
        df_after_stock_pct = df_after_stock_pct.loc[total_stock_code]
        df_before_stock_pct = df_before_stock_pct.loc[total_stock_code]

        stock_num = df_before_stock_pct.shape[0]
        before_stock_return = np.matrix(df_before_stock_pct['ChangePCT']).T
        after_stock_return = np.matrix(df_after_stock_pct['ChangePCT']).T

        df_fund_quote["TradingDayUpdate"] = [datestr.strftime("%Y-%m-%d") for datestr in df_fund_quote["TradingDay"]]
        df_fund_quote = df_fund_quote.set_index("TradingDayUpdate")
        before_fund_return = df_fund_quote.loc[before_trade_date.strftime("%Y-%m-%d")]["NVRDailyGrowthRate"]
        after_fund_return = df_fund_quote.loc[after_trade_date.strftime("%Y-%m-%d")]["NVRDailyGrowthRate"]

        df_current_heav_poc.set_index("StockInnerCode", inplace=True)
        current_total_pos = df_current_industry['RatioInNV'].sum()

        self.logger.info("current industry RatioInNV: %s;heav_poc RatioInNV: %s;curretnt stockbool SEC: %s" % (
        current_total_pos, df_current_heav_poc['RatioInNV'].sum(),len(df_curret_stock['SEC_FirstIndustryCode'].unique())))

        TOP_10 = np.mat(np.zeros((stock_num, stock_num)))
        top_10 = np.mat(np.zeros((stock_num, 1)))
        POS_MAX = np.mat(np.zeros((stock_num, stock_num)))
        pos_max = np.mat(np.ones((stock_num, 1)))
        pos_min = np.mat(np.zeros((stock_num, 1)))
        POS_MIN = np.diag([1] * stock_num)

        stock_inner_code_list = df_before_stock_pct.index.tolist()
        for inner_code_num in range(0, len(stock_inner_code_list)):
            check_inner_code = stock_inner_code_list[inner_code_num]
            if check_inner_code in df_current_heav_poc.index.tolist():
                TOP_10[inner_code_num, inner_code_num] = 1
                top_10[inner_code_num] = df_current_heav_poc.loc[check_inner_code]["RatioInNV"]
            else:
                POS_MAX[inner_code_num, inner_code_num] = 1
                pos_max[inner_code_num] = df_current_heav_poc["RatioInNV"].min()
        self.logger.info("matrix init finish!")

        SEC_Num = 19
        df_curret_stock = df_curret_stock.loc[stock_inner_code_list]
        df_sec = pd.DataFrame(np.zeros((SEC_Num, stock_num)), index=list(string.ascii_uppercase[:SEC_Num]),
                              columns=stock_inner_code_list)
        sec = np.mat(np.zeros((SEC_Num, 1)))
        for inner_code in df_curret_stock.index:
            df_sec.loc[df_curret_stock.loc[inner_code]['SEC_FirstIndustryCode']][inner_code]=1

        for InduDiscCode in df_sec.index:
            loc_num  = df_sec.index.tolist().index(InduDiscCode)
            if InduDiscCode in df_current_industry["InduDiscCode"].tolist():
                sec[loc_num]=df_current_industry[df_current_industry["InduDiscCode"] == InduDiscCode]['RatioInNV']

        # for index_sec_indu in df_sec.index.tolist():
        #     for column_stock_inner_code in df_sec.columns.tolist():
        #         if df_curret_stock.loc[column_stock_inner_code]["SEC_FirstIndustryCode"] == index_sec_indu:
        #             df_sec.loc[index_sec_indu][column_stock_inner_code] = 1
        #
        #     if index_sec_indu in df_current_industry["InduDiscCode"].tolist():
        #         sec[df_sec.index.tolist().index(index_sec_indu)] = \
        #             df_current_industry[df_current_industry["InduDiscCode"] == index_sec_indu]['RatioInNV']

        SEC = np.mat(df_sec)
        self.logger.info("FundPocOptimize slove......")

        w = cp.Variable((stock_num, 1))
        constr = []
        constr.append(SEC @ w == sec)
        constr.append(TOP_10 @ w == top_10)
        constr.append(POS_MAX @ w <= pos_max)
        constr.append(POS_MIN @ w >= pos_min)
        constr.append(sum(w) == current_total_pos)
        for cons in constr:
            if not cons.is_dcp():
                self.logger.error("current constr is not DCP!!!")

        target_fun = cp.square(before_stock_return.T @ w - before_fund_return) + cp.square(
            after_stock_return.T @ w - after_fund_return)
        if not target_fun.is_dcp():
            self.logger.error("current target_fun is not DCP!!!")

        # self.logger.info("target_fun : %s"%target_fun.is_dqcp())
        prob = cp.Problem(cp.Minimize(target_fun), constr)

        # self.logger.info("prob : %s" % prob.is_dqcp())

        prob.solve(solver=cp.OSQP, max_iter=100000, rho=1e-10, verbose=False)  # Returns the optimal value.

        self.logger.info("FundPocOptimize slove status %s" % prob.status)
        weight_se = pd.Series()
        weight_se.name = "StockInnerCode"
        weight_df = pd.DataFrame(weight_se)
        if prob.status not in ["infeasible_inaccurate", "infeasible"]:
            weight_list = [num[0] for num in w.value]
            weight_se = pd.Series(weight_list, index=stock_inner_code_list, name='stock_weight')
            weight_se[weight_se < 1e-7] = 0
            weight_df = pd.concat([df_curret_stock.loc[weight_se.index.tolist()][['SecuAbbr', 'SecuCode']], weight_se],
                                  axis=1, sort=True)
        return weight_df

    def calc_daily_weight(self, stock_quote, last_weight_df, fund_rate):
        stock_num = last_weight_df.shape[0]
        try:
            stookbool_quote = stock_quote.loc[last_weight_df.index.tolist()]['ChangePCT']
        except:
            a = 0

        w = cp.Variable((stock_num, 1))
        lamda = 10

        pos_min = np.mat(np.zeros((stock_num, 1)))
        POS_Init = np.diag([1] * stock_num)
        pos_max = 0.1 * np.mat(np.ones((stock_num, 1)))

        constr = []
        # constr.append(SEC @ w == sec)
        # constr.append(TOP_10 @ w == top_10)
        constr.append(POS_Init @ w <= pos_max)
        constr.append(POS_Init @ w >= pos_min)
        # constr.append(sum(w) == current_total_pos)
        target_fun = cp.sum_squares(fund_rate - w.T @ stookbool_quote) + lamda * cp.sum_squares(
            np.mat(last_weight_df['stock_weight']).T - w)

        prob = cp.Problem(cp.Minimize(target_fun), constr)
        prob.solve(solver=cp.OSQP, max_iter=100000, rho=1e-5, verbose=False)  # Returns the optimal value.
        self.logger.info("FundPocOptimize slove status %s" % prob.status)
        weight_se = pd.Series()
        weight_se.name = "StockInnerCode"
        weight_df = pd.DataFrame(weight_se)
        if prob.status not in ["infeasible_inaccurate", "infeasible"]:
            weight_list = [num[0] for num in w.value]
            weight_se = pd.Series(weight_list, index=last_weight_df.index.tolist(), name='stock_weight')
            weight_se[weight_se < 1e-7] = 0
            weight_df = pd.DataFrame(weight_se)
        return weight_df
