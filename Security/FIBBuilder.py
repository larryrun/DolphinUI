

class FIBBuilder(object):

    default_fib_bar = {
        # 通用财务指标
        'total_revenue_ps': 2.76, 'revenue_ps': 2.75, 'capital_rese_ps': 1.68, 'surplus_rese_ps': 0.19, 'undist_profit_ps': 1.705, 'gross_margin': 339861664.0,
        # 偿债能力指标
        'current_ratio': 1.76, 'quick_ratio': 1.36, 'liba_cash_ratio': 0.3596581429804615, 'total_asset_liab_ratio': 0.406262709603628, 'ebit_fin_ratio': 6.54214459416338,
        'fcff_int_ratio': 4.07793689953756, 'debt_to_eqt': 0.67, 'assets_to_eqt': 1.67, 'dp_assets_to_eqt': 1.75,
        # 运营能力指标
        'ar_turn': 3.27, 'inv_turn': 3.56261213042226, 'ca_turn': 0.66, 'assets_turn': 0.36, 'roic': 5.23,
        # 盈利能力指标
        'grossprofit_margin': 28.655, 'netprofit_margin': 10.03, 'roa_yearly': 4.61, 'roa_dp': 3.28, 'roe': 6.47, 'roe_waa': 6.58, 'roe_dt': 5.79, 'roe_yearly': 8.62,
        'roe_yoy': 38.955, 'q_roe': 2.45, 'eps_dv_ratio_ratio': 0.0277525481885155, 'dv_ratio': 0.76, 'pe': 42.44, 'ps': 3.2, 'netprofit': 131939536.0,
        # 发展能力指标
        'or_yoy': 16.03, 'q_sales_yoy': 22.2, 'netprofit_yoy': 39.52, 'dt_netprofit_yoy': 42.09, 'assets_yoy': 9.74, 'eqt_yoy': 6.235, 'peg': 1.4653278819905649,
        # 现金流量指标
        'fcff': 152852544.0, 'fcfe': 195418448.0, 'fcff_ps': 0.28500000000000003, 'fcfe_ps': 0.38, 'ocfps': 0.39, 'sale_cash_ratio': 1.0014134085547002,
        'netprofit_cash_ratio': 1.23877074761221, 'ops_netprofit_rank': 1.040176750388, 'ops_cash_rank': 0.13436648405827,
        # 综合绩效指标
        'pb': 2.79, 'bps': 4.51, 'bps_yoy': 5.51, 'pcf': 0.0283018870968602, 'op_profit_ratio': 0.117732219121733, 'profit_cost_ratio': 0.1311522863196365,
        'eps': 0.3, 'dt_eps': 0.29, 'diluted2_eps': 0.29, 'basic_eps_yoy': 40.94499999999999, 'dt_eps_yoy': 41.575,
        # 股东人数
        'holder_num': 30049.0,
        # 老唐财务指标
        't_cash_st_debt_ratio': 1.40453878858223, 't_cash_debt_ratio': 0.602273153561964, 't_cash_depos_ff_ratio': 0.0, 't_total_revenue_yoy': 1.01721412936049,
        't_total_revenue_qoq': 1.6100187975965, 't_accounts_receiv_yoy': 1.04534568198272, 't_accounts_receiv_qoq': 1.04019573934235, 't_prepay_revenue_ratio': 0.0357960051347024,
        't_inventory_yoy': 1.05622426969314, 't_inventory_qoq': 1.028011217728, 't_invent_revenue_yoy': 1.04304788182796, 't_invent_revenue_qoq': 0.6384510311553024,
        't_salary_yoy': 1.01219243171536, 't_salary_qoq': 1.47047827301779, 't_surplus_cap_ratio': 0.06583658364878475, 't_ebit_prod_assets_ratio': 1.95106392539061,
        't_receive_total_assets': 0.175242662908453, 't_grossprofit_margin_dt_yoy': 2.50999999999999, 't_grossprofit_margin_dt_qoq': 1.11, 't_grossprofit_margin_yoy': 0.9884020981425521,
        't_grossprofit_margin_qoq': 1.00402713013989, 't_netprofit_margin_dt_yoy': 2.86, 't_netprofit_margin_dt_qoq': 1.5, 't_netprofit_margin_yoy': 1.00945945945946,
        't_netprofit_margin_qoq': 1.0007023180865149, 't_act_amor_depr_ratio': 3.80079464092459, 't_act_income_ratio': 1.23877074761221, 't_company_portrait': 0
    }

    def __init__(self, custom_fib_bar={}):
        self.custom_fib_bar = custom_fib_bar

    def build(self, indicators):
        scores = {'DebtPay':  self.get_debtpay_score(indicators),
                  'Operate':  self.get_operate_score(indicators),
                  'Profit':   self.get_profit_score(indicators),
                  'Growth':   self.get_growth_score(indicators),
                  'CashFlow': self.get_cashflow_score(indicators),
                  'General':  self.get_general_score(indicators),
                  'MrTang':  self.get_mr_tang_score(indicators),
                  'Booming':  self.get_booming_score(indicators),
                  'Anomaly':  self.get_anomaly_score(indicators)
                  }
        return scores

    def get_fib_bar(self, name):
        return self.custom_fib_bar[name] if name in self.custom_fib_bar else self.default_fib_bar[name]

    def get_debtpay_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.current_ratio[0] > self.get_fib_bar('current_ratio') else 0)
            score += (1 if indicators.quick_ratio[0] > self.get_fib_bar('quick_ratio') else 0)
            score += (1 if indicators.liba_cash_ratio[0] > self.get_fib_bar('liba_cash_ratio') else 0)
            score += (1 if indicators.total_asset_liab_ratio[0] < self.get_fib_bar('total_asset_liab_ratio') else 0)
            score += (1 if indicators.ebit_fin_ratio[0] > self.get_fib_bar('ebit_fin_ratio') else 0)
            score += (1 if indicators.fcff_int_ratio[0] > self.get_fib_bar('fcff_int_ratio') else 0)
        return score

    def get_operate_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.ar_turn[0] > self.get_fib_bar('ar_turn') else 0)
            score += (1 if indicators.inv_turn[0] > self.get_fib_bar('inv_turn') else 0)
            score += (1 if indicators.ca_turn[0] > self.get_fib_bar('ca_turn') else 0)
            score += (1 if indicators.assets_turn[0] > self.get_fib_bar('assets_turn') else 0)
            score += (1 if indicators.roic[0] > self.get_fib_bar('roic') else 0)
        return score

    def get_profit_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.grossprofit_margin[0] > self.get_fib_bar('grossprofit_margin') else 0)
            score += (1 if indicators.netprofit_margin[0] > self.get_fib_bar('netprofit_margin') else 0)
            score += (1 if indicators.roa_yearly[0] > self.get_fib_bar('roa_yearly') or
                           indicators.roa_dp[0] > self.get_fib_bar('roa_dp')
                      else 0)
            score += (1 if indicators.roe[0] > self.get_fib_bar('roe') or
                           indicators.roe_waa[0] > self.get_fib_bar('roe_waa') or
                           indicators.roe_dt[0] > self.get_fib_bar('roe_dt') or
                           indicators.roe_yearly[0] > self.get_fib_bar('roe_yearly') or
                           indicators.q_roe[0] > self.get_fib_bar('q_roe') or
                           indicators.roe_yoy[0] > self.get_fib_bar('roe_yoy')
                      else 0)
            score += (1 if indicators.eps_dv_ratio_ratio[0] > self.get_fib_bar('eps_dv_ratio_ratio') else 0)
            score += (1 if indicators.dv_ratio[0] > self.get_fib_bar('dv_ratio') else 0)
            score += (1 if 0 < indicators.pe[0] < self.get_fib_bar('pe') else 0)
            score += (1 if 0 < indicators.ps[0] < self.get_fib_bar('ps') else 0)
        return score

    def get_growth_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.or_yoy[0] > self.get_fib_bar('or_yoy') or
                           indicators.q_sales_yoy[0] > self.get_fib_bar('q_sales_yoy')
                      else 0)
            score += (1 if indicators.netprofit_yoy[0] > self.get_fib_bar('netprofit_yoy') or
                           indicators.dt_netprofit_yoy[0] > self.get_fib_bar('dt_netprofit_yoy')
                      else 0)
            score += (1 if indicators.assets_yoy[0] > self.get_fib_bar('assets_yoy') else 0)
            score += (1 if indicators.eqt_yoy[0] > self.get_fib_bar('eqt_yoy') else 0)
            score += (1 if 0 < indicators.peg[0] < self.get_fib_bar('peg') else 0)
        return score

    def get_cashflow_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.fcff[0] > self.get_fib_bar('fcff') or
                           indicators.fcfe[0] > self.get_fib_bar('fcfe') or
                           indicators.fcff_ps[0] > self.get_fib_bar('fcff_ps') or
                           indicators.fcfe_ps[0] > self.get_fib_bar('fcfe_ps')
                      else 0)
            score += (1 if indicators.ocfps[0] > self.get_fib_bar('ocfps') else 0)
            score += (1 if indicators.sale_cash_ratio[0] > self.get_fib_bar('sale_cash_ratio') else 0)
            score += (1 if indicators.netprofit_cash_ratio[0] > self.get_fib_bar('netprofit_cash_ratio') else 0)
            score += (1 if indicators.ops_netprofit_rank[0] > self.get_fib_bar('ops_netprofit_rank') else 0)
            score += (1 if indicators.ops_cash_rank[0] > self.get_fib_bar('ops_cash_rank') else 0)
        return score

    def get_general_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.pb[0] > self.get_fib_bar('pb') else 0)
            score += (1 if indicators.bps[0] > self.get_fib_bar('bps') or
                           indicators.bps_yoy[0] > self.get_fib_bar('bps_yoy')
                      else 0)
            score += (1 if indicators.pcf[0] > self.get_fib_bar('pcf') else 0)
            score += (1 if indicators.op_profit_ratio[0] > self.get_fib_bar('op_profit_ratio') else 0)
            score += (1 if indicators.profit_cost_ratio[0] > self.get_fib_bar('profit_cost_ratio') else 0)
            score += (1 if indicators.eps[0] > self.get_fib_bar('eps') or
                           indicators.dt_eps[0] > self.get_fib_bar('dt_eps') or
                           indicators.diluted2_eps[0] > self.get_fib_bar('diluted2_eps') or
                           indicators.basic_eps_yoy[0] > self.get_fib_bar('basic_eps_yoy') or
                           indicators.dt_eps_yoy[0] > self.get_fib_bar('dt_eps_yoy')
                      else 0)
        return score

    def get_mr_tang_score(self, indicators):
        score = 0
        if not indicators.empty:
            score += (1 if indicators.t_total_revenue_yoy[0] > self.get_fib_bar('t_total_revenue_yoy') or
                           indicators.t_total_revenue_qoq[0] > self.get_fib_bar('t_total_revenue_qoq')
                      else 0)
            score += (1 if indicators.t_accounts_receiv_yoy[0] < self.get_fib_bar('t_accounts_receiv_yoy') or
                           indicators.t_accounts_receiv_qoq[0] < self.get_fib_bar('t_accounts_receiv_qoq')
                      else 0)
            score += (1 if indicators.t_prepay_revenue_ratio[0] < self.get_fib_bar('t_prepay_revenue_ratio') else 0)
            score += (1 if indicators.t_inventory_yoy[0] < self.get_fib_bar('t_inventory_yoy') or
                           indicators.t_inventory_qoq[0] < self.get_fib_bar('t_inventory_qoq')
                      else 0)
            score += (1 if indicators.t_invent_revenue_yoy[0] > self.get_fib_bar('t_invent_revenue_yoy') or
                           indicators.t_invent_revenue_qoq[0] > self.get_fib_bar('t_invent_revenue_qoq')
                      else 0)
            score += (1 if indicators.t_salary_yoy[0] > self.get_fib_bar('t_salary_yoy') or
                           indicators.t_salary_qoq[0] > self.get_fib_bar('t_salary_qoq')
                      else 0)
            score += (1 if indicators.t_surplus_cap_ratio[0] > self.get_fib_bar('t_surplus_cap_ratio') else 0)
            score += (1 if indicators.t_receive_total_assets[0] > self.get_fib_bar('t_receive_total_assets') else 0)
            score += (1 if indicators.t_grossprofit_margin_dt_yoy[0] > self.get_fib_bar('t_grossprofit_margin_dt_qoq') or
                           indicators.t_grossprofit_margin_dt_qoq[0] > self.get_fib_bar('t_grossprofit_margin_dt_qoq')
                      else 0)
            score += (1 if indicators.t_grossprofit_margin_yoy[0] > self.get_fib_bar('t_grossprofit_margin_yoy') or
                           indicators.t_grossprofit_margin_qoq[0] > self.get_fib_bar('t_grossprofit_margin_qoq')
                      else 0)
            score += (1 if indicators.t_netprofit_margin_dt_yoy[0] > self.get_fib_bar('t_netprofit_margin_dt_yoy') or
                           indicators.t_netprofit_margin_dt_qoq[0] > self.get_fib_bar('t_netprofit_margin_dt_qoq')
                      else 0)
            score += (1 if indicators.t_netprofit_margin_yoy[0] > self.get_fib_bar('t_netprofit_margin_yoy') or
                           indicators.t_netprofit_margin_qoq[0] > self.get_fib_bar('t_netprofit_margin_qoq')
                      else 0)
            score += (1 if indicators.t_act_amor_depr_ratio[0] > self.get_fib_bar('t_act_amor_depr_ratio') else 0)
            score += (1 if indicators.t_act_income_ratio[0] > self.get_fib_bar('t_act_income_ratio') else 0)
        return score

    def get_booming_score(self, indicators):
        score = 0
        #     寻找优质企业：1. 经营活动现金流净额 > 净利润 > 0
        #                 2. 销售商品、提供劳务收到的现金 >= 营业收入
        #                 3. 投资活动产生的现金流净额 < 0，且主要投入新项目
        #                 4. 现金及现金等价物净增加额 > 0，可放宽为分红因素
        #                 5. 期末现金及现金等价物余额 >= 有息负债
        score += (1 if indicators.eps[0] > 0 else 0)
        score += (1 if indicators.ocfps[0] > 0 else 0)
        score += (1 if indicators.netprofit_cash_ratio[0] > 1 else 0)
        score += (1 if indicators.t_act_income_ratio[0] > 1 else 0)
        score += (1 if indicators.sale_cash_ratio[0] > 1 else 0)
        score += (1 if indicators.liba_cash_ratio[0] > 1 else 0)
        # 0 < PEG < 1
        score += (1 if 0.6 < indicators.peg[0] < 1 else 0)
        score += (2 if 0 < indicators.peg[0] < 0.6 else 0)
        # 毛利率增加
        score += (1 if indicators.t_grossprofit_margin_dt_yoy[0] > 0 or
                       indicators.t_grossprofit_margin_dt_qoq[0] > 0
                  else 0)
        score += (2 if (indicators.t_grossprofit_margin_dt_yoy[0] > 0 and indicators.t_grossprofit_margin_yoy[0] > 1.1) or
                       (indicators.t_grossprofit_margin_dt_qoq[0] > 0 and indicators.t_grossprofit_margin_qoq[0] > 1.1)
                  else 0)
        # 净利率增加
        score += (1 if indicators.t_netprofit_margin_dt_yoy[0] > 0 or
                       indicators.t_netprofit_margin_dt_qoq[0] > 0
                  else 0)
        score += (2 if (indicators.t_netprofit_margin_dt_yoy[0] > 0 and indicators.t_netprofit_margin_yoy[0] > 1.1) or
                       (indicators.t_netprofit_margin_dt_qoq[0] > 0 and indicators.t_netprofit_margin_qoq[0] > 1.1)
                  else 0)
        # 近期股东人数大幅减小
        holder_num_change = indicators.holder_num / indicators.holder_num.shift(-1)
        score += (1 if len(holder_num_change[holder_num_change <= 0.9]) > 0 else 0)
        score += (1 if len(holder_num_change[holder_num_change <= 0.8]) > 0 else 0)
        score += (1 if len(holder_num_change[holder_num_change <= 0.7]) > 0 else 0)
        return score

    def get_anomaly_score(self, indicators):
        score = 0
        #  货比资金余额小于短期负载
        score += (1 if indicators.t_cash_st_debt_ratio[0] < 1 else 0)
        #  货比资金充裕，却借了很多有息负债
        score += (1 if indicators.t_cash_debt_ratio[0] < 1 else 0)
        #  定期存款和货币资金很多，流动资金却很少
        score += (1 if indicators.t_cash_depos_ff_ratio[0] > 5 else 0)
        #  应收账款增速大于销售增速
        score += (1 if indicators.t_accounts_receiv_yoy[0] > indicators.t_total_revenue_yoy[0] or
                       indicators.t_accounts_receiv_qoq[0] > indicators.t_total_revenue_qoq[0]
                  else 0)
        #  存货销售增速比大于1
        score += (1 if indicators.t_invent_revenue_yoy[0] > 1 or
                       indicators.t_invent_revenue_qoq[0] > 1
                  else 0)
        # 经营活动现金流量净额小于摊销折扣
        score += (1 if indicators.t_act_amor_depr_ratio[0] < 1 else 0)
        # 近期股东人数大幅增加
        holder_num_change = indicators.holder_num / indicators.holder_num.shift(-1)
        score += (1 if len(holder_num_change[holder_num_change >= 1.1]) > 0 else 0)
        score += (1 if len(holder_num_change[holder_num_change >= 1.2]) > 0 else 0)
        score += (1 if len(holder_num_change[holder_num_change >= 1.3]) > 0 else 0)
        return score
