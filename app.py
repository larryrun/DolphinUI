from flask import Flask, render_template, request
from Database.DatabaseClient import DatabaseClient
from Security.FIBBuilder import FIBBuilder
import datetime
import pandas as pd
import numpy as np

app = Flask(__name__)

database_client = DatabaseClient(dev=True)

global_cache_finance_indicators = {}


@app.template_filter('stock_code_normalizer')
def stock_code_normalizer(code):
    return code[-2:] + code[:-3]


@app.route('/vip/kewpie')
def vip_kewpie():
    start = datetime.date.today() + datetime.timedelta(days=45)
    end = start + datetime.timedelta(days=45)
    start = start.strftime('%Y-%m-%d')
    end = end.strftime('%Y-%m-%d')
    return render_template('vip/kewpie.html',
                           strategy_history_index_trend_begin=start,
                           strategy_history_index_trend_end=end)


@app.route('/vip/kewpie_zsqs', methods=['GET', 'POST'])
def vip_kewpie_zsqs():
    indexes_with_ranks = []
    if request.method == 'POST':
        date_from = request.form.get('strategy_history_index_trend_begin', '')
        date_to = request.form.get('strategy_history_index_trend_end', '')
        if date_from and date_to:
            date_from = pd.to_datetime(date_from)
            date_to = pd.to_datetime(date_to)
            indexes_with_ranks = get_indexes_trends(date_from, date_to)
    return render_template('dolphin/trends/indexes.html',
                           indexes=indexes_with_ranks)


@app.route('/vip/eric')
def vip_eric():
    date = datetime.date.today() + datetime.timedelta(days=-270)
    date = date.strftime('%Y-%m-%d')
    return render_template('vip/eric.html',
                           strategy_dbzt_window_start=date,
                           strategy_dbzt_min_up_pct=10,
                           strategy_dbzt_happen_in_days=30,
                           strategy_dblz_window_start=date,
                           strategy_dblz_min_up_pct=10,
                           strategy_dblz_happen_min_days=7,
                           strategy_dblz_happen_in_days=30)


@app.route('/vip/eric_dbzt', methods=['GET', 'POST'])
def vip_eric_dbzt():

    stocks = {}
    if request.method == 'POST':
        today = datetime.date.today()
        window_start = today + datetime.timedelta(days=-270)
        window_start = request.form.get('strategy_dbzt_window_start', window_start.strftime('%Y%m%d'))
        happen_start = request.form.get('strategy_dbzt_happen_in_days', '30')
        happen_start = today + datetime.timedelta(days=-int(happen_start))
        happen_start = happen_start.strftime('%Y%m%d')
        min_up_rate = int(request.form.get('strategy_dbzt_min_up_pct', 10)) / 100.0 + 1
        jbm_rank = request.form.get('strategy_dbzt_jbm_rank', 0)
        if not jbm_rank:
            jbm_rank = 0

        sql = """
        SELECT a.code, b.name, a.date, b.bar, a.change_pct 
        FROM DailyTrade a, (
            SELECT c.code, d.name, min(c.close) * %s as bar 
            FROM DailyTrade c, StockBasic d
            WHERE c.code = d.code AND d.list <= '%s' AND c.date >= '%s'
            GROUP BY c.code, d.name
        ) b
        WHERE a.code = b.code AND a.date > '%s' AND a.open < b.bar AND a.change_pct > 9.9
        ORDER BY a.date DESC, a.code;
        """ % (min_up_rate, window_start, window_start, happen_start)
        stocks_query = pd.read_sql_query(sql, database_client.get_engine())

        cached = {}
        stocks_aggregated = pd.DataFrame({'date': [], 'code': [], 'name': [], 'index': [], 'index_name': []})
        for code in stocks_query.code:
            if code not in cached:
                stock = stocks_query.query("code == '%s'" % code)
                ranked = False
                if jbm_rank > 0:
                    index, indexes, members = get_index_members_by_code(code)
                    if index:
                        member = members.query("code == '%s'" % code)
                        member = member.query("ranking1 <= %s or ranking2 <= %s" % (jbm_rank, jbm_rank))
                        if not member.empty:
                            ranked = True
                    else:
                        ranked = True
                if jbm_rank <= 0 or ranked:
                    aggregated = {
                        'date': stock.date.iat[0],
                        'code': stock.code.iat[0],
                        'name': stock.name.iat[0],
                        # 'index': index,
                        # 'index_name': indexes.name[index],
                    }
                    stocks_aggregated = stocks_aggregated.append(aggregated, ignore_index=True)
                cached[code] = True

        stocks = stocks_aggregated.to_dict(orient='records')

    return render_template('common/stocks_v2.html', description='底部涨停策略', stocks=stocks)


@app.route('/vip/eric_dblz', methods=['GET', 'POST'])
def vip_eric_dblz():

    stocks = {}
    if request.method == 'POST':
        today = datetime.date.today()
        window_start = today + datetime.timedelta(days=-270)
        window_start = request.form.get('strategy_dblz_window_start', window_start.strftime('%Y%m%d'))
        happen_start = request.form.get('strategy_dblz_happen_in_days', '30')
        happen_start = today + datetime.timedelta(days=-int(happen_start))
        happen_start = happen_start.strftime('%Y%m%d')
        happen_min_days = int(request.form.get('strategy_dblz_happen_min_days', 7))
        min_up_rate = int(request.form.get('strategy_dblz_min_up_pct', 10)) / 100.0 + 1
        jbm_rank = request.form.get('strategy_dblz_jbm_rank', 0)
        if not jbm_rank:
            jbm_rank = 0

        sql = """
        SELECT a.code, b.name, b.bar 
        FROM DailyTrade a, (
            SELECT c.code, d.name, min(c.close) * %s as bar 
            FROM DailyTrade c, StockBasic d
            WHERE c.code = d.code AND d.list <= '%s' AND c.date >= '%s'
            GROUP BY c.code, d.name
        ) b
        WHERE a.code = b.code AND a.date > '%s' AND a.open < b.bar
        GROUP BY a.code, b.name, b.bar
        ORDER BY a.code
        """ % (min_up_rate, window_start, window_start, happen_start)

        stocks_query = pd.read_sql_query(sql, database_client.get_engine())

        stocks_aggregated = pd.DataFrame()
        for _, row in stocks_query.iterrows():

            code = row['code']
            name = row['name']
            bar = row['bar']

            sql = """
            SELECT date, close, change_pct 
            FROM DailyTrade
            WHERE code = '%s' AND date >= '%s'
            ORDER BY date
            """ % (code, happen_start)

            stock_query = pd.read_sql_query(sql, database_client.get_engine())
            stock_query.set_index('date', inplace=True, drop=True)

            # 计算连涨天数

            base = pd.Series([np.sign(c) for c in stock_query.change_pct], stock_query.index)
            base = base.shift(-1, fill_value=0)
            zero = pd.Series(np.zeros(len(base)), base.index)
            periods = base
            period = base
            shift = 1
            while True:
                equals = (period == base.shift(-shift)) & (np.abs(periods) == shift)
                if stock_query[equals].empty:
                    break
                period = zero.copy()
                period[equals] = base[equals]
                periods = periods + period
                shift += 1

            # 查找低位连涨

            matches = stock_query[periods >= happen_min_days]
            matches = matches.query("close <= %d" % bar)
            if matches.empty:
                continue

            ranked = False
            if jbm_rank > 0:
                index, indexes, members = get_index_members_by_code(code)
                if index:
                    member = members.query("code == '%s'" % code)
                    member = member.query("ranking1 >= %s or ranking2 >= %s" % (jbm_rank, jbm_rank))
                    # member = members.query("code == '%s' and ranking1 >= %s or ranking2 >= %s" % (code, jbm_rank, jbm_rank))
                    if not member.empty:
                        ranked = True
                else:
                    ranked = True
            if jbm_rank <= 0 or ranked:
                aggregated = {
                    'date': matches.index[0],
                    'code': code,
                    'name': name,
                    #'index': index,
                    #'index_name': indexes.name[index],
                }
                stocks_aggregated = stocks_aggregated.append(aggregated, ignore_index=True)

        stocks = stocks_aggregated.to_dict(orient='records')

    return render_template('common/stocks_v2.html', description='底部连涨策略', stocks=stocks)


@app.route('/vip/dshj')
def vip_dshj():
    date = datetime.date.today() + datetime.timedelta(days=-1)
    date = date.strftime('%Y-%m-%d')
    return render_template('vip/dshj.html',
                           strategy_cci_date=date,
                           strategy_cci_daily='on',
                           strategy_cci_daily_min=100,
                           strategy_cci_daily_max=380,
                           strategy_cci_weekly='on',
                           strategy_cci_weekly_min=150,
                           strategy_cci_weekly_max=380,
                           strategy_cci_monthly='on',
                           strategy_cci_monthly_min=100,
                           strategy_cci_monthly_max=380,
                           strategy_cci_quarterly='on',
                           strategy_cci_quarterly_min=100,
                           strategy_cci_quarterly_max=380,
                           roe='on',
                           roe_min=5,
                           roe_max='',
                           tr_yoy='on',
                           tr_yoy_min=5,
                           tr_yoy_max='',
                           netprofit_yoy='on',
                           netprofit_yoy_min=5,
                           netprofit_yoy_max='',
                           exclude_st='on',
                           exclude_suspended='on')


@app.route('/vip/dshj_cci', methods=['GET', 'POST'])
def vip_dshj_cci():

    stocks = {}

    if request.method == 'POST':

        daily_cci = ("on" == request.form.get('strategy_cci_daily', ""))
        daily_cci_min = request.form.get('strategy_cci_daily_min', None)
        daily_cci_max = request.form.get('strategy_cci_daily_max', None)
        weekly_cci = ("on" == request.form.get('strategy_cci_weekly', ""))
        weekly_cci_min = request.form.get('strategy_cci_weekly_min', None)
        weekly_cci_max = request.form.get('strategy_cci_weekly_max', None)
        monthly_cci = ("on" == request.form.get('strategy_cci_monthly', ""))
        monthly_cci_min = request.form.get('strategy_cci_monthly_min', None)
        monthly_cci_max = request.form.get('strategy_cci_monthly_max', None)
        quarterly_cci = ("on" == request.form.get('strategy_cci_quarterly', ""))
        quarterly_cci_min = request.form.get('strategy_cci_quarterly_min', None)
        quarterly_cci_max = request.form.get('strategy_cci_quarterly_max', None)

        roe = ("on" == request.form.get('roe', ""))
        roe_min = request.form.get('roe_min', None)
        roe_max = request.form.get('roe_max', None)
        tr_yoy = ("on" == request.form.get('tr_yoy', ""))
        tr_yoy_min = request.form.get('tr_yoy_min', None)
        tr_yoy_max = request.form.get('tr_yoy_max', None)
        netprofit_yoy = ("on" == request.form.get('netprofit_yoy', ""))
        netprofit_yoy_min = request.form.get('netprofit_yoy_min', None)
        netprofit_yoy_max = request.form.get('netprofit_yoy_max', None)

        exclude_st = ("on" == request.form.get('exclude_st', ""))
        exclude_suspended = ("on" == request.form.get('exclude_suspended', ""))

        all_satisfy = ("yes" == request.form.get('strategy_cci_all_satisfy', ""))
        start_date = request.form.get('strategy_cci_date', None)

        audit_event = pd.DataFrame({'date': [pd.datetime.today().strftime("%Y-%m-%d %H:%M:%S")],
                                    'ip': [request.remote_addr],
                                    'event': [str({
                                        'url': '/vip/dshj_cci',
                                        'daily_cci': daily_cci,
                                        'daily_cci_min': daily_cci_min,
                                        'daily_cci_max': daily_cci_max,
                                        'weekly_cci': weekly_cci,
                                        'weekly_cci_min': weekly_cci_min,
                                        'weekly_cci_max': weekly_cci_max,
                                        'monthly_cci': monthly_cci,
                                        'monthly_cci_min': monthly_cci_min,
                                        'monthly_cci_max': monthly_cci_max,
                                        'quarterly_cci': quarterly_cci,
                                        'quarterly_cci_min': quarterly_cci_min,
                                        'quarterly_cci_max': quarterly_cci_max,
                                        'roe': roe,
                                        'roe_min': roe_min,
                                        'roe_max': roe_max,
                                        'tr_yoy': tr_yoy,
                                        'tr_yoy_min': tr_yoy_min,
                                        'tr_yoy_max': tr_yoy_max,
                                        'netprofit_yoy': netprofit_yoy,
                                        'netprofit_yoy_min': netprofit_yoy_min,
                                        'netprofit_yoy_max': netprofit_yoy_max,
                                        'exclude_st': exclude_st,
                                        'exclude_suspended': exclude_suspended,
                                        'all_satisfy': all_satisfy,
                                        'start_date': start_date
                                    })]})
        database_client.write_audit_event(audit_event)

        if start_date:

            def end_of_week(date):
                return date + datetime.timedelta(days=6-date.weekday())

            def end_of_month(date):
                if date.month in [1, 3, 5, 7, 8, 10, 12]:
                    return datetime.date(date.year, date.month, 31)
                elif date.month in [4, 6, 9, 11]:
                    return datetime.date(date.year, date.month, 30)
                else:
                    return datetime.date(date.year, date.month, 29 if date.year % 4 == 0 else 28)

            def end_of_quarter(date):
                if date.month <= 3:
                    return datetime.date(date.year, 3, 31)
                elif date.month <= 6:
                    return datetime.date(date.year, 6, 30)
                elif date.month <= 9:
                    return datetime.date(date.year, 9, 30)
                else:
                    return datetime.date(date.year, 12, 31)

            def min_max_range(min, max):
                if min and max:
                    return (min, max)
                elif min:
                    return (min, 1000000000)
                elif max:
                    return (-1000000000, max)
                else:
                    return None

            strategies = [
                (daily_cci,     'Daily_CCI_14',     daily_cci_min,     daily_cci_max,     end_of_week),
                (weekly_cci,    'Weekly_CCI_14',    weekly_cci_min,    weekly_cci_max,    end_of_month),
                (monthly_cci,   'Monthly_CCI_14',   monthly_cci_min,   monthly_cci_max,   end_of_quarter),
                (quarterly_cci, 'Quarterly_CCI_14', quarterly_cci_min, quarterly_cci_max, None)
            ]

            date = datetime.date.today() + datetime.timedelta(days=-365)
            sql = "SELECT code, name FROM Dolphin.StockBasic WHERE list >= '%s'" % (date.strftime('%Y%m%d'))
            sub_monthly_new_stocks = pd.read_sql_query(sql, database_client.get_engine())
            sub_monthly_new_stocks.set_index('code', inplace=True)

            date = datetime.date.today() + datetime.timedelta(days=-1278)
            sql = "SELECT code, name FROM Dolphin.StockBasic WHERE list >= '%s'" % (date.strftime('%Y%m%d'))
            sub_quarterly_new_stocks = pd.read_sql_query(sql, database_client.get_engine())
            sub_quarterly_new_stocks_copy = sub_quarterly_new_stocks.copy()
            sub_quarterly_new_stocks.set_index('code', inplace=True)

            exclude_stocks = pd.DataFrame()
            sql_not_suspended = " STATUS = 'L' " if exclude_suspended else " (1 = 1) "
            sql_not_st = """ NAME NOT LIKE '%%ST%%' """ if exclude_st else " (1 = 1) "
            sql = "SELECT code, name FROM Dolphin.StockBasic WHERE %s AND %s " % (sql_not_st, sql_not_suspended)
            if exclude_st or exclude_suspended:
                exclude_stocks = pd.read_sql_query(sql, database_client.get_engine())
                exclude_stocks.set_index('code', inplace=True)

            basic_indicators_stocks = pd.DataFrame()
            basic_indicators = roe or tr_yoy or netprofit_yoy
            if basic_indicators:
                sql = "SELECT a.code, a.end_date, a.roe, a.tr_yoy, a.netprofit_yoy " \
                      "FROM Dolphin.FinanceIndicators a, " \
                      "  (SELECT code, max(end_date) AS end_date FROM Dolphin.FinanceIndicators GROUP BY CODE) AS b " \
                      "WHERE a.code = b.code AND a.end_date = b.end_date"
                basic_indicators_stocks = pd.read_sql_query(sql, database_client.get_engine())
                basic_indicators_stocks.set_index('code', inplace=True, drop=True)
                if roe:
                    if roe_min:
                        basic_indicators_stocks = basic_indicators_stocks.query("roe >= %s" % roe_min)
                    if roe_max:
                        basic_indicators_stocks = basic_indicators_stocks.query("roe <= %s" % roe_max)
                if tr_yoy:
                    if tr_yoy_min:
                        basic_indicators_stocks = basic_indicators_stocks.query("tr_yoy >= %s" % tr_yoy_min)
                    if tr_yoy_max:
                        basic_indicators_stocks = basic_indicators_stocks.query("tr_yoy <= %s" % tr_yoy_max)
                if netprofit_yoy:
                    if netprofit_yoy_min:
                        basic_indicators_stocks = basic_indicators_stocks.query("netprofit_yoy >= %s" % netprofit_yoy_min)
                    if netprofit_yoy_max:
                        basic_indicators_stocks = basic_indicators_stocks.query("netprofit_yoy <= %s" % netprofit_yoy_max)
                if basic_indicators_stocks.empty:
                    return render_template('common/stocks.html', stocks=stocks)

            successful = True
            start_date = pd.to_datetime(start_date).strftime('%Y%m%d')
            stocks_query = pd.DataFrame({'code': [], 'name': [], 'date': []})
            template_sql = "SELECT a.code, b.name, a.date FROM TradeIndicator a, StockBasic b WHERE a.name = '%s' AND a.date >= '%s' AND a.value >= %s AND a.value <= %s AND a.code = b.code"
            for (apply, metric, min, max, date_converter) in strategies:
                if apply:
                    range = min_max_range(min, max)
                    if range:
                        sql = template_sql % (metric, start_date, range[0], range[1])
                        query = pd.read_sql_query(sql, database_client.get_engine())
                        if 'Monthly_CCI_14' == metric and not stocks_query.empty:
                            stocks_query_copy = stocks_query.copy()
                            stocks_query_copy.set_index('code', inplace=True)
                            stocks_joint = stocks_query_copy.join(sub_monthly_new_stocks, how="inner", rsuffix='_r')
                            stocks_joint.reset_index(inplace=True)
                            if not stocks_joint.empty:
                                query = query.append(stocks_joint[['code', 'name', 'date']])
                        if 'Quarterly_CCI_14' == metric and not stocks_query.empty:
                            stocks_query_copy = stocks_query.copy()
                            stocks_query_copy.set_index('code', inplace=True)
                            stocks_joint = stocks_query_copy.join(sub_quarterly_new_stocks, how="inner", rsuffix='_r')
                            stocks_joint.reset_index(inplace=True)
                            if not stocks_joint.empty:
                                query = query.append(stocks_joint[['code', 'name', 'date']])
                        if query.empty and all_satisfy:
                            successful = False
                            break
                        if not stocks_query.empty and all_satisfy:
                            query.set_index(query.apply(lambda row: "%s_%s" % (row[0], row[2].strftime('%Y%m%d')), axis=1), inplace=True)
                            stocks_query.set_index(stocks_query.apply(lambda row: "%s_%s" % (row[0], row[2].strftime('%Y%m%d')), axis=1), inplace=True)
                            stocks_joint = stocks_query.join(query, how="inner", rsuffix='_r')
                            if stocks_joint.empty:
                                successful = False
                                break
                            stocks_query = stocks_joint[['code', 'name', 'date']]
                        else:
                            stocks_query = stocks_query.append(query)
                        if date_converter:
                            stocks_query['date'] = stocks_query.date.apply(lambda x: date_converter(x))
                            stocks_query = stocks_query.drop_duplicates()

            if successful:
                stocks_query = stocks_query[['code', 'name']]
                stocks_query = stocks_query.drop_duplicates()
                if not exclude_stocks.empty:
                    stocks_query = stocks_query.join(exclude_stocks, on='code', how='inner', rsuffix='_r')
                    stocks_query = stocks_query[['code', 'name']]
                if basic_indicators:
                    stocks_query = stocks_query.join(basic_indicators_stocks, on='code', how='inner')
                    stocks_query = stocks_query[['code', 'name']]
                stocks = stocks_query.to_dict(orient='records')

    return render_template('common/stocks.html', stocks=stocks)


@app.route('/dolphin/trends/stocks', methods=['GET'])
def get_stocks_trends():
    sql = "SELECT code, name FROM StockBasic WHERE STATUS = 'L'"
    stocks = pd.read_sql_query(sql, database_client.get_engine())
    stocks.set_index('code', drop=True, inplace=True)
    date_from = pd.datetime.today()
    days = int(request.args.get('days', '45'))
    date_to = pd.datetime.today() + pd.Timedelta(days=days)
    code = request.args.get('code', '')
    trend = get_history_trend('DailyTrade', code=code, date_from=date_from.strftime('%m%d'), date_to=date_to.strftime('%m%d'))
    years = int(request.args.get('years', '6'))
    trend = trend.query("years >= %s" % years)[:100]
    trend['name'] = stocks['name']
    trend['trend'] = trend.apply(lambda x: "参考历史%d年的同期表现，未来%s天, 胜率: %.2f%%, 涨幅（均）: %.2f%%, 方差: %.2f%%, 高位（均）: %.2f%%, 低位（均）: %.2f%%" %
                                           (x.years, days, x.win_rate, x.win_pct_mean, x.win_pct_std, x.win_max_mean, x.win_min_mean),
                                 axis=1)
    trend.reset_index(inplace=True)
    stocks = trend.to_dict(orient='records')
    return render_template('dolphin/trends/stocks.html',
                           days=days,
                           years=years,
                           stocks=stocks)


@app.route('/dolphin/trends/indexes', methods=['GET'])
def dolphin_trends_indexes():
    sql = "SELECT code, name FROM IndexBasic WHERE MARKET = 'SW' AND CATEGORY = '二级行业指数'"
    indexes = pd.read_sql_query(sql, database_client.get_engine())
    indexes.set_index('code', drop=True, inplace=True)
    date_from = pd.datetime.today()
    days = int(request.args.get('days', '45'))
    date_to = pd.datetime.today() + pd.Timedelta(days=days)
    trend = get_history_trend('WeeklyIndex', code=None, date_from=date_from.strftime('%m%d'), date_to=date_to.strftime('%m%d'))
    date_from = pd.datetime.today() + pd.Timedelta(days=-92 * 6)
    index_members_with_rank = get_index_members_with_rank(indexes.index, date_from=date_from)
    indexes_with_ranks = []
    for index in trend.index:
        if index in indexes.index:
            trend_ = trend.query("code == '%s'" % index)
            index_with_ranks = {
                'code': index,
                'name': indexes.name[index],
                'trend': '参考历史%d年的同期表现，未来%s天, 胜率: %.2f%%, 涨幅（均）: %.2f%%, 方差: %.2f%%, 高位（均）: %.2f%%, 低位（均）: %.2f%%' %
                         (trend_['years'].iat[0], days, trend_['win_rate'].iat[0], trend_['win_pct_mean'].iat[0],
                          trend_['win_pct_std'].iat[0], trend_['win_max_mean'].iat[0], trend_['win_min_mean'].iat[0])
            }
            if index in index_members_with_rank:
                narrow_rank = index_members_with_rank[index]['narrow_spectrum_rank']
                broad_rank = index_members_with_rank[index]['broad_spectrum_rank']
                rank = set([x['code'] for x in narrow_rank[:2]]) | set([x['code'] for x in broad_rank[:2]]) | \
                      (set([x['code'] for x in narrow_rank[:5]]) & set([x['code'] for x in broad_rank[:5]]))
                index_with_ranks['rank'] = rank
            indexes_with_ranks.append(index_with_ranks)
    return render_template('dolphin/trends/indexes.html',
                           indexes=indexes_with_ranks)


def get_indexes_trends(date_from, date_to):
    sql = "SELECT code, name FROM IndexBasic WHERE MARKET = 'SW' AND CATEGORY = '二级行业指数'"
    indexes = pd.read_sql_query(sql, database_client.get_engine())
    indexes.set_index('code', drop=True, inplace=True)
    date_from_str = date_from.strftime('%m%d')
    date_to_str = date_to.strftime('%m%d')
    trend = get_history_trend('WeeklyIndex', code=None, date_from=date_from_str, date_to=date_to_str)
    date_from = pd.datetime.today() + pd.Timedelta(days=-92 * 6)
    index_members_with_rank = get_index_members_with_rank(indexes.index, date_from=date_from)
    indexes_with_ranks = []
    for index in trend.index:
        if index in indexes.index:
            trend_ = trend.query("code == '%s'" % index)
            index_with_ranks = {
                'code': index,
                'name': indexes.name[index],
                'trend': '参考历史%d年的同期（%s ~ %s）表现, 胜率: %.2f%%, 涨幅（均）: %.2f%%, 方差: %.2f%%, 高位（均）: %.2f%%, 低位（均）: %.2f%%' %
                         (trend_['years'].iat[0], date_from_str, date_to_str, trend_['win_rate'].iat[0], trend_['win_pct_mean'].iat[0],
                          trend_['win_pct_std'].iat[0], trend_['win_max_mean'].iat[0], trend_['win_min_mean'].iat[0])
            }
            if index in index_members_with_rank:
                narrow_rank = index_members_with_rank[index]['narrow_spectrum_rank']
                broad_rank = index_members_with_rank[index]['broad_spectrum_rank']
                rank = set([x['code'] for x in narrow_rank[:2]]) | set([x['code'] for x in broad_rank[:2]]) | \
                      (set([x['code'] for x in narrow_rank[:5]]) & set([x['code'] for x in broad_rank[:5]]))
                index_with_ranks['rank'] = rank
            indexes_with_ranks.append(index_with_ranks)
    return indexes_with_ranks


def get_history_trend(table, code, date_from, date_to, year_from=None):

    match_date_from = "DATE_FORMAT(date,'%%m%%d') >= '" + date_from + "'" if date_from else '1 = 1'
    match_date_to = "DATE_FORMAT(date,'%%m%%d') <= '" + date_to + "'" if date_to else '1 = 1'
    match_code = "code = '%s'" % code if code else '1 = 1'
    match_year_from = '%s0101' % year_from if year_from else '20120101'

    sql = "SELECT code, YEAR(date) as year, DATE_FORMAT(date,'%%m%%d') as month_date, open, high, low, close, pre_close "
    sql += "FROM %s WHERE date >= '%s' AND %s AND %s AND %s " % (table, match_year_from, match_date_from, match_date_to, match_code)
    sql += "ORDER BY code, year DESC, month_date DESC "

    engine = database_client.get_engine()

    data = pd.read_sql_query(sql, engine)
    group_by = data.groupby(by=['code', 'year'])
    aggregated_by_code_year = pd.DataFrame({})
    for code, year in group_by.groups:
        print('Aggregation at %s-%s' % (code, year))
        group = group_by.get_group((code, year))
        aggregated = {
            'id': '%s-%d' % (code, year),
            'code': code,
            'year': year,
            'open': group.open.iat[-1],
            'high': group.high.max(),
            'low': group.low.min(),
            'close': group.close.iat[0],
            'pre_close': group.pre_close.iat[-1]
        }
        aggregated_by_code_year = aggregated_by_code_year.append(aggregated, ignore_index=True)
    aggregated_by_code_year['pre_close'].replace(0, aggregated_by_code_year['open'], inplace=True)
    aggregated_by_code_year.set_index('id', drop=True, inplace=True)
    aggregated_by_code_year['change_pct'] = (aggregated_by_code_year.close / aggregated_by_code_year.pre_close - 1) * 100.0
    aggregated_by_code_year['change_max'] = (aggregated_by_code_year.high / aggregated_by_code_year.open - 1) * 100.0
    aggregated_by_code_year['change_min'] = (aggregated_by_code_year.low / aggregated_by_code_year.open - 1) * 100.0
    print(aggregated_by_code_year)

    aggregated_by_code = pd.DataFrame({})
    group_by = aggregated_by_code_year.groupby(by=['code'])
    for code in group_by.groups:
        print('Aggregation at %s' % code)
        group = group_by.get_group(code)
        years = len(group)
        aggregated = {
            'code': code,
            'years': years,
            'win_rate': len(group.change_pct[group.change_pct > 0]) * 100.0 / years,
            'win_pct_mean': group.change_pct.mean(),
            'win_pct_std': group.change_pct.std(),
            'win_max_mean': group.change_max.mean(),
            'win_min_mean': group.change_min.mean()
        }
        aggregated_by_code = aggregated_by_code.append(aggregated, ignore_index=True)
    aggregated_by_code.sort_values(['win_rate', 'years', 'win_pct_mean', 'win_max_mean'], ascending=False, inplace=True)
    aggregated_by_code.set_index('code', drop=True, inplace=True)
    print(aggregated_by_code)

    return aggregated_by_code


@app.route('/dolphin/indexes/members', methods=['GET'])
def get_index_members():
    index = request.args.get('index', '')
    if index:
        _, indexes, members = get_index_members_by_index(index)
        return render_template('dolphin/indexes/members.html',
                               index={'code': index, 'name': indexes.name[index]},
                               members=members.to_dict(orient='records'))
    code = request.args.get('code', '')
    if code:
        index, indexes, members = get_index_members_by_code(code)
        if index:
            member = members.query("code == '%s'" % code)
            return render_template('dolphin/indexes/members.html',
                                   index={'code': index, 'name': indexes.name[index]},
                                   stock={'code': code, 'ranking1': member.ranking1.iat[0], 'ranking2': member.ranking2.iat[0]},
                                   members=members.to_dict(orient='records'))
    return render_template('common/404.html')


def get_index_members_by_index(index):
    sql = "SELECT code, name FROM IndexBasic WHERE CODE = '%s'" % index
    indexes = pd.read_sql_query(sql, database_client.get_engine())
    indexes.set_index('code', drop=True, inplace=True)
    sql = "SELECT code, name FROM StockBasic WHERE CODE in ( SELECT con_code FROM IndexMember WHERE INDEX_CODE = '%s' )" % index
    stock_basics = pd.read_sql_query(sql, database_client.get_engine())
    stock_basics.set_index('code', drop=True, inplace=True)
    date_from = pd.datetime.today() + pd.Timedelta(days=-92 * 6)
    index_members_with_rank = get_index_members_with_rank([index], date_from=date_from)
    members = get_index_members_helper(index, index_members_with_rank, stock_basics)
    return index, indexes, members


def get_index_members_by_code(code):
    sql = """SELECT a.index_code as code, a.index_name as name
             FROM IndexMember a, IndexBasic b 
             WHERE a.index_code = b.code AND b.market = 'SW' 
             AND b.category = '二级行业指数' AND a.CON_CODE = '%s'""" % code
    indexes = pd.read_sql_query(sql, database_client.get_engine())
    indexes.set_index('code', drop=True, inplace=True)
    if indexes.empty:
        return None, None, None
    index = indexes.index[0]
    sql = "SELECT code, name FROM StockBasic WHERE CODE in ( SELECT con_code FROM IndexMember WHERE INDEX_CODE = '%s' )" % index
    stock_basics = pd.read_sql_query(sql, database_client.get_engine())
    stock_basics.set_index('code', drop=True, inplace=True)
    date_from = pd.datetime.today() + pd.Timedelta(days=-92 * 6)
    index_members_with_rank = get_index_members_with_rank([index], date_from=date_from)
    members = get_index_members_helper(index, index_members_with_rank, stock_basics)
    return index, indexes, members


def get_index_members_helper(index, index_members, stocks):
    broad_rank = {}
    for rank in index_members[index]['broad_spectrum_rank']:
        broad_rank[rank['code']] = rank['ranking']
    members = pd.DataFrame()
    for narrow_rank in index_members[index]['narrow_spectrum_rank']:
        code = narrow_rank['code']
        name = stocks.name[code]
        ranking1 = narrow_rank['Ranking']
        del narrow_rank['Ranking']
        del narrow_rank['code']
        ranking2 = broad_rank[code]
        member = {
            'code': code,
            'name': name,
            'score': narrow_rank,
            'ranking1': ranking1,
            'ranking2': ranking2
        }
        members = members.append(member, ignore_index=True)
    return members


def get_index_members_with_rank(index_codes, date_from, date_to=None, weights={'Anomaly': -1}):

    full_indicators = get_finance_indicators(date_from=date_from, date_to=date_to)
    head_indicators = full_indicators.groupby('code').head(1)
    head_indicators.set_index('code', drop=True, inplace=True)

    index_members_with_rank = {}
    for index_code in index_codes:

        index_members_with_rank[index_code] = {}

        # 窄谱排名：相对于行业基本面中值的相对强弱

        sql = "SELECT con_code as code, 1 as dummy FROM IndexMember WHERE INDEX_CODE = '%s'" % index_code
        codes = pd.read_sql_query(sql, database_client.get_engine())
        codes.set_index('code', drop=True, inplace=True)
        codes_indicators = head_indicators.join(codes, how='inner', lsuffix='_l', rsuffix='_r')
        codes_indicators.drop(columns=['dummy'], inplace=True)
        median = codes_indicators.apply(lambda x: x[x > 0].median(), axis=0).fillna(0)
        fib_bar = pd.DataFrame({'median': median}).T.to_dict(orient='records')[0]
        print(fib_bar)
        narrow_spectrum_rank = pd.DataFrame({})
        codes = codes.index
        for code in codes:
            code_indicators = full_indicators.query("code == '%s'" % code)
            if code_indicators.empty:
                continue
            code_indicators.reset_index(drop=True, inplace=True)
            score = FIBBuilder(fib_bar).build(code_indicators)
            summary = 0
            for key in score:
                weight = weights[key] if key in weights else 1
                summary += score[key] * weight
            score['code'] = code
            score['Summary'] = summary
            print(score)
            narrow_spectrum_rank = narrow_spectrum_rank.append(score, ignore_index=True)
        narrow_spectrum_rank['Ranking'] = (-narrow_spectrum_rank['Summary']).argsort().argsort() + 1
        narrow_spectrum_rank.sort_values(by=['Ranking'], ascending=True, inplace=True)
        narrow_spectrum_rank.reset_index(drop=True, inplace=True)
        print(narrow_spectrum_rank)

        # 广谱排名：相对于行业成分股基本面的绝对强弱

        sql = """ SELECT a.code, a.pe, a.ps, a.pb 
                  FROM DailyBasic a, ( 
                    SELECT code, max(date) date 
                    FROM DailyBasic 
                    WHERE code in ( 
                      %s 
                    ) 
                    GROUP BY code
                  ) b 
                  WHERE a.code = b.code AND a.date = b.date 
                  ORDER BY a.code, a.date DESC """ % (','.join("'%s'" % code for code in codes))
        basic = pd.read_sql_query(sql, database_client.get_engine())
        basic.set_index('code', drop=True, inplace=True)
        codes_indicators['pe'] = basic.pe
        codes_indicators['pb'] = basic.pb
        codes_indicators['ps'] = basic.ps

        order_by = {
            # 通用财务指标
            'pe': 'asc', 'pb': 'asc', 'ps': 'asc', 'total_revenue_ps': 'desc', 'revenue_ps': 'desc',
            # 偿债能力指标
            'current_ratio': 'desc', 'quick_ratio': 'desc', 'liba_cash_ratio': 'desc', 'total_asset_liab_ratio': 'desc',
            'ebit_fin_ratio': 'desc', 'fcff_int_ratio': 'desc',
            # 运营能力指标
            'ar_turn': 'desc', 'inv_turn': 'desc', 'ca_turn': 'desc', 'assets_turn': 'desc', 'roic': 'desc',
            # 盈利能力指标
            'grossprofit_margin': 'desc', 'netprofit_margin': 'desc', 'roa_yearly': 'desc', 'roa_dp': 'desc',
            'roe': 'desc', 'roe_waa': 'desc', 'roe_dt': 'desc', 'roe_yearly': 'desc', 'roe_yoy': 'desc', 'q_roe': 'desc',
            'eps_dv_ratio_ratio': 'desc', 'dv_ratio': 'desc',
            # 发展能力指标
            'or_yoy': 'desc', 'q_sales_yoy': 'desc', 'netprofit_yoy': 'desc', 'dt_netprofit_yoy': 'desc',
            'assets_yoy': 'desc', 'eqt_yoy': 'desc', 'peg': 'asc',
            # 现金流量指标
            'fcff_ps': 'desc', 'fcfe_ps': 'desc', 'ocfps': 'desc', 'sale_cash_ratio': 'desc',
            'netprofit_cash_ratio': 'desc', 'ops_netprofit_rank': 'desc', 'ops_cash_rank': 'desc',
            # 综合绩效指标
            'bps': 'desc', 'bps_yoy': 'desc', 'pcf': 'desc', 'op_profit_ratio': 'desc', 'profit_cost_ratio': 'desc',
            'diluted2_eps': 'desc', 'basic_eps_yoy': 'desc', 'dt_eps_yoy': 'desc',
            # 老唐财务指标
            't_cash_st_debt_ratio': 'desc', 't_cash_debt_ratio': 'desc', 't_cash_depos_ff_ratio': 'desc',
            't_total_revenue_yoy': 'desc', 't_total_revenue_qoq': 'desc', 't_accounts_receiv_yoy': 'asc',
            't_accounts_receiv_qoq': 'asc', 't_prepay_revenue_ratio': 'desc', 't_inventory_yoy': 'asc',
            't_inventory_qoq': 'asc', 't_invent_revenue_yoy': 'asc', 't_invent_revenue_qoq': 'asc',
            't_salary_qoq': 'desc', 't_salary_yoy': 'desc', 't_surplus_cap_ratio': 'desc',
            't_ebit_prod_assets_ratio': 'desc', 't_receive_total_assets': 'asc', 't_grossprofit_margin_dt_yoy': 'desc',
            't_grossprofit_margin_dt_qoq': 'desc', 't_grossprofit_margin_yoy': 'desc', 't_grossprofit_margin_qoq': 'desc',
            't_netprofit_margin_dt_yoy': 'desc', 't_netprofit_margin_dt_qoq': 'desc', 't_netprofit_margin_yoy': 'desc',
            't_netprofit_margin_qoq': 'desc', 't_act_amor_depr_ratio': 'desc', 't_act_income_ratio': 'desc',
        }

        broad_spectrum_rank = pd.DataFrame({})
        for column in codes_indicators.columns:
            if column in order_by:
                if order_by[column] == 'desc':
                    broad_spectrum_rank[column] = (-codes_indicators[column]).argsort().argsort() + 1
                else:
                    broad_spectrum_rank[column] = codes_indicators[column].argsort().argsort() + 1
        broad_spectrum_rank['score'] = broad_spectrum_rank.apply(lambda x: x.sum(), axis=1)
        broad_spectrum_rank['ranking'] = broad_spectrum_rank['score'].argsort().argsort() + 1
        broad_spectrum_rank.sort_values(by=['ranking'], ascending=True, inplace=True)
        broad_spectrum_rank.reset_index(inplace=True)
        print(broad_spectrum_rank)

        index_members_with_rank[index_code] = {
            'narrow_spectrum_rank': narrow_spectrum_rank.to_dict(orient='records'),
            'broad_spectrum_rank': broad_spectrum_rank.to_dict(orient='records')
        }

    return index_members_with_rank


def get_finance_indicators(date_from=None, date_to=None):

    sql = """ SELECT a.code, 
                   # 通用财务指标
                   a.total_revenue_ps, a.revenue_ps, a.capital_rese_ps, a.surplus_rese_ps, a.undist_profit_ps, a.gross_margin, 
                   # 偿债能力指标
                   a.current_ratio, a.quick_ratio, b.liba_cash_ratio, b.total_asset_liab_ratio, b.ebit_fin_ratio, b.fcff_int_ratio, a.debt_to_eqt, a.assets_to_eqt, a.dp_assets_to_eqt,
                   # 运营能力指标
                   a.ar_turn, b.inv_turn, a.ca_turn, a.assets_turn, a.roic,
                   # 盈利能力指标
                   a.grossprofit_margin, a.netprofit_margin, a.roa_yearly, a.roa_dp, a.roe, a.roe_waa, a.roe_dt, a.roe_yearly, a.roe_yoy, q_roe, 
                   b.eps_dv_ratio_ratio, b.dv_ratio, b.pe, b.ps, b.netprofit,
                   # 发展能力指标
                   a.or_yoy, a.q_sales_yoy, a.netprofit_yoy, a.dt_netprofit_yoy, a.assets_yoy, a.eqt_yoy, b.peg,
                   # 现金流量指标
                   a.fcff, a.fcfe, a.fcff_ps, a.fcfe_ps, a.ocfps, b.sale_cash_ratio, b.netprofit_cash_ratio, b.ops_netprofit_rank, b.ops_cash_rank,
                   # 综合绩效指标
                   b.pb, a.bps, a.bps_yoy, b.pcf, b.op_profit_ratio, b.profit_cost_ratio, a.eps, a.dt_eps, a.diluted2_eps, a.basic_eps_yoy, a.dt_eps_yoy, 
                   # 股东人数
                   b.holder_num,
                   # 老唐财务指标
                   b.t_cash_st_debt_ratio, b.t_cash_debt_ratio, b.t_cash_depos_ff_ratio, b.t_total_revenue_yoy, b.t_total_revenue_qoq, 
                   b.t_accounts_receiv_yoy, b.t_accounts_receiv_qoq, b.t_prepay_revenue_ratio, b.t_inventory_yoy, b.t_inventory_qoq, 
                   b.t_invent_revenue_yoy, b.t_invent_revenue_qoq, b.t_salary_yoy, b.t_salary_qoq, b.t_surplus_cap_ratio, b.t_ebit_prod_assets_ratio, 
                   b.t_receive_total_assets, b.t_grossprofit_margin_dt_yoy, b.t_grossprofit_margin_dt_qoq, b.t_grossprofit_margin_yoy, b.t_grossprofit_margin_qoq, 
                   b.t_netprofit_margin_dt_yoy, b.t_netprofit_margin_dt_qoq, b.t_netprofit_margin_yoy, b.t_netprofit_margin_qoq, b.t_act_amor_depr_ratio, 
                   b.t_act_income_ratio, b.t_company_portrait
            FROM FinanceIndicators a, FinanceIndicatorsBoost b
            WHERE a.code = b.code AND a.end_date = b.date AND %s AND %s ORDER BY a.code, a.end_date DESC """

    match_date_from = "a.end_date >= '%s'" % date_from.strftime("%Y%m%d") if date_from else "1 = 1"
    match_date_to = "a.end_date <= '%s'" % date_to.strftime("%Y%m%d") if date_to else "1 = 1"

    key = "(%s)_(%s)" % (match_date_from, match_date_to)
    if key in global_cache_finance_indicators:
        cache = global_cache_finance_indicators[key]
        delta = pd.datetime.now() - cache['datetime']
        if delta.days < 1:
            return cache['indicators']

    sql = sql % (match_date_from, match_date_to)
    indicators = pd.read_sql_query(sql, database_client.get_engine())
    global_cache_finance_indicators[key] = {
        'datetime': pd.datetime.now(),
        'indicators': indicators
    }
    return indicators


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)