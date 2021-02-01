from flask import Flask, render_template, request
from Database.DatabaseClient import DatabaseClient
import datetime
import pandas as pd


app = Flask(__name__)

database_client = DatabaseClient()


@app.template_filter('stock_code_normalizer')
def stock_code_normalizer(code):
    return code[-2:] + code[:-3]


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
                           roe='on',
                           roe_min=5,
                           roe_max='',
                           tr_yoy='on',
                           tr_yoy_min=5,
                           tr_yoy_max='',
                           netprofit_yoy='on',
                           netprofit_yoy_min=5,
                           netprofit_yoy_max='')


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

        roe = ("on" == request.form.get('roe', ""))
        roe_min = request.form.get('roe_min', None)
        roe_max = request.form.get('roe_max', None)
        tr_yoy = ("on" == request.form.get('tr_yoy', ""))
        tr_yoy_min = request.form.get('tr_yoy_min', None)
        tr_yoy_max = request.form.get('tr_yoy_max', None)
        netprofit_yoy = ("on" == request.form.get('netprofit_yoy', ""))
        netprofit_yoy_min = request.form.get('netprofit_yoy_min', None)
        netprofit_yoy_max = request.form.get('netprofit_yoy_max', None)

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
                                        'roe': roe,
                                        'roe_min': roe_min,
                                        'roe_max': roe_max,
                                        'tr_yoy': tr_yoy,
                                        'tr_yoy_min': tr_yoy_min,
                                        'tr_yoy_max': tr_yoy_max,
                                        'netprofit_yoy': netprofit_yoy,
                                        'netprofit_yoy_min': netprofit_yoy_min,
                                        'netprofit_yoy_max': netprofit_yoy_max,
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
                (daily_cci,   'Daily_CCI_14',   daily_cci_min,   daily_cci_max,   end_of_week),
                (weekly_cci,  'Weekly_CCI_14',  weekly_cci_min,  weekly_cci_max,  end_of_month),
                (monthly_cci, 'Monthly_CCI_14', monthly_cci_min, monthly_cci_max, None)
            ]

            date = datetime.date.today() + datetime.timedelta(days=-365)
            sql = "SELECT code, name FROM Dolphin.StockBasic WHERE list >= '%s'" % (date.strftime('%Y%m%d'))
            sub_new_stocks = pd.read_sql_query(sql, database_client.get_engine())
            sub_new_stocks.set_index('code', inplace=True)

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
                            stocks_joint = stocks_query_copy.join(sub_new_stocks, how="inner", rsuffix='_r')
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
                if basic_indicators:
                    stocks_query = stocks_query.join(basic_indicators_stocks, on='code', how='inner')
                    stocks_query = stocks_query[['code', 'name']]
                stocks = stocks_query.to_dict(orient='records')

    return render_template('common/stocks.html', stocks=stocks)


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)