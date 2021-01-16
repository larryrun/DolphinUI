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
    date = datetime.date.today()
    date = date + datetime.timedelta(days=-date.day+1)
    date = date.strftime('%Y-%m-%d')
    return render_template('vip/dshj.html',
                           strategy_cci_date=date,
                           strategy_cci_weekly=300,
                           strategy_cci_monthly=300,
                           strategy_cci_both_satisfy='on')


@app.route('/vip/dshj_cci', methods=['GET', 'POST'])
def vip_dshj_cci():

    stocks = {}
    if request.method == 'POST':

        weekly_cci = request.form.get('strategy_cci_weekly', None)
        monthly_cci = request.form.get('strategy_cci_monthly', None)
        both_satisfy = ("on" == request.form.get('strategy_cci_both_satisfy', ""))
        start_date = request.form.get('strategy_cci_date', None)

        audit_event = pd.DataFrame({'date': [pd.datetime.today().strftime("%Y-%m-%d %H:%M:%S")],
                                    'ip': [request.remote_addr],
                                    'event': [str({
                                        'url': '/vip/dshj_cci',
                                        'weekly_cci': weekly_cci,
                                        'monthly_cci': monthly_cci,
                                        'both_satisfy': both_satisfy,
                                        'start_date': start_date
                                    })]})
        database_client.write_audit_event(audit_event)

        weekly_stocks = pd.DataFrame({'code':[], 'name':[]})
        if weekly_cci:
            sql = "SELECT a.code, b.name, a.date FROM TradeIndicator a, StockBasic b WHERE a.name = 'Weekly_CCI_14' AND a.date >= '%s' AND a.value >= %s AND a.code = b.code"
            sql = sql % (pd.to_datetime(start_date).strftime('%Y%m%d'), weekly_cci)
            weekly_stocks = pd.read_sql_query(sql, database_client.get_engine())
            weekly_stocks['date'] = weekly_stocks.date.apply(lambda x: last_day_of_month(x))
            weekly_stocks = weekly_stocks.drop_duplicates()

        monthly_stocks = pd.DataFrame({'code':[], 'name':[]})
        if monthly_cci:
            sql = "SELECT a.code, b.name, a.date FROM TradeIndicator a, StockBasic b WHERE a.name = 'Monthly_CCI_14' AND a.date >= '%s' AND a.value >= %s AND a.code = b.code"
            sql = sql % (pd.to_datetime(start_date).strftime('%Y%m%d'), monthly_cci)
            monthly_stocks = pd.read_sql_query(sql, database_client.get_engine())

        if both_satisfy:
            if not weekly_stocks.empty and not monthly_stocks.empty:
                weekly_stocks.set_index(weekly_stocks.apply(lambda row: "%s_%s" % (row[0], row[2].strftime('%Y%m%d')), axis=1), inplace=True)
                monthly_stocks.set_index(monthly_stocks.apply(lambda row: "%s_%s" % (row[0], row[2].strftime('%Y%m%d')), axis=1), inplace=True)
                joint_stocks = weekly_stocks.join(monthly_stocks, how="inner", rsuffix='_m')
                stocks = joint_stocks[['code', 'name']]
                stocks = stocks.to_dict(orient='records')
        else:
            stocks = pd.DataFrame({'code':[], 'name':[]})
            stocks = stocks.append(weekly_stocks[['code', 'name']])
            stocks = stocks.append(monthly_stocks[['code', 'name']])
            stocks = stocks.drop_duplicates()
            stocks = stocks.to_dict(orient='records')

    return render_template('common/stocks.html', stocks=stocks)


def last_day_of_month(date):
    if date.month in [1, 3, 5, 7, 8, 10, 12]:
        return datetime.date(date.year, date.month, 31)
    elif date.month in [4, 6, 9, 11]:
        return datetime.date(date.year, date.month, 30)
    else:
        return datetime.date(date.year, date.month, 29 if date.year % 4 == 0 else 28)


#if __name__ == '__main__':
#    app.run(debug=True)