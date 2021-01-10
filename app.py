from flask import Flask, render_template, request
from flaskext.mysql import MySQL
import datetime

app = Flask(__name__)
app.config['MYSQL_DATABASE_HOST'] = 'sh-cdb-1m56f2js.sql.tencentcdb.com'
app.config['MYSQL_DATABASE_PORT'] = 60388
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'pwd4root'
app.config['MYSQL_DATABASE_DB'] = 'Dolphin'
mysql = MySQL()
mysql.init_app(app)
NAMES = ['Weekly_CCI_14', 'Monthly_CCI_14']


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/cci.html', methods=['GET', 'POST'])
def cci():
    if request.method == 'POST':
        name = request.form['name']
        val = request.form['value']
        date = request.form['date']
        msg = ''
        result = []
        try:
            cur = mysql.get_db().cursor()
            cur.execute('SELECT code, date, value FROM TradeIndicator WHERE name = %s AND value >= %s AND date >= %s ORDER by date DESC', (name, val, date))
            rows = cur.fetchall()
            for row in rows:
                code = row[0]
                date = row[1]
                value = row[2]
                result.append({'code': code, 'date': date, 'value': value})
        except Exception as err:
            msg = '数据库查询出错:' + err.__repr__()
        if msg == '' and len(result) == 0:
            msg = '无数据'
        return render_template('cci.html', names=NAMES, selectedName=name, selectedValue=val, selectedDate=date, msg=msg, result=result)
    else:
        today = datetime.date.today()
        last_day_of_last_month = datetime.date(today.year, today.month, 1) - datetime.timedelta(1)
        first_day_of_last_month = datetime.date(last_day_of_last_month.year, last_day_of_last_month.month, 1)
        return render_template('cci.html', names=NAMES, selectedDate=first_day_of_last_month.strftime('%Y%m%d'))
