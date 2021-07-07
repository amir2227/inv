from flask import Flask, request
from flask import jsonify
from datetime import date
import config
import MySQLdb

app = Flask(__name__)


def get_database_connection():
    """connects to the MySQL database and returns the connection"""
    return MySQLdb.connect(host=config.MYSQL_HOST,
                           user=config.MYSQL_USERNAME,
                           passwd=config.MYSQL_PASSWORD,
                           db=config.MYSQL_DB_NAME,
                           charset='utf8')


@app.route('/GDP')
def gdp():
    try:
        arg = request.args['field'] 
        db = get_database_connection()
        cur = db.cursor()
        cur.execute('SELECT d_date,{} FROM GDP'.format(arg))
        row_headers = [x[0] for x in cur.description]
        rows = cur.fetchall()
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))
        return jsonify(json_data), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()

@app.route('/COT')
def cot():
    try:
        arg = request.args['field'] 
        db = get_database_connection()
        cur = db.cursor()
        cur.execute(f'SELECT d_date,L_{arg},S_{arg},a_{arg} FROM COT')
        row_headers = [x[0] for x in cur.description]
        rows = cur.fetchall()
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))
        return jsonify(json_data), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()

@app.route('/E_SCORE')
def EScore():
    try:
        arg = request.args['field']
        db = get_database_connection()
        cur = db.cursor()
        cur.execute('SELECT d_date,{} FROM E_SCORE'.format(arg))
        row_headers = [x[0] for x in cur.description]
        rows = cur.fetchall()
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))
        return jsonify(json_data), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()


@app.route('/IR')
def ir():
    try:
        arg = request.args['field'] 
        db = get_database_connection()
        cur = db.cursor()
        cur.execute('SELECT d_date,{} FROM IR'.format(arg))
        row_headers = [x[0] for x in cur.description]
        rows = cur.fetchall()
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))
        return jsonify(json_data), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()

@app.route('/date')
def find_by_date():
    try:
        arg = request.args['data'] 
        d_date = request.args['date']
        db = get_database_connection()
        cur = db.cursor()
        cur.execute(f"SELECT * FROM {arg} where d_date like '%{d_date}%'")
        row_headers = [x[0] for x in cur.description]
        rows = cur.fetchall()
        json_data = []
        for result in rows:
            json_data.append(dict(zip(row_headers, result)))
        return jsonify(json_data), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()
@app.route('/test')
def test():
    try:
        json_data_ir = {}
        json_data_gdp = {}
        json_data_cot = {}
        json_data_escore = {}
        today = date.today()
        db = get_database_connection()
        cur = db.cursor()
        cur.execute(f"SELECT * FROM IR where d_date = '{today.year}.0'")
        row_headers_ir = [x[0] for x in cur.description]
        rows = cur.fetchall()
        cur.execute(f"SELECT * FROM GDP where d_date = '{today.year}'")
        print(f"SELECT * FROM ir where d_date = '{today.year}'")
        row_headers_gdp = [x[0] for x in cur.description]
        rows2 = cur.fetchall()
        cur.execute(f"""SELECT * FROM COT where
                    d_date <= '{today}' order by d_date desc limit 1""")
        row_headers_cot = [x[0] for x in cur.description]
        rows3 = cur.fetchall()
        cur.execute(f"""SELECT * FROM E_SCORE where
                    d_date <= '{today}' order by d_date desc limit 1""")
        row_headers_escore = [x[0] for x in cur.description]
        rows4 = cur.fetchall()
        for result in rows:
            json_data_ir = dict(zip(row_headers_ir, result))
        for result2 in rows2:
            json_data_gdp = dict(zip(row_headers_gdp, result2))
        for result3 in rows3:
            json_data_cot = dict(zip(row_headers_cot, result3))
        for result4 in rows4:
            json_data_escore = dict(zip(row_headers_escore, result4))
        return jsonify({'ir': json_data_ir, 'gdp': json_data_gdp,
                    'e_score': json_data_escore, 'cot': json_data_cot}), 200
    except Exception as e:
        print(e)
        return jsonify({'error': f'there is some problem in database {e}'})
    finally:
        cur.close()


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
