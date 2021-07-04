from flask import Flask, request
from flask import jsonify
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
        cur.execute('SELECT d_date,{} FROM e_score'.format(arg))
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
        cur.execute('SELECT d_date,{} FROM ir'.format(arg))
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
