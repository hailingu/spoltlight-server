from flask import Flask
import requests
import operators.operator
app = Flask(__name__)

url = 'http://localhost:8081'
payload = 'action=login&username=azkaban&password=azkaban'
headers = {
    'Content-type': 'application/x-www-form-urlencoded',
    'X-Requested-With': 'XMLHttpRequest'
}


@app.route('/')
def hello_flask():
    a = operators.operator.Operator()
    print(a)
    r = requests.post(url, data=payload, headers=headers)
    return r.content

def create_azkaban_job(job_id):
    return 0


if __name__ == '__main__':
    app.debug = True
    app.run(host='127.0.0.1', port=5000)
