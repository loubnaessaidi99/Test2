from flask import Flask, request

app = Flask(__name__)

# Fonction principale déclenchée par un événement HTTP
def main(request):
    return 'Data processing complete.'

@app.route('/hello_http', methods=['POST'])
def handle_request():
    return main(request)

if __name__ == "__main__":
    app.run(debug=True)
