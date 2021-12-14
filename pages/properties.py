PORT = 4100

from flask import Flask, request
app = Flask(__name__, template_folder='../frontend/')
