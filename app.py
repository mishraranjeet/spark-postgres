from flask import Flask
from flask import request, jsonify, render_template, redirect
from connection import spark_db_connct

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return jsonify("Hello world !!")



@app.route("/show", methods=["GET"])
def showschema():
    df = spark_db_connct()

    spark_df = df.sql("select * from user limit 100000").show()
    
    return spark_df


if __name__ == "__main__":
    app.run()
