# default
from typing import Dict

# install
from fastapi import FastAPI

# original
from util import Utility

# inti
app = FastAPI()
ut = Utility()


@app.post("/")
async def index():
    print("post index complete!")
    return {"result": "success"}


@app.post("/rswq")
async def receiving_sensor_water_quality(params: Dict):
    """
    localデバイスからセンサーデータをpostするので、保存処理後BQに格納
    :param params:
    :return:
    """
    try:
        print("params:", params)
        return {"result": "success"}
    except Exception as e:
        print("error", e)
