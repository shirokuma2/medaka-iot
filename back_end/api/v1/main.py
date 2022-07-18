# default

# install
from fastapi import FastAPI
from pydantic import BaseModel

# original
from process.util import Utility, log
import process.receiving_sensor_water_quality as rswq

# inti
app = FastAPI()
ut = Utility()


@app.post("/")
async def index():
    print("post index complete!")
    return {"result": "success"}


class RSWQ(BaseModel):
    location: str
    sensor: str
    measurements: float


@app.post("/rswq")
async def receiving_sensor_water_quality(params: RSWQ):
    """
    localデバイスからセンサーデータをpostするので、保存処理後BQに格納
    :param params:
    :return:
    """
    try:
        result = rswq.main(params)
        return {"result": result}
    except Exception as e:
        log(types="error", get_traceback=ut.get_traceback(), exception=e)
        return {"result": "failure"}
