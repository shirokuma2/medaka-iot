# from fastapi.testclient import TestClient
# import pytest
#
# import inspect
# from back_end.api.v1.process.util import Utility
# from main import app
#
# ut = Utility()
# client = TestClient(app)
# app_name = "api-v1"
#
#
# class TestApp:
#     def test_user(self):
#         keys = list(user_.keys())
#         func = inspect.stack()[0][3].replace("test_", "")
#         res = client.get(f"/{app_name}/{path}/{func}", headers=get_headers)
#         assert res.status_code == 200
#         result = res.json()["result"]
#         # check base
#         assert isinstance(result, dict)
#         assert list(result.keys()) == keys
#         assert result["Ack"] in ['Success', 'Warning']
#         # check unique
#         assert result["User"]["UserID"] == "fineoasis-jp"
#         assert list(result["User"].keys()) == list(user_["User"].keys())
