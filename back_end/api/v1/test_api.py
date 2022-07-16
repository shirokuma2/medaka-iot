from fastapi.testclient import TestClient
import pytest

import inspect
from util import Utility
from main import app

utl = Utility()
client = TestClient(app)
app_name = "api-ebayapi"
path = "prod"
exuid = "x0KHMvt7HDbzBMqeRUcRxo4nZUp2"
# exuid = "UUHSYXg8VzfiJ0gJDZ9pb9oI9PA2"
ref_tk = utl.exdb.collection("setting").document(exuid).collection("token").document("ebay")
ref = ref_tk.get().to_dict()["refresh_token"]


class TestApp:

    @pytest.fixture(scope="session")
    def get_headers(self):
        # sian6com expo prod account
        headers = {"refresh_token": ref}
        iaf_token = client.get(f"/{app_name}/{path}/iaf_token", headers=headers).json()["result"]
        return {"iaf_token": iaf_token}

    def test_user(self, get_headers):
        print("category_", category_.keys())
        keys = list(user_.keys())
        func = inspect.stack()[0][3].replace("test_", "")
        res = client.get(f"/{app_name}/{path}/{func}", headers=get_headers)
        assert res.status_code == 200
        result = res.json()["result"]
        # check base
        assert isinstance(result, dict)
        assert list(result.keys()) == keys
        assert result["Ack"] in ['Success', 'Warning']
        # check unique
        assert result["User"]["UserID"] == "fineoasis-jp"
        assert list(result["User"].keys()) == list(user_["User"].keys())

