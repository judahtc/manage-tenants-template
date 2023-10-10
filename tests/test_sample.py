from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


class TestAdd:
    def test_add_positive_integers(self):
        assert 5 == 5


def test_calculate_new_disbursements():
    response = client.get("/zimnat/12/calculate-new-disbursements")
    print(response.json())
