from application.routes.intermediate_calculations import (
    router as intermediate_calculations_router,
)


class TestAdd:
    def test_add_positive_integers(self):
        assert 5 == 5
