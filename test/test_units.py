from unittest.mock import Mock

import pytest

from main import main
from controller.tasks import AUTHS


def run(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    return main(req)


@pytest.mark.parametrize(
    "auth",
    AUTHS,
    ids=[i["client"] for i in AUTHS],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        ("2021-10-01", "2021-11-02"),
    ],
    ids=["auto", "manual"],
)
def test_auto(auth, start, end):
    """Test the scripts for auto mode"""

    res = run(
        {
            "auth": auth,
            "start": start,
            "end": end,
        }
    )
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]
