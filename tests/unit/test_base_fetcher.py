import pytest
from macro_pipeline.fetchers.base import BaseFetcher


def test_base_fetcher_cannot_be_instantiated():
    with pytest.raises(TypeError) as excinfo:
        BaseFetcher()
    assert "Can't instantiate abstract class BaseFetcher" in str(excinfo.value)


def test_subclass_must_implement_fetch_data():
    class IncompleteFetcher(BaseFetcher):
        pass

    with pytest.raises(TypeError) as excinfo:
        IncompleteFetcher()
    assert "fetch_data" in str(excinfo.value)
