import os
import shutil
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "src"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from so_lakehouse.spark_session import local_spark
    work = tmp_path_factory.mktemp("spark")
    s = local_spark("so-lakehouse-tests", work_dir=str(work))
    yield s
    s.stop()


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    """A private copy of data/demo_landing that tests can add drift/broken batches to."""
    root = tmp_path_factory.mktemp("landing")
    shutil.copytree(os.path.join(REPO, "data", "demo_landing"), str(root), dirs_exist_ok=True)
    return str(root)


@pytest.fixture(scope="session")
def cfg(landing):
    from so_lakehouse.config import PipelineConfig
    return PipelineConfig.for_local(landing, pii_salt="test-salt", schema_prefix="test_")
