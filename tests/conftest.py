import os
import pytest
import shutil

from tests.common import write_roocs_cfg, MINI_CEDA_MASTER_DIR

CEDA_TEST_DATA_REPO_URL = "https://github.com/cedadev/mini-ceda-archive"

write_roocs_cfg()

@pytest.fixture
def load_ceda_test_data():
    """
    This fixture ensures that the required test data repository
    has been cloned to the cache directory within the home directory.
    """
    tmp_repo = "/tmp/.mini-ceda-archive"
    test_data_dir = os.path.join(tmp_repo, "archive")

    if not os.path.isdir(MINI_CEDA_MASTER_DIR):

        os.makedirs(MINI_CEDA_MASTER_DIR)
        os.system(f"git clone {CEDA_TEST_DATA_REPO_URL} {tmp_repo}")

        shutil.move(test_data_dir, MINI_CEDA_MASTER_DIR)
        shutil.rmtree(tmp_repo)
