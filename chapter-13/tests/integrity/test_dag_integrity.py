import pytest

from ..conftest import get_import_errors


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a DAG files"""
    if rel_path and rv:  # Make sure our no op test doesn't raise an error
        raise Exception(f"{rel_path} failed to import with message \n {rv}")
