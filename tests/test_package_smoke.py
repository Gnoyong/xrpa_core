from xrpa_core import __version__


def test_package_exposes_version() -> None:
    assert isinstance(__version__, str)
    assert __version__
