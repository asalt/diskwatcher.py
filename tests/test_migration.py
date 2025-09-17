from pathlib import Path

from diskwatcher.db.migration import build_alembic_config


def test_build_alembic_config_sets_database_url(tmp_path):
    custom_ini = tmp_path / "alembic.ini"
    custom_ini.write_text("""
[alembic]
script_location = migrations
""")

    config = build_alembic_config(
        ini_path=custom_ini,
        database_url="sqlite:///tmp/catalog.db",
    )

    assert config.get_main_option("sqlalchemy.url") == "sqlite:///tmp/catalog.db"
    assert config.get_main_option("script_location") == "migrations"
