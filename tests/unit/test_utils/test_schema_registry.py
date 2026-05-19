from src.utils.schema_registry import (
    REQUIRED_STREAM_COLUMNS,
    SONGS_SELECT_COLUMNS,
    USERS_SELECT_COLUMNS,
)


def test_required_stream_columns_exact_order() -> None:
    assert ["user_id", "track_id", "listen_time"] == REQUIRED_STREAM_COLUMNS


def test_songs_select_columns_contain_required_fields() -> None:
    expected = {"track_id", "track_name", "artists", "track_genre", "duration_ms"}
    assert expected.issubset(set(SONGS_SELECT_COLUMNS))


def test_users_select_columns_contain_user_id() -> None:
    assert "user_id" in USERS_SELECT_COLUMNS


def test_all_schemas_are_lists_of_strings() -> None:
    for schema in (REQUIRED_STREAM_COLUMNS, SONGS_SELECT_COLUMNS, USERS_SELECT_COLUMNS):
        assert isinstance(schema, list)
        assert all(isinstance(column, str) for column in schema)
