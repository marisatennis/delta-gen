"""Unit tests for column filtering functionality (DG-3)."""
from deltagen.model import ColumnConfig, StageConfig, TableConfig


class TestStageConfigFiltering:
    """Test column filtering at the StageConfig level."""

    def test_filter_columns_by_temporary(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", temporary=False),
                ColumnConfig(name="temp1", temporary=True),
                ColumnConfig(name="name", temporary=False),
                ColumnConfig(name="temp2", temporary=True),
            ],
        )

        result = stage.filter_columns(temporary=False)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "name"}

    def test_filter_columns_by_natural(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", natural=True),
                ColumnConfig(name="email", natural=True),
                ColumnConfig(name="name", natural=False),
                ColumnConfig(name="address", natural=False),
            ],
        )

        result = stage.filter_columns(natural=True)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_filter_columns_combined_attributes(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", natural=True, temporary=False),
                ColumnConfig(name="email", natural=True, temporary=False),
                ColumnConfig(name="temp_key", natural=True, temporary=True),
                ColumnConfig(name="name", natural=False, temporary=False),
            ],
        )

        result = stage.filter_columns(natural=True, temporary=False)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_filter_columns_by_extension_attribute(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="ssn", extensions={"pii": True}),
                ColumnConfig(name="email", extensions={"pii": True}),
                ColumnConfig(name="id", extensions={"pii": False}),
                ColumnConfig(name="name"),  # no pii extension
            ],
        )

        result = stage.filter_columns(pii=True)
        assert len(result) == 2
        assert {col.name for col in result} == {"ssn", "email"}

    def test_filter_columns_mixed_core_and_extension(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(
                    name="ssn", temporary=False, extensions={"pii": True, "masked": True}
                ),
                ColumnConfig(name="email", temporary=False, extensions={"pii": True}),
                ColumnConfig(
                    name="temp_ssn", temporary=True, extensions={"pii": True}
                ),
                ColumnConfig(name="id", temporary=False),
            ],
        )

        # Filter for persistent PII columns
        result = stage.filter_columns(temporary=False, pii=True)
        assert len(result) == 2
        assert {col.name for col in result} == {"ssn", "email"}

        # Filter for masked persistent columns
        result = stage.filter_columns(temporary=False, masked=True)
        assert len(result) == 1
        assert result[0].name == "ssn"

    def test_filter_columns_no_kwargs_returns_all(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id"),
                ColumnConfig(name="name"),
                ColumnConfig(name="email"),
            ],
        )

        result = stage.filter_columns()
        assert len(result) == 3
        assert {col.name for col in result} == {"id", "name", "email"}

    def test_filter_columns_no_matches(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", natural=False),
                ColumnConfig(name="name", natural=False),
            ],
        )

        result = stage.filter_columns(natural=True)
        assert len(result) == 0
        assert result == ()

    def test_filter_columns_by_nullable(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", nullable=False),
                ColumnConfig(name="name", nullable=True),
                ColumnConfig(name="email", nullable=False),
            ],
        )

        result = stage.filter_columns(nullable=False)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_get_persistent_columns(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", temporary=False),
                ColumnConfig(name="temp1", temporary=True),
                ColumnConfig(name="name", temporary=False),
                ColumnConfig(name="temp2", temporary=True),
            ],
        )

        result = stage.get_persistent_columns()
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "name"}

    def test_get_natural_key_columns(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", natural=True),
                ColumnConfig(name="email", natural=True),
                ColumnConfig(name="name", natural=False),
            ],
        )

        result = stage.get_natural_key_columns()
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_temporary_columns_returns_tuple_of_names(self):
        stage = StageConfig(
            name="test_stage",
            columns=[
                ColumnConfig(name="id", temporary=False),
                ColumnConfig(name="temp1", temporary=True),
                ColumnConfig(name="temp2", temporary=True),
            ],
        )

        result = stage.temporary_columns()
        assert isinstance(result, tuple)
        assert result == ("temp1", "temp2")


class TestTableConfigFiltering:
    """Test column filtering at the TableConfig level (across stages)."""

    def test_filter_columns_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False),
                        ColumnConfig(name="temp1", temporary=True),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="name", temporary=False),
                        ColumnConfig(name="temp2", temporary=True),
                    ],
                ),
            ],
        )

        result = table.filter_columns(temporary=False)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "name"}

    def test_filter_columns_by_natural_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True),
                        ColumnConfig(name="name", natural=False),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="email", natural=True),
                        ColumnConfig(name="address", natural=False),
                    ],
                ),
            ],
        )

        result = table.filter_columns(natural=True)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_filter_columns_combined_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True, temporary=False),
                        ColumnConfig(name="temp_id", natural=True, temporary=True),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="email", natural=True, temporary=False),
                        ColumnConfig(name="name", natural=False, temporary=False),
                    ],
                ),
            ],
        )

        result = table.filter_columns(natural=True, temporary=False)
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_filter_columns_by_extension_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="ssn", extensions={"pii": True}),
                        ColumnConfig(name="id", extensions={"pii": False}),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="email", extensions={"pii": True}),
                        ColumnConfig(name="name"),
                    ],
                ),
            ],
        )

        result = table.filter_columns(pii=True)
        assert len(result) == 2
        assert {col.name for col in result} == {"ssn", "email"}

    def test_get_persistent_columns_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False),
                        ColumnConfig(name="temp1", temporary=True),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="name", temporary=False),
                        ColumnConfig(name="temp2", temporary=True),
                    ],
                ),
            ],
        )

        result = table.get_persistent_columns()
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "name"}

    def test_get_natural_key_columns_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", natural=True),
                        ColumnConfig(name="name", natural=False),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="email", natural=True),
                        ColumnConfig(name="address", natural=False),
                    ],
                ),
            ],
        )

        result = table.get_natural_key_columns()
        assert len(result) == 2
        assert {col.name for col in result} == {"id", "email"}

    def test_temporary_columns_property_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id", temporary=False),
                        ColumnConfig(name="temp1", temporary=True),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="temp2", temporary=True),
                        ColumnConfig(name="name", temporary=False),
                    ],
                ),
            ],
        )

        result = table.temporary_columns
        assert isinstance(result, tuple)
        assert set(result) == {"temp1", "temp2"}

    def test_filter_columns_empty_table(self):
        table = TableConfig(name="test_table", stages=[])

        result = table.filter_columns(temporary=False)
        assert len(result) == 0
        assert result == ()

    def test_filter_columns_no_kwargs_returns_all_across_stages(self):
        table = TableConfig(
            name="test_table",
            stages=[
                StageConfig(
                    name="stage1",
                    columns=[
                        ColumnConfig(name="id"),
                        ColumnConfig(name="name"),
                    ],
                ),
                StageConfig(
                    name="stage2",
                    columns=[
                        ColumnConfig(name="email"),
                    ],
                ),
            ],
        )

        result = table.filter_columns()
        assert len(result) == 3
        assert {col.name for col in result} == {"id", "name", "email"}


class TestFilteringReturnTypes:
    """Test that all filtering methods return tuples, not lists."""

    def test_stage_filter_columns_returns_tuple(self):
        stage = StageConfig(name="test", columns=[ColumnConfig(name="id")])
        result = stage.filter_columns()
        assert isinstance(result, tuple)

    def test_stage_get_persistent_columns_returns_tuple(self):
        stage = StageConfig(name="test", columns=[ColumnConfig(name="id")])
        result = stage.get_persistent_columns()
        assert isinstance(result, tuple)

    def test_stage_get_natural_key_columns_returns_tuple(self):
        stage = StageConfig(name="test", columns=[ColumnConfig(name="id")])
        result = stage.get_natural_key_columns()
        assert isinstance(result, tuple)

    def test_stage_temporary_columns_returns_tuple(self):
        stage = StageConfig(name="test", columns=[ColumnConfig(name="id")])
        result = stage.temporary_columns()
        assert isinstance(result, tuple)

    def test_table_filter_columns_returns_tuple(self):
        table = TableConfig(
            name="test",
            stages=[StageConfig(name="s1", columns=[ColumnConfig(name="id")])],
        )
        result = table.filter_columns()
        assert isinstance(result, tuple)

    def test_table_get_persistent_columns_returns_tuple(self):
        table = TableConfig(
            name="test",
            stages=[StageConfig(name="s1", columns=[ColumnConfig(name="id")])],
        )
        result = table.get_persistent_columns()
        assert isinstance(result, tuple)

    def test_table_get_natural_key_columns_returns_tuple(self):
        table = TableConfig(
            name="test",
            stages=[StageConfig(name="s1", columns=[ColumnConfig(name="id")])],
        )
        result = table.get_natural_key_columns()
        assert isinstance(result, tuple)

    def test_table_temporary_columns_returns_tuple(self):
        table = TableConfig(
            name="test",
            stages=[StageConfig(name="s1", columns=[ColumnConfig(name="id")])],
        )
        result = table.temporary_columns
        assert isinstance(result, tuple)
