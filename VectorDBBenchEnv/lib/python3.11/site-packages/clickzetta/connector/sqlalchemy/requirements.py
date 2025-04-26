import sqlalchemy.testing.requirements
import sqlalchemy.testing.exclusions

supported = sqlalchemy.testing.exclusions.open
unsupported = sqlalchemy.testing.exclusions.closed


class Requirements(sqlalchemy.testing.requirements.SuiteRequirements):
    @property
    def index_reflection(self):
        return unsupported()

    @property
    def indexes_with_ascdesc(self):
        return unsupported()

    @property
    def unique_constraint_reflection(self):
        return unsupported()

    @property
    def autoincrement_insert(self):
        return unsupported()

    @property
    def primary_key_constraint_reflection(self):
        return unsupported()

    @property
    def foreign_keys(self):
        return unsupported()

    @property
    def foreign_key_constraint_reflection(self):
        return unsupported()

    @property
    def on_update_cascade(self):
        return unsupported()

    @property
    def named_constraints(self):
        return unsupported()

    @property
    def temp_table_reflection(self):
        return unsupported()

    @property
    def temporary_tables(self):
        return unsupported()

    @property
    def duplicate_key_raises_integrity_error(self):
        return unsupported()

    @property
    def precision_numerics_many_significant_digits(self):
        return supported()

    @property
    def date_coerces_from_datetime(self):
        return unsupported()

    @property
    def window_functions(self):
        return supported()

    @property
    def ctes(self):
        return supported()

    @property
    def views(self):
        return supported()

    @property
    def schemas(self):
        return unsupported()

    @property
    def implicit_default_schema(self):
        return supported()

    @property
    def comment_reflection(self):
        return supported()

    @property
    def unicode_ddl(self):
        return unsupported()

    @property
    def datetime_literals(self):
        return supported()

    @property
    def timestamp_microseconds(self):
        return supported()

    @property
    def datetime_historic(self):
        return supported()

    @property
    def date_historic(self):
        return supported()

    @property
    def precision_numerics_enotation_small(self):
        return supported()

    @property
    def precision_numerics_enotation_large(self):
        return supported()

    @property
    def update_from(self):
        return supported()

    @property
    def order_by_label_with_expression(self):
        return supported()

    @property
    def sql_expression_limit_offset(self):
        return supported()


class WithSchemas(Requirements):
    @property
    def schemas(self):
        return supported()
