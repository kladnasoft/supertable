# route: supertable.utils.tests.test_diagnostic_redaction
"""An exception type name can carry content, and it ends up in logs.

``safe_exception_type`` is called from request logging, so whatever it returns
crosses a trust boundary. A class name is normally a plain identifier, but a
dynamically constructed one can embed a table name, a path, or a token — and
then that string is what the log says.
"""

from __future__ import annotations

from supertable.utils.diagnostic_redaction import safe_exception_type


def test_builtin_exceptions_report_their_own_name():
    assert safe_exception_type(ValueError("x")) == "ValueError"
    assert safe_exception_type(TypeError()) == "TypeError"


def test_normal_custom_exception_reports_its_own_name():
    class TableNotFound(RuntimeError):
        pass

    assert safe_exception_type(TableNotFound()) == "TableNotFound"


def test_a_name_carrying_content_is_not_echoed():
    """The case this exists for: a class name built from a value.

    The leaked fragment must not survive into the returned string, because the
    caller writes that straight into a request log.
    """
    leaky = type("token st_od_abc123 for customers", (RuntimeError,), {})
    got = safe_exception_type(leaky())
    assert "st_od_abc123" not in got and "customers" not in got
    assert got.endswith("RuntimeError")


def test_an_absurdly_long_name_is_collapsed():
    long = type("E" * 500, (ValueError,), {})
    got = safe_exception_type(long())
    assert len(got) < 100 and got.endswith("ValueError")


def test_it_falls_back_to_the_nearest_usable_base():
    """Two unusable names deep, it keeps walking rather than giving up."""
    outer = type("bad name one", (ValueError,), {})
    inner = type("bad name two", (outer,), {})
    assert safe_exception_type(inner()).endswith("ValueError")


def test_base_exception_is_handled():
    assert safe_exception_type(KeyboardInterrupt()) == "KeyboardInterrupt"
