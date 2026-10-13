"""
MOBs (Months on Books) package.

- :class:`Mobs` — calculation of per-source seniority tables and imputation
  of the feature-engineering outputs for low-seniority clients
  (``mobs`` pipeline stage).
- :class:`PreviousFixerPayrollIncome` / :class:`PreviousFixerPayrollStreaks`
  — source-specific multi-threaded imputers used by ``Mobs.run_mobs``.
"""
from .mobs import (
    Mobs,
    contiguous_mobs,
    count_non_null_values,
    discontiguous_mobs,
    get_last_contiguous_non_null_value,
    get_numeric_columns,
)
from .payroll_income import PreviousFixerPayroll, PreviousFixerPayrollIncome
from .payroll_streaks import PreviousFixerPayrollStreaks

__all__ = [
    "Mobs",
    "contiguous_mobs",
    "count_non_null_values",
    "discontiguous_mobs",
    "get_last_contiguous_non_null_value",
    "get_numeric_columns",
    "PreviousFixerPayroll",
    "PreviousFixerPayrollIncome",
    "PreviousFixerPayrollStreaks",
]
