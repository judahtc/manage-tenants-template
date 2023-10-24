import pandas as pd

from application.modeling import helper


def aggregate_new_and_existing_loans_capital_repayments(
    capital_repayments_new_disbursements_df: pd.Series,
    capital_repayments_existing_loans: pd.Series,
    start_date: str,
    months_to_forecast: int,
):
    return (
        capital_repayments_new_disbursements_df["total"]
        .add(capital_repayments_existing_loans, fill_value=0)
        .reindex(helper.generate_columns(start_date, months_to_forecast))
    )


def generate_loan_book_template(start_date: str, months_to_forecast: int):
    loan_book = pd.DataFrame(
        index=[
            "Opening Balance",
            "New Disbursements",
            "Repayments",
            "Interest Income",
            "Closing Balance",
        ],
        columns=helper.generate_columns(start_date, months_to_forecast),
    )
    return loan_book


def insert_loan_book_items(
    loan_book: pd.DataFrame,
    opening_balance_on_loan_book: float,
    disbursements: pd.Series,
    total_interest_income: pd.Series,
    total_capital_repayments: pd.Series,
):
    loan_book.loc[
        "Opening Balance", loan_book.columns[0]
    ] = opening_balance_on_loan_book
    loan_book.loc["New Disbursements"] = disbursements
    loan_book.loc["Repayments"] = -total_capital_repayments.add(
        total_interest_income, fill_value=0
    )
    loan_book.loc["Interest Income"] = total_interest_income

    return loan_book


def calculate_loan_book_yearly(loan_book: pd.DataFrame):
    loan_book_yearly = loan_book.groupby(
        pd.DatetimeIndex(loan_book.columns).year, axis=1
    ).sum()
    loan_book_yearly.columns = loan_book_yearly.columns.astype(str)
    return helper.calculate_opening_and_closing_balances(loan_book_yearly)
