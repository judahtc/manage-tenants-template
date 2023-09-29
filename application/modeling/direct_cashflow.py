import numpy as np
import pandas as pd

from application.modeling import helper


def generate_direct_cashflow_template(valuation_date, months_to_forecast):
    direct_cashflow = pd.DataFrame(
        index=[
            "CASH INFLOWS",
            "Receipts From Trade Receivables",
            "Capital Repayment",
            "Interest Income",
            "Other Income",
            "Issue Of Shares",
            "Short Term Borrowing",
            "Long Term Borrowing",
            "Total Cash Inflows",
            "CASH OUTFLOWS",
            "Disbursements",
            "Operating Expenses",
            "Capital Expenses",
            "Interest Expense",
            "Dividend Paid",
            "Capital Repayment On Borrowings",
            "Tax Paid",
            "Total Cash Outflows",
            "Net Increase/Decrease In Cash",
            "Opening Balance",
            "Closing Balance",
        ],
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        data=np.nan,
    )
    return direct_cashflow


def calculate_operating_expenses(income_statement: pd.DataFrame):
    provisions = income_statement.loc[
        income_statement.index.str.contains("provision", case=False)
    ].sum()
    operating_expenses = (
        income_statement.loc["TOTAL EXPENSES"]
        - provisions
        - income_statement.loc["Depreciation"]
    )
    return operating_expenses


def calculate_capital_expenses(
    details_of_new_assets: pd.DataFrame, valuation_date: str, months_to_forecast: int
):
    capital_expenses = details_of_new_assets[["cost", "purchase_date"]]
    capital_expenses = capital_expenses.assign(
        purchase_date=helper.convert_to_datetime(capital_expenses["purchase_date"])
    )
    capital_expenses["purchase_date"] = (
        capital_expenses["purchase_date"].dt.to_period("M").dt.strftime("%b-%Y")
    )
    capital_expenses = (
        capital_expenses.groupby("purchase_date")["cost"]
        .sum()
        .reindex(
            helper.generate_columns(valuation_date, months_to_forecast), fill_value=0
        )
    )
    return capital_expenses


def calculate_direct_cashflow_borrowing(
    details_of_new_borrowing: pd.DataFrame, valuation_date: str, months_to_forecast: int
):
    direct_cashflow_borrowing = details_of_new_borrowing[
        ["principal", "loan_start_date"]
    ]
    direct_cashflow_borrowing = direct_cashflow_borrowing.assign(
        loan_start_date=helper.convert_to_datetime(
            direct_cashflow_borrowing["loan_start_date"]
        )
    )
    direct_cashflow_borrowing["loan_start_date"] = (
        direct_cashflow_borrowing["loan_start_date"]
        .dt.to_period("M")
        .dt.strftime("%b-%Y")
    )
    direct_cashflow_borrowing = (
        direct_cashflow_borrowing.groupby("loan_start_date")["principal"]
        .sum()
        .reindex(
            helper.generate_columns(valuation_date, months_to_forecast), fill_value=0
        )
    )
    return direct_cashflow_borrowing


def insert_issue_of_shares(direct_cashflow: pd.DataFrame, parameters: pd.DataFrame):
    direct_cashflow.loc["Issue Of Shares"] = parameters.loc["ISSUE_OF_SHARES"]
    return direct_cashflow


def aggregate_existing_short_and_long_term_borrowing(
    details_of_existing_long_term_borrowing,
    details_of_existing_short_term_borrowing,
    valuation_date,
):
    details_of_existing_borrowings = pd.concat(
        [
            details_of_existing_long_term_borrowing,
            details_of_existing_short_term_borrowing,
        ]
    )
    details_of_existing_borrowings = details_of_existing_borrowings.assign(
        loan_end_date=pd.Period(valuation_date)
        + details_of_existing_borrowings["remaining_loan_term"]
    )
    return details_of_existing_borrowings


def aggregate_new_short_and_long_term_borrowing(
    details_of_new_long_term_borrowing,
    details_of_new_short_term_borrowing,
):
    details_of_new_borrowings = pd.concat(
        [details_of_new_long_term_borrowing, details_of_new_short_term_borrowing]
    )
    return details_of_new_borrowings


def calculate_loan_end_date_on_new_borrowing(details_of_new_borrowings: pd.DataFrame):
    details_of_new_borrowings = details_of_new_borrowings.assign(
        loan_start_date=helper.convert_to_datetime(
            details_of_new_borrowings.loan_start_date
        )
    )
    details_of_new_borrowings = details_of_new_borrowings.assign(
        loan_start_date=details_of_new_borrowings.loan_start_date.dt.to_period("M")
    )

    details_of_new_borrowings = details_of_new_borrowings.assign(
        loan_end_date=details_of_new_borrowings.loan_start_date
        + details_of_new_borrowings.remaining_loan_term.astype(np.int64)
    )
    return details_of_new_borrowings


def calculate_direct_cashflow_capital_repayment_on_borrowings(
    details_of_borrowing: pd.DataFrame, valuation_date, months_to_forecast
):
    direct_cashflow_capital_repayment_on_borrowings = details_of_borrowing[
        ["principal", "loan_end_date"]
    ]
    direct_cashflow_capital_repayment_on_borrowings = (
        direct_cashflow_capital_repayment_on_borrowings.groupby("loan_end_date")[
            "principal"
        ]
        .sum()
        .reindex(
            helper.generate_columns(valuation_date, months_to_forecast), fill_value=0
        )
    )
    return direct_cashflow_capital_repayment_on_borrowings


def calculate_opening_and_closing_balances_for_direct_cashflows(
    direct_cashflow: pd.DataFrame, cash_on_hand_opening_balance: float
):
    direct_cashflow.loc[
        "Opening Balance", direct_cashflow.columns[0]
    ] = cash_on_hand_opening_balance

    direct_cashflow.columns = pd.PeriodIndex(direct_cashflow.columns, freq="M")

    for period in direct_cashflow.columns:
        direct_cashflow.loc["Closing Balance", period] = direct_cashflow.loc[
            "Net Increase/Decrease In Cash":"Opening Balance", period
        ].sum()
        if period == direct_cashflow.columns[-1]:
            break
        direct_cashflow.loc["Opening Balance", period + 1] = direct_cashflow.loc[
            "Closing Balance", period
        ]
    direct_cashflow.columns = map(str, direct_cashflow.columns.strftime("%b-%Y"))

    return direct_cashflow


def generate_loan_book_template(valuation_date: str, months_to_forecast: int):
    loan_book = pd.DataFrame(
        index=[
            "Opening Balance",
            "New Disbursements",
            "Capital Repayments",
            "Closing Balance",
        ],
        columns=helper.generate_columns(valuation_date, months_to_forecast),
    )
    return loan_book


def insert_loan_book_items(
    loan_book: pd.DataFrame,
    opening_balance_on_loan_book: float,
    capital_repayment: pd.Series,
    disbursements: pd.Series,
):
    loan_book.loc[
        "Opening Balance", loan_book.columns[0]
    ] = opening_balance_on_loan_book
    loan_book.loc["Capital Repayments"] = capital_repayment
    loan_book.loc["New Disbursements"] = disbursements

    return loan_book


def calculate_opening_and_closing_balances_for_loan_book(loan_book: pd.DataFrame):
    loan_book.columns = pd.PeriodIndex(loan_book.columns, freq="M")
    for period in loan_book.columns:
        loan_book.loc["Closing Balance", period] = loan_book.loc[
            "Opening Balance":"Capital Repayments", period
        ].sum()

        if period == loan_book.columns[-1]:
            break

        loan_book.loc["Opening Balance", period + 1] = loan_book.loc[
            "Closing Balance", period
        ]
    loan_book.columns = map(str, loan_book.columns.strftime("%b-%Y"))
    return loan_book


def calculate_capital_repayment_on_borrowings(
    details_of_existing_long_term_borrowing: pd.DataFrame,
    details_of_existing_short_term_borrowing: pd.DataFrame,
    details_of_new_short_term_borrowing: pd.DataFrame,
    details_of_new_long_term_borrowing: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    details_of_existing_borrowings = aggregate_existing_short_and_long_term_borrowing(
        details_of_existing_long_term_borrowing=details_of_existing_long_term_borrowing,
        details_of_existing_short_term_borrowing=details_of_existing_short_term_borrowing,
        valuation_date=valuation_date,
    )
    details_of_new_borrowings = aggregate_new_short_and_long_term_borrowing(
        details_of_new_long_term_borrowing=details_of_new_long_term_borrowing,
        details_of_new_short_term_borrowing=details_of_new_short_term_borrowing,
    )

    details_of_new_borrowings = calculate_loan_end_date_on_new_borrowing(
        details_of_new_borrowings
    )

    capital_repayment_on_new_borrowings = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_new_borrowings,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_existing_borrowings = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_existing_borrowings,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_borrowings = helper.add_series(
        [capital_repayment_on_new_borrowings, capital_repayment_on_existing_borrowings]
    )

    return pd.DataFrame(
        {
            "capital_repayment_on_new_borrowings": capital_repayment_on_new_borrowings,
            "capital_repayment_on_existing_borrowings": capital_repayment_on_existing_borrowings,
            "total": capital_repayment_on_borrowings,
        }
    )
