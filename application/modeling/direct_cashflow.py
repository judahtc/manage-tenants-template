import numpy as np
import pandas as pd

from application.modeling import helper


def add_other_assets(parameters: pd.DataFrame, direct_cashflow_df: pd.DataFrame):
    other_assets = parameters.loc[
        [
            "INTANGIBLE_ASSETS",
            "INVESTMENT_IN_SUBSIDIARIES",
            "INVESTMENT_IN_ASSOCIATES",
            "INVESTMENT_PROPERTIES",
            "EQUITY_INVESTMENTS",
            "LONG_TERM_MONEY_MARKET_INVESTMENTS",
            "SHORT_TERM_MONEY_MARKET_INVESTMENTS",
            "LOANS_TO_RELATED_ENTITIES",
        ]
    ]

    purchase_of_other_assets = (np.where(other_assets > 0, 1, 0) * other_assets).sum()

    sale_of_other_assets = (np.where(other_assets < 0, 1, 0) * other_assets).sum()

    direct_cashflow_df.loc[
        "Purchase Of Other Assets"
    ] = helper.change_period_index_to_strftime(purchase_of_other_assets)

    direct_cashflow_df.loc[
        "Sale Of Other Assets"
    ] = helper.change_period_index_to_strftime(sale_of_other_assets)

    return direct_cashflow_df


def add_equity_and_intercompany_loans(
    parameters: pd.DataFrame, direct_cashflow_df: pd.DataFrame
):
    equity_and_intercompany_loans = parameters.loc[
        [
            "TREASURY_SHARES",
            "INTERCOMPANY_LOANS",
            "SHARE_CAPITAL",
            "SHARE_PREMIUM",
            "OTHER_COMPONENTS_OF_EQUITY",
        ]
    ]

    issue_of_equity_and_intercompany_loans = (
        np.where(equity_and_intercompany_loans > 0, 1, 0)
        * equity_and_intercompany_loans
    ).sum()
    sale_of_equity_and_repayments_on_intercompany_loans = (
        np.where(equity_and_intercompany_loans < 0, 1, 0)
        * equity_and_intercompany_loans
    ).sum()

    direct_cashflow_df.loc[
        "Issue Of Equity And Intercompany Loans"
    ] = helper.change_period_index_to_strftime(issue_of_equity_and_intercompany_loans)

    direct_cashflow_df.loc[
        "Sale Of Equity And Repayments On Intercompany Loans"
    ] = helper.change_period_index_to_strftime(
        sale_of_equity_and_repayments_on_intercompany_loans
    )

    return direct_cashflow_df


def generate_direct_cashflow_template(valuation_date, months_to_forecast):
    direct_cashflow = pd.DataFrame(
        index=[
            "CASH INFLOWS",
            "Short Term Borrowing",
            "Long Term Borrowing",
            "Capital Repayment",
            "Interest Income",
            "Other Income",
            "Receipts From Receivables",
            "Purchase Of Inventory",
            "Issue Of Equity And Intercompany Loans",
            "Purchase Of Other Assets",
            "Total Cash Inflows",
            "CASH OUTFLOWS",
            "Disbursements",
            "Interest Expense",
            "Capital Repayment On Borrowings",
            "Operating Expenses",
            "Capital Expenses",
            "Payments To Payables",
            "Sale Of Equity And Repayments On Intercompany Loans",
            "Sale Of Other Assets",
            "Dividend Paid",
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


def calculate_loan_end_date_on_existing_borrowing(
    details_of_existing_borrowing: pd.DataFrame, valuation_date: str
):
    details_of_existing_borrowing = details_of_existing_borrowing.assign(
        loan_end_date=pd.Period(valuation_date)
        + details_of_existing_borrowing["remaining_loan_term"]
    )

    return details_of_existing_borrowing


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


def calculate_capital_repayment_on_borrowings(
    details_of_existing_long_term_borrowing: pd.DataFrame,
    details_of_existing_short_term_borrowing: pd.DataFrame,
    details_of_new_short_term_borrowing: pd.DataFrame,
    details_of_new_long_term_borrowing: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    details_of_new_short_term_borrowing = calculate_loan_end_date_on_new_borrowing(
        details_of_new_short_term_borrowing
    )

    details_of_new_long_term_borrowing = calculate_loan_end_date_on_new_borrowing(
        details_of_new_long_term_borrowing
    )

    details_of_existing_short_term_borrowing = (
        calculate_loan_end_date_on_existing_borrowing(
            details_of_existing_short_term_borrowing, valuation_date
        )
    )

    details_of_existing_long_term_borrowing = (
        calculate_loan_end_date_on_existing_borrowing(
            details_of_existing_long_term_borrowing, valuation_date
        )
    )

    capital_repayment_on_new_short_term_borrowing = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_new_short_term_borrowing,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_new_long_term_borrowing = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_new_long_term_borrowing,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_existing_short_term_borrowing = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_existing_short_term_borrowing,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_existing_long_term_borrowing = (
        calculate_direct_cashflow_capital_repayment_on_borrowings(
            details_of_borrowing=details_of_existing_long_term_borrowing,
            valuation_date=valuation_date,
            months_to_forecast=months_to_forecast,
        )
    )

    capital_repayment_on_short_term_borrowing = helper.add_series(
        [
            capital_repayment_on_new_short_term_borrowing,
            capital_repayment_on_existing_short_term_borrowing,
        ]
    )

    capital_repayment_on_long_term_borrowing = helper.add_series(
        [
            capital_repayment_on_new_long_term_borrowing,
            capital_repayment_on_existing_long_term_borrowing,
        ]
    )

    return pd.DataFrame(
        {
            "short_term_borrowing": capital_repayment_on_short_term_borrowing,
            "long_term_borrowing": capital_repayment_on_new_long_term_borrowing,
            "total": helper.add_series(
                [
                    capital_repayment_on_long_term_borrowing,
                    capital_repayment_on_short_term_borrowing,
                ]
            ),
        }
    )


def calculate_tax_paid(tax_schedule: pd.DataFrame):
    tax_paid = {}
    tax_payment_dates = tax_schedule.columns[
        tax_schedule.columns.str.contains("Mar|Jun|Sep|Dec")
    ]
    for date in tax_schedule.columns:
        if date in tax_payment_dates:
            tax_date_location = tax_schedule.columns.get_loc(date)
            initial_outstanding_months = tax_date_location - 2
            tax_paid[date] = (
                tax_schedule.loc["Tax Charged"]
                .iloc[initial_outstanding_months : tax_date_location + 1]
                .sum()
            )
        else:
            tax_paid[date] = 0

    tax_schedule.loc["Tax Paid"] = -pd.Series(tax_paid)
    return tax_schedule


def generate_tax_schedule(
    taxation: pd.Series,
    opening_balance: float,
    valuation_date: str,
    months_to_forecast: int,
):
    tax_schedule = pd.DataFrame(
        index=["Opening Balance", "Tax Charged", "Tax Paid", "Closing Balance"],
        columns=helper.generate_columns(valuation_date, months_to_forecast),
    )
    tax_schedule.loc["Tax Charged"] = taxation
    tax_schedule.loc["Opening Balance", tax_schedule.columns[0]] = opening_balance
    tax_schedule = calculate_tax_paid(tax_schedule=tax_schedule)

    tax_schedule = helper.calculate_opening_and_closing_balances(tax_schedule)
    return tax_schedule


def calculate_long_and_short_term_borrowing_for_direct_cashflow(
    details_of_new_long_term_borrowing: pd.DataFrame,
    details_of_new_short_term_borrowing: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    short_term_borrowing = calculate_direct_cashflow_borrowing(
        details_of_new_borrowing=details_of_new_short_term_borrowing,
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )

    long_term_borrowing = calculate_direct_cashflow_borrowing(
        details_of_new_borrowing=details_of_new_long_term_borrowing,
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )

    return pd.DataFrame(
        {
            "long_term_borrowing": long_term_borrowing,
            "short_term_borrowing": short_term_borrowing,
            "total": short_term_borrowing + long_term_borrowing,
        }
    )
