import pandas as pd

from application.modeling import helper


def calculate_ratios(
    start_date: pd.Timestamp,
    months_to_forecast: int,
    income_statement_df: pd.DataFrame,
    loan_book_df: pd.DataFrame,
    balance_sheet_df: pd.DataFrame,
    direct_cashflow_df: pd.DataFrame,
):
    ratios = pd.DataFrame(
        columns=helper.generate_columns(
            start_date=start_date, period=months_to_forecast
        ),
        index=[
            "Gross Interest Income",
            "Interest Expense",
            "Management Expenses",
            "PBT",
            "Loan Book",
            "Disbursements",
            "Average Loan Size",
            "Client Count",
            "Operational Self Sufficiency",
        ],
    )

    ratios.loc["Gross Interest Income"] = income_statement_df.loc["Interest Income"]
    ratios.loc["Interest Expense"] = income_statement_df.loc["Finance Costs"]
    ratios.loc["Management Expenses"] = income_statement_df.loc["TOTAL EXPENSES"]
    ratios.loc["PBT"] = income_statement_df.loc["PROFIT / (LOSS) BEFORE TAX"]
    ratios.loc["Loan Book"] = loan_book_df.loc["Closing Balance"]
    ratios.loc["Disbursements"] = loan_book_df.loc["New Disbursements"]
    ratios.loc["Operational Self Sufficiency"] = income_statement_df.loc[
        "Total Revenue"
    ] / (
        income_statement_df.loc["Finance Costs"]
        + balance_sheet_df.loc["Provisions"]
        + -direct_cashflow_df.loc["Operating Expenses"]
    )

    return ratios
