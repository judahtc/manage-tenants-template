import numpy as np
import pandas as pd

from application.modeling import helper


def generate_inventories_schedule(
    opening_inventories: int,
    inventories_used: pd.Series,
    new_inventories: pd.Series,
    valuation_date: str,
    months_to_forecast: int,
):
    inventories_used.index = helper.generate_columns(valuation_date, months_to_forecast)

    new_inventories.index = helper.generate_columns(valuation_date, months_to_forecast)

    inventories = pd.DataFrame(
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        index=[
            "Opening Balance",
            "New Inventories",
            "Inventories Used",
            "Closing Balance",
        ],
        data=0,
    )

    inventories.loc["New Inventories"] = new_inventories
    inventories.loc["Inventories Used"] = -inventories_used
    inventories.iloc[0, 0] = opening_inventories
    inventories = helper.calculate_opening_and_closing_balances(inventories)
    return inventories


def generate_payables_schedule(
    opening_payables: int,
    payments_to_payables: pd.Series,
    new_payables: pd.Series,
    valuation_date: str,
    months_to_forecast: int,
):
    payments_to_payables.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )
    new_payables.index = helper.generate_columns(valuation_date, months_to_forecast)

    payables = pd.DataFrame(
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        index=[
            "Opening Balance",
            "New Payables",
            "Payments To Payables",
            "Closing Balance",
        ],
        data=0,
    )

    payables.loc["New Payables"] = new_payables
    payables.loc["Payments To Payables"] = -payments_to_payables
    payables.iloc[0, 0] = opening_payables

    payables = helper.calculate_opening_and_closing_balances(payables)
    return payables


def generate_receivables_schedule(
    opening_receivables: int,
    receipts_from_receivables: pd.Series,
    new_receivables: pd.Series,
    valuation_date: str,
    months_to_forecast: int,
):
    receipts_from_receivables.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )

    new_receivables.index = helper.generate_columns(valuation_date, months_to_forecast)

    receivables = pd.DataFrame(
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        index=[
            "Opening Balance",
            "New Receivables",
            "Receipts From Receivables",
            "Closing Balance",
        ],
        data=0,
    )

    receivables.loc["New Receivables"] = new_receivables
    receivables.loc["Receipts From Receivables"] = -receipts_from_receivables
    receivables.iloc[0, 0] = opening_receivables
    receivables = helper.calculate_opening_and_closing_balances(receivables)
    return receivables


def generate_balance_sheet_template(valuation_date: str, months_to_forecast: int):
    balance_sheet_index = pd.DataFrame(
        {
            "STATEMENT_OF_FINANCIAL_POSITION": [
                "ASSETS",
                "NON CURRENT ASSETS",
                "Property Plant And Equipment",
                "Intangible Assets",
                "Investment In Subsidiaries",
                "Investment In Associates",
                "Investment Properties",
                "Equity Investments",
                "Long Term Money Market Investments",
                "Loans To Related Entities",
                "Non Current Assets",
                "CURRENT ASSETS",
                "Inventories",
                "Intergroup Receivables",
                "Loan Book",
                "Trade Receivables",
                "Other Receivables",
                "Cash On Hand",
                "Short Term Money Market Investments",
                "Current Assets",
                "TOTAL ASSETS",
                "EQUITY AND LIABILITIES",
                "CAPITAL AND RESERVES",
                "Issued Share Capital",
                "Share Premium",
                "Other Components Of Equity",
                "Treasury Shares",
                "Retained Earnings",
                "Capital And Reserves",
                "NON CURRENT LIABILITIES",
                "Loans",
                "Intercompany Loans",
                "Deferred Taxation",
                "Non Current Liabilities",
                "CURRENT LIABILITIES",
                "Trade Payables",
                "Other Payables",
                "Borrowings",
                "Provision For Taxation",
                "Provisions",
                "Current Liabilities",
                "TOTAL EQUITY AND LIABILITIES",
                "CHECK",
            ]
        }
    )

    balance_sheet_index["STATEMENT_OF_FINANCIAL_POSITION"] = balance_sheet_index[
        "STATEMENT_OF_FINANCIAL_POSITION"
    ].str.strip()

    balance_sheet_template = pd.DataFrame(
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        index=balance_sheet_index["STATEMENT_OF_FINANCIAL_POSITION"],
    )

    return balance_sheet_template


# def calculate_other_assets(
#     balance_sheet_df: pd.DataFrame,
#     parameters: pd.DataFrame,
#     opening_balances: pd.DataFrame,
# ):
#     other_assets_index = pd.Index(
#         [
#             "INTANGIBLE_ASSETS",
#             "INVESTMENT_IN_SUBSIDIARIES",
#             "INVESTMENT_IN_ASSOCIATES",
#             "INVESTMENT_PROPERTIES",
#             "EQUITY_INVESTMENTS",
#             "LONG_TERM_MONEY_MARKET_INVESTMENTS",
#             "SHORT_TERM_MONEY_MARKET_INVESTMENTS",
#             "LOANS_TO_RELATED_ENTITIES",
#             "INVENTORIES",
#             "OTHER_RECEIVABLES",
#             "INTERGROUP_RECEIVABLES",
#             "OTHER_RECEIVABLES",
#         ]
#     )

#     other_assets = (
#         parameters.loc[other_assets_index].cumsum(axis=1)
#         + opening_balances[other_assets_index].T.values
#     )

#     other_assets.columns = pd.PeriodIndex(other_assets.columns, freq="M").strftime(
#         "%b-%Y"
#     )

#     other_assets.index = other_assets_index.str.title().str.replace("_", " ")

#     balance_sheet_df.loc[other_assets.index] = other_assets

#     return balance_sheet_df


def sum_financial_statements_totals(financial_statement: pd.DataFrame):
    total_rows = financial_statement.index[
        financial_statement.index.str.lower().duplicated()
    ]

    for total_row in total_rows:
        financial_statement.loc[total_row] = financial_statement.iloc[
            financial_statement.index.get_loc(total_row.upper())
            + 1 : financial_statement.index.get_loc(total_row)
        ].sum()

    return financial_statement


def calculate_final_balances(balance_sheet_df: pd.DataFrame):
    balance_sheet_df.loc["TOTAL ASSETS"] = (
        balance_sheet_df.loc["Non Current Assets"]
        + balance_sheet_df.loc["Current Assets"]
    )

    balance_sheet_df.loc["TOTAL EQUITY AND LIABILITIES"] = (
        balance_sheet_df.loc["Current Liabilities"]
        + balance_sheet_df.loc["Non Current Liabilities"]
        + balance_sheet_df.loc["Capital And Reserves"]
    )

    balance_sheet_df.loc["CHECK"] = (
        balance_sheet_df.loc["TOTAL ASSETS"]
        == balance_sheet_df.loc["TOTAL EQUITY AND LIABILITIES"]
    )

    return balance_sheet_df


def calculate_short_term_loans_schedules(
    long_and_short_term_borrowing_df: pd.DataFrame,
    capital_repayment_on_borrowings_df: pd.DataFrame,
    opening_balances: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    short_term_loans_schedule = pd.DataFrame(
        index=[
            "Opening Balance",
            "Borrowings",
            "Repayments",
            "Closing Balance",
        ],
        columns=helper.generate_columns(valuation_date, months_to_forecast),
    )

    short_term_loans_schedule.loc[
        "Opening Balance", short_term_loans_schedule.columns[0]
    ] = opening_balances["SHORT_TERM_LOANS"].iat[0]

    short_term_loans_schedule.loc["Borrowings"] = long_and_short_term_borrowing_df[
        "short_term_borrowing"
    ]

    short_term_loans_schedule.loc["Repayments"] = -capital_repayment_on_borrowings_df[
        "short_term_borrowing"
    ]

    short_term_loans_schedule = helper.calculate_opening_and_closing_balances(
        short_term_loans_schedule
    )

    return short_term_loans_schedule


def calculate_long_term_loans_schedules(
    long_and_short_term_borrowing_df: pd.DataFrame,
    capital_repayment_on_borrowings_df: pd.DataFrame,
    opening_balances: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    long_term_loans_schedule = pd.DataFrame(
        index=[
            "Opening Balance",
            "Borrowings",
            "Repayments",
            "Closing Balance",
        ],
        columns=helper.generate_columns(valuation_date, months_to_forecast),
    )

    long_term_loans_schedule.loc[
        "Opening Balance", long_term_loans_schedule.columns[0]
    ] = opening_balances["LONG_TERM_LOANS"].iat[0]

    long_term_loans_schedule.loc["Borrowings"] = long_and_short_term_borrowing_df[
        "long_term_borrowing"
    ]

    long_term_loans_schedule.loc["Repayments"] = -capital_repayment_on_borrowings_df[
        "long_term_borrowing"
    ]

    long_term_loans_schedule = helper.calculate_opening_and_closing_balances(
        long_term_loans_schedule
    )
    return long_term_loans_schedule
