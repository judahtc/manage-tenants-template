import numpy as np
import pandas as pd

from application.modeling import helper


def generate_statement_of_cashflow_template(
    valuation_date: str, months_to_forecast: int
):
    return pd.DataFrame(
        index=pd.Index(
            [
                "Profit/(loss) Per I/S",
                "Treasury Movements",
                "Adjustments For:",
                "Depreciation",
                "Interest Expense Accrued",
                "Other Non-Cash Items",
                "Cash From Operations Before WC",
                "Working Capital Movements",
                "(Increase)/Decrease In Receivables",
                "Increase/(Decrease) In Payables",
                "(Increase)/Decrease In Loan Book (Principle)",
                "(Increase)/Decrease In Loan Book (Interest)",
                "Increase/(Decrease) In Borrowings",
                "Cash From Operations After WC",
                "Interest Paid",
                "Tax Paid",
                "Net Cash Flow From Operations",
                "CASH FLOW FROM INVESTING ACTIVITIES",
                "Purchase Of Fixed Assets",
                "Cash Flow From Investing Activities",
                "CASH FLOW FROM FINANCING ACTIVITIES",
                "Increase/(Decrease) In Long Term Borrowings",
                "Increase/(Decrease) In Short Term Borrowings",
                "Repayment Of Borrowings",
                "Dividend Paid",
                "Cash Flow From Financing Activities",
                "Net Increase/(Decrease) In Cash",
                "Cash At Beginning Of Period",
                "Cash At End Of Period",
            ],
            name="STATEMENT_OF_CASHFLOWS",
        ),
        columns=helper.generate_columns(valuation_date, months_to_forecast),
    )


def calculate_cash_at_end_and_beginning_of_period(
    statement_of_cashflow_df: pd.DataFrame, opening_balances: pd.DataFrame
):
    statement_of_cashflow_df.loc[
        "Cash At Beginning Of Period", statement_of_cashflow_df.columns[0]
    ] = opening_balances["CASH_ON_HAND"].iat[0]

    cash_at_end_of_period_loc = statement_of_cashflow_df.index.get_loc(
        "Cash At End Of Period"
    )
    cash_at_beginning_of_period_loc = statement_of_cashflow_df.index.get_loc(
        "Cash At Beginning Of Period"
    )

    for index, period in enumerate(statement_of_cashflow_df.columns):
        statement_of_cashflow_df.iloc[
            cash_at_end_of_period_loc, index
        ] = statement_of_cashflow_df.iloc[
            cash_at_beginning_of_period_loc - 1 : cash_at_end_of_period_loc, index
        ].sum()

        if period == statement_of_cashflow_df.columns[-1]:
            break

        statement_of_cashflow_df.iloc[
            cash_at_beginning_of_period_loc, index + 1
        ] = statement_of_cashflow_df.iloc[cash_at_end_of_period_loc, index]

    return statement_of_cashflow_df
