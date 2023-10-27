import numpy as np
import pandas as pd

from application.modeling import helper


def reindex_output(df: pd.DataFrame):
    columns = pd.DatetimeIndex(df.columns)
    index = pd.date_range(columns.min(), columns.max(), freq="M").strftime("%b-%Y")
    return df.T.reindex(index=index).T


def calculate_outstanding_on_straight_line_borrowings(
    effective_dates: pd.Series,
    tenures: pd.Series,
    amounts: pd.Series,
    loan_identifiers: pd.Series,
):
    array_of_series = []
    for i, _ in effective_dates.items():
        effective_date = effective_dates[i]
        tenure = tenures[i]
        amount = amounts[i]
        loan_identifier = loan_identifiers[i]

        array_of_series.append(
            pd.Series(
                data=amount,
                index=pd.period_range(
                    start=effective_date,
                    periods=tenure,
                    freq="M",
                ),
                name=loan_identifier,
            )
        )

    df = pd.concat(array_of_series, axis=1).T.fillna(0)
    df.columns = df.columns.strftime("%b-%Y")
    return df


def calculate_straight_line_payments(
    effective_dates: pd.Series,
    tenures: pd.Series,
    frequencies: pd.Series,
    amounts: pd.Series,
    loan_identifiers: pd.Series,
):
    amounts_results = []
    freq_key = {1: "12M", 2: "6M", 3: "4M", 4: "3M", 6: "2M", 12: "M"}

    years = tenures // 12
    number_of_payments = years * frequencies

    for i, _ in effective_dates.items():
        effective_date = effective_dates[i]
        tenure = tenures[i]
        frequency = frequencies[i]
        amount = amounts[i]
        loan_identifier = loan_identifiers[i]
        periods = number_of_payments[i]

        if frequency == 0:
            index = pd.date_range(
                start=(effective_date + pd.DateOffset(months=tenure)),
                periods=1,
                freq="D",
            ).strftime("%b-%Y")

        else:
            index = pd.date_range(
                effective_date + pd.DateOffset(months=12 // frequency),
                periods=periods,
                freq=freq_key[frequency],
            ).strftime("%b-%Y")

        amounts_results.append(pd.Series(amount, index=index, name=loan_identifier))

    return pd.concat(amounts_results, axis=1).T.fillna(0)


def calculate_interest(
    frequencies: pd.Series, annual_interest: pd.Series, tenures: pd.Series
):
    interest = pd.Series([], dtype=float)

    for i, v in frequencies.items():
        if frequencies[i] == 0:
            interest[i] = annual_interest[i] * tenures[i] / 12
        else:
            interest[i] = annual_interest[i] / frequencies[i]

    return interest


def calculate_straight_line_loans_schedules(
    interest_rates: pd.Series,
    effective_dates: pd.Series,
    frequencies: pd.Series,
    loan_identifiers: pd.Series,
    tenures: pd.Series,
    amounts: pd.Series,
):
    effective_dates = helper.convert_to_datetime(effective_dates)

    annual_interest = amounts * interest_rates

    interest = calculate_interest(
        frequencies=frequencies, annual_interest=annual_interest, tenures=tenures
    )

    interest_payments = calculate_straight_line_payments(
        effective_dates=effective_dates,
        tenures=tenures,
        frequencies=frequencies,
        amounts=interest,
        loan_identifiers=loan_identifiers,
    )

    capital_repayments = calculate_straight_line_payments(
        effective_dates=effective_dates,
        tenures=tenures,
        frequencies=frequencies * 0,
        amounts=amounts,
        loan_identifiers=loan_identifiers,
    )

    outstanding_balance = calculate_outstanding_on_straight_line_borrowings(
        effective_dates=effective_dates,
        tenures=tenures,
        amounts=amounts,
        loan_identifiers=loan_identifiers,
    )

    return {
        "interest_payments": interest_payments,
        "capital_repayments": capital_repayments,
        "outstanding_balance_at_start": outstanding_balance,
    }


def calculate_reducing_balance_loans_schedules(
    interest_rates: pd.Series,
    effective_dates: pd.Series,
    frequencies: pd.Series,
    loan_identifiers: pd.Series,
    tenures: pd.Series,
    amounts: pd.Series,
    is_interest_rate_annual: bool = True,
):
    outstanding_balances_results = []
    interest_payments_results = []
    capital_repayments_results = []

    effective_dates = helper.convert_to_datetime(effective_dates)

    freq_key = {1: "12M", 2: "6M", 3: "4M", 4: "3M", 6: "2M", 12: "M"}

    years = tenures / 12
    number_of_payments = years * frequencies

    if is_interest_rate_annual:
        effective_interest_rates = np.power(1 + interest_rates, 1 / frequencies) - 1
        print("interests divided")
    else:
        effective_interest_rates = interest_rates
        print("interest rates not divided")

    annuity_factor = (
        1 - (1 + effective_interest_rates) ** (-number_of_payments)
    ) / effective_interest_rates

    repayments = amounts / annuity_factor

    for i, _ in interest_rates.items():
        effective_date = effective_dates[i]
        frequency = frequencies[i]
        periods = number_of_payments[i]
        effective_interest_rate = effective_interest_rates[i]
        repayment = repayments[i]
        loan_identifier = loan_identifiers[i]

        index = pd.date_range(
            effective_date + pd.DateOffset(months=12 // frequency),
            periods=periods,
            freq=freq_key[frequency],
        ).strftime("%b-%Y")

        annuity_factors = (
            1
            - np.power(
                1 + effective_interest_rate,
                -np.arange(periods, 0, -1),
            )
        ) / effective_interest_rate

        outstanding_balances = annuity_factors * repayment

        outstanding_balances_results.append(
            pd.Series(
                outstanding_balances,
                index=index,
                name=loan_identifier,
            )
        )

        interest_payments = outstanding_balances * effective_interest_rate

        interest_payments_results.append(
            pd.Series(
                interest_payments,
                index=index,
                name=loan_identifier,
            )
        )

        capital_repayments_results.append(
            pd.Series(
                repayment - interest_payments,
                index=index,
                name=loan_identifier,
            )
        )

    repayments.index = loan_identifiers

    return {
        "outstanding_balance_at_start": pd.concat(
            outstanding_balances_results, axis=1
        ).T.fillna(0),
        "capital_repayments": pd.concat(capital_repayments_results, axis=1).T.fillna(0),
        "interest_payments": pd.concat(interest_payments_results, axis=1).T.fillna(0),
        "repayments": repayments,
    }


def calculate_borrowings_schedules(borrowings: pd.DataFrame):
    straight_line = borrowings.loc[borrowings["method"] == "straight_line"]
    reducing_balance = borrowings.loc[borrowings["method"] == "reducing_balance"]

    straight_line_loans_schedules = calculate_straight_line_loans_schedules(
        effective_dates=straight_line["effective_date"],
        tenures=straight_line["tenure"],
        frequencies=straight_line["frequency"],
        amounts=straight_line["nominal_amount"],
        loan_identifiers=straight_line["institution"],
        interest_rates=straight_line["interest_rate"],
    )

    reducing_balance_loans_schedules = calculate_reducing_balance_loans_schedules(
        interest_rates=reducing_balance["interest_rate"],
        effective_dates=reducing_balance["effective_date"],
        frequencies=reducing_balance["frequency"],
        loan_identifiers=reducing_balance["institution"],
        tenures=reducing_balance["tenure"],
        amounts=reducing_balance["nominal_amount"],
    )

    return {
        "interest_payments": reindex_output(
            pd.concat(
                [
                    straight_line_loans_schedules["interest_payments"],
                    reducing_balance_loans_schedules["interest_payments"],
                ]
            ).fillna(0)
        ),
        "capital_repayments": reindex_output(
            pd.concat(
                [
                    straight_line_loans_schedules["capital_repayments"],
                    reducing_balance_loans_schedules["capital_repayments"],
                ]
            ).fillna(0)
        ),
        "outstanding_balance_at_start": reindex_output(
            pd.concat(
                [
                    straight_line_loans_schedules["outstanding_balance_at_start"],
                    reducing_balance_loans_schedules["outstanding_balance_at_start"],
                ]
            ).fillna(0)
        ),
    }
