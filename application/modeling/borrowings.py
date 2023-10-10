import numpy as np
import pandas as pd

from application.modeling import helper


def reindex_output(df: pd.DataFrame):
    return df.T.reindex(
        pd.PeriodIndex(df.columns, freq="M").sort_values().strftime("%b-%Y"),
        fill_value=0,
    ).T


def calculate_straight_line_payments(
    effective_dates: pd.Series,
    tenures: pd.Series,
    frequencies: pd.Series,
    amounts: pd.Series,
    loan_identifiers: pd.Series,
):
    amounts_results = []
    freq_key = {1: "BA", 2: "2BQ", 4: "BQ", 12: "BM"}
    for i, _ in effective_dates.items():
        effective_date = effective_dates[i]
        tenure = tenures[i]
        frequency = frequencies[i]
        amount = amounts[i]
        loan_identifier = loan_identifiers[i]

        if frequency == 0:
            index = (
                pd.date_range(
                    effective_date + pd.DateOffset(months=tenure),
                    periods=1,
                    freq="A",
                )
            ).strftime("%b-%Y")

        elif frequency == 2:
            index = (
                pd.date_range(
                    effective_date + pd.DateOffset(months=6),
                    periods=frequency,
                    freq=freq_key[frequency],
                )
            ).strftime("%b-%Y")
        else:
            index = (
                pd.date_range(
                    effective_date, periods=frequency, freq=freq_key[frequency]
                )
            ).strftime("%b-%Y")

        amounts_results.append(pd.Series(amount, index=index, name=loan_identifier))

    return pd.concat(amounts_results, axis=1).T.fillna(0)


def calculate_interest(frequencies: pd.Series, annual_interest: pd.Series):
    interest = pd.Series([], dtype=float)

    for i, v in frequencies.items():
        if frequencies[i] == 0:
            interest[i] = 0
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
        frequencies=frequencies, annual_interest=annual_interest
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

    return {
        "interest_payments": reindex_output(interest_payments),
        "capital_repayments": reindex_output(capital_repayments),
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

    freq_key = {1: "BA", 4: "BQ", 12: "BM"}

    years = tenures / 12
    number_of_payments = years * frequencies

    if is_interest_rate_annual:
        effective_interest_rate = np.power(1 + interest_rates, 1 / frequencies) - 1
    else:
        effective_interest_rate = interest_rates

    annuity_factor = (
        1 - (1 + effective_interest_rate) ** (-number_of_payments)
    ) / effective_interest_rate

    repayment = amounts / annuity_factor

    for index, _ in interest_rates.items():
        series_index = pd.date_range(
            effective_dates[index],
            periods=number_of_payments[index],
            freq=freq_key[frequencies[index]],
        ).strftime("%b-%Y")

        annuity_factors = (
            1
            - np.power(
                1 + effective_interest_rate[index],
                -np.arange(number_of_payments[index], 0, -1),
            )
        ) / effective_interest_rate[index]

        outstanding_balances = annuity_factors * repayment[index]

        outstanding_balances_results.append(
            pd.Series(
                outstanding_balances,
                index=series_index,
                name=loan_identifiers[index],
            )
        )

        interest_payments = outstanding_balances * effective_interest_rate[index]

        interest_payments_results.append(
            pd.Series(
                interest_payments,
                index=series_index,
                name=loan_identifiers[index],
            )
        )

        capital_repayments_results.append(
            pd.Series(
                repayment[index] - interest_payments,
                index=series_index,
                name=loan_identifiers[index],
            )
        )

    repayment.index = loan_identifiers

    return {
        "outstanding_balance_at_start": reindex_output(
            pd.concat(outstanding_balances_results, axis=1).T.fillna(0)
        ),
        "capital_repayments": reindex_output(
            pd.concat(capital_repayments_results, axis=1).T.fillna(0)
        ),
        "interest_payments": reindex_output(
            pd.concat(interest_payments_results, axis=1).T.fillna(0)
        ),
        "repayments": repayment,
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
    }
