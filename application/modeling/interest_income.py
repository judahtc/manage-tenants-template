import numpy as np
import pandas as pd

from application.modeling import helper


def calculate_repayment_amount(disbursements, monthly_interest_rate, average_loan_term):
    annuity_factor = (
        1 - ((1 + monthly_interest_rate) ** (-average_loan_term))
    ) / monthly_interest_rate
    return disbursements / annuity_factor


def generate_columns(start_date: str, period: int):
    return pd.period_range(start=start_date, periods=period, freq="M").strftime("%b-%Y")


def generate_loan_schedule_new_disbursements(
    disbursements: pd.Series,
    monthly_interest_rate: float,
    repayment_amount: pd.Series,
    months_to_forecast: int,
) -> dict:
    number_of_disbursements = len(disbursements)

    outstanding_at_start = np.zeros((number_of_disbursements, months_to_forecast))
    interest = np.zeros((number_of_disbursements, months_to_forecast))
    capital_repayments = np.zeros((number_of_disbursements, months_to_forecast))
    outstanding_at_start[:, 0] = disbursements.values

    for i in range(months_to_forecast):
        interest[:, i] = outstanding_at_start[:, i] * monthly_interest_rate
        capital_repayments[:, i] = np.where(
            outstanding_at_start[:, i] < repayment_amount.values - interest[:, i],
            outstanding_at_start[:, i],
            repayment_amount.values - interest[:, i],
        )
        if i + 1 == months_to_forecast:
            break
        outstanding_at_start[:, i + 1] = np.maximum(
            outstanding_at_start[:, i] - capital_repayments[:, i], 0
        )
    date_of_disbursement = disbursements.index
    columns = generate_columns(date_of_disbursement[1], months_to_forecast)
    return {
        "interest": helper.shift(
            pd.DataFrame(interest, index=date_of_disbursement, columns=columns)
        ),
        "capital_repayments": helper.shift(
            pd.DataFrame(
                capital_repayments, index=date_of_disbursement, columns=columns
            )
        ),
        "outstanding_at_start": helper.shift(
            pd.DataFrame(
                outstanding_at_start, index=date_of_disbursement, columns=columns
            )
        ),
    }


def calculate_monthly_repayments_new_disbursements(
    new_disbursements_df: pd.DataFrame, parameters: pd.DataFrame
):
    # Todo: Try using a loop on this one

    sme_monthly_repayment = calculate_repayment_amount(
        disbursements=new_disbursements_df["sme_disbursements"],
        monthly_interest_rate=parameters.loc["SME_INTEREST_RATE"],
        average_loan_term=parameters.loc["SME_AVERAGE_LOAN_TERM"],
    )

    consumer_ssb_monthly_repayment = calculate_repayment_amount(
        disbursements=new_disbursements_df["consumer_ssb_disbursements"],
        monthly_interest_rate=parameters.loc["CONSUMER_SSB_INTEREST_RATE"],
        average_loan_term=parameters.loc["CONSUMER_SSB_AVERAGE_LOAN_TERM"],
    )
    consumer_pvt_monthly_repayment = calculate_repayment_amount(
        disbursements=new_disbursements_df["consumer_pvt_disbursements"],
        monthly_interest_rate=parameters.loc["CONSUMER_PVT_INTEREST_RATE"],
        average_loan_term=parameters.loc["CONSUMER_PVT_AVERAGE_LOAN_TERM"],
    )

    b2b_monthly_repayment = calculate_repayment_amount(
        disbursements=new_disbursements_df["b2b_disbursements"],
        monthly_interest_rate=parameters.loc["B2B_INTEREST_RATE"],
        average_loan_term=parameters.loc["B2B_AVERAGE_LOAN_TERM"],
    )

    return pd.DataFrame(
        {
            "sme_monthly_repayment": sme_monthly_repayment,
            "b2b_monthly_repayment": b2b_monthly_repayment,
            "consumer_ssb_monthly_repayment": consumer_ssb_monthly_repayment,
            "consumer_pvt_monthly_repayment": consumer_pvt_monthly_repayment,
            "total": helper.add_series([b2b_monthly_repayment, consumer_pvt_monthly_repayment
                                        ,consumer_ssb_monthly_repayment,sme_monthly_repayment])
        }
    )


def generate_loan_schedules_existing_loans(
    outstanding_balance: pd.Series,
    interest_rate_monthly: pd.Series,
    repayment_amount_monthly: pd.Series,
    valuation_date: str,
    months_to_project: int = 12 * 10,
):
    number_of_loans = outstanding_balance.shape[0]

    outstanding_at_start = np.zeros((number_of_loans, months_to_project))
    interest = np.zeros((number_of_loans, months_to_project))
    capital_repayment = np.zeros((number_of_loans, months_to_project))

    outstanding_at_start[:, 0] = outstanding_balance.values
    for i in range(months_to_project - 1):
        interest[:, i] = outstanding_at_start[:, i] * interest_rate_monthly
        capital_repayment[:, i] = np.where(
            outstanding_at_start[:, i] == 0,
            0,
            repayment_amount_monthly - interest[:, i],
        )
        outstanding_at_start[:, i + 1] = np.maximum(
            0, outstanding_at_start[:, i] - capital_repayment[:, i]
        )

    index = outstanding_balance.index
    columns = helper.generate_columns(valuation_date, months_to_project)

    return {
        "interest": pd.DataFrame(interest, index=index, columns=columns),
        "capital_repayment": pd.DataFrame(
            capital_repayment, index=index, columns=columns
        ),
        "outstanding_at_start": pd.DataFrame(
            outstanding_at_start, index=index, columns=columns
        ),
    }


def aggregate_new_and_existing_loans_interest_income(
    interest_income_new_disbursements_df: pd.Series,
    interest_income_existing_loans: pd.Series,
    valuation_date: str,
    months_to_forecast: int,
):
    return (
        interest_income_new_disbursements_df['total']
        .add(interest_income_existing_loans, fill_value=0)
        .reindex(helper.generate_columns(valuation_date, months_to_forecast))
    )


def generate_loan_schedules_for_all_new_disbursements(
    new_disbursements_df: pd.DataFrame,
    parameters: pd.DataFrame,
    monthly_repayment_new_disbursements_df: pd.DataFrame,
    months_to_forecast: int,
):
    # Todo: Try using a loop

    sme_loan_schedules = generate_loan_schedule_new_disbursements(
        disbursements=new_disbursements_df["sme_disbursements"],
        monthly_interest_rate=parameters.loc["SME_INTEREST_RATE"],
        repayment_amount=monthly_repayment_new_disbursements_df[
            "sme_monthly_repayment"
        ],
        months_to_forecast=months_to_forecast,
    )

    consumer_ssb_loan_schedules = generate_loan_schedule_new_disbursements(
        disbursements=new_disbursements_df["consumer_ssb_disbursements"],
        monthly_interest_rate=parameters.loc["CONSUMER_SSB_INTEREST_RATE"],
        repayment_amount=monthly_repayment_new_disbursements_df[
            "consumer_ssb_monthly_repayment"
        ],
        months_to_forecast=months_to_forecast,
    )

    b2b_loan_schedules = generate_loan_schedule_new_disbursements(
        disbursements=new_disbursements_df["b2b_disbursements"],
        monthly_interest_rate=parameters.loc["B2B_INTEREST_RATE"],
        repayment_amount=monthly_repayment_new_disbursements_df[
            "b2b_monthly_repayment"
        ],
        months_to_forecast=months_to_forecast,
    )

    consumer_pvt_loan_schedules = generate_loan_schedule_new_disbursements(
        disbursements=new_disbursements_df["consumer_pvt_disbursements"],
        monthly_interest_rate=parameters.loc["CONSUMER_PVT_INTEREST_RATE"],
        repayment_amount=monthly_repayment_new_disbursements_df[
            "consumer_pvt_monthly_repayment"
        ],
        months_to_forecast=months_to_forecast,
    )

    return {
        "sme_loan_schedules": sme_loan_schedules,
        "b2b_loan_schedules": b2b_loan_schedules,
        "consumer_ssb_loan_schedules": consumer_ssb_loan_schedules,
        "consumer_pvt_loan_schedules": consumer_pvt_loan_schedules,
    }


def generate_capital_repayment_new_disbursements_df(
    loan_schedules_for_all_new_disbursements: dict,
):
    # ? Can't we clean the code below?
    capital_repayment_new_disbursements_df = pd.DataFrame(
        {
            "sme_capital_repayments": loan_schedules_for_all_new_disbursements[
                "sme_loan_schedules"
            ]["capital_repayments"].sum(),
            "b2b_capital_repayments": loan_schedules_for_all_new_disbursements[
                "b2b_loan_schedules"
            ]["capital_repayments"].sum(),
            "consumer_pvt_capital_repayments": loan_schedules_for_all_new_disbursements[
                "consumer_pvt_loan_schedules"
            ]["capital_repayments"].sum(),
            "consumer_ssb_capital_repayments": loan_schedules_for_all_new_disbursements[
                "consumer_ssb_loan_schedules"
            ]["capital_repayments"].sum(),
            "total": helper.add_series(
                [
                    loan_schedules_for_all_new_disbursements["sme_loan_schedules"][
                        "capital_repayments"
                    ].sum(),
                    loan_schedules_for_all_new_disbursements["b2b_loan_schedules"][
                        "capital_repayments"
                    ].sum(),
                    loan_schedules_for_all_new_disbursements[
                        "consumer_pvt_loan_schedules"
                    ]["capital_repayments"].sum(),
                    loan_schedules_for_all_new_disbursements[
                        "consumer_ssb_loan_schedules"
                    ]["capital_repayments"].sum(),
                ]
            ),
        }
    )
    return capital_repayment_new_disbursements_df


def generate_interest_income_new_disbursements_df(
    loan_schedules_for_all_new_disbursements: pd.DataFrame,
):
    # ? Can't we clean the code below?

    interest_income_new_disbursements_df = pd.DataFrame(
        {
            "sme_interest_income": loan_schedules_for_all_new_disbursements[
                "sme_loan_schedules"
            ]["interest"].sum(),
            "b2b_interest_income": loan_schedules_for_all_new_disbursements[
                "b2b_loan_schedules"
            ]["interest"].sum(),
            "consumer_pvt_interest_income": loan_schedules_for_all_new_disbursements[
                "consumer_pvt_loan_schedules"
            ]["interest"].sum(),
            "consumer_ssb_interest_income": loan_schedules_for_all_new_disbursements[
                "consumer_ssb_loan_schedules"
            ]["interest"].sum(),
            "total": helper.add_series(
                [
                    loan_schedules_for_all_new_disbursements["sme_loan_schedules"][
                        "interest"
                    ].sum(),
                    loan_schedules_for_all_new_disbursements["b2b_loan_schedules"][
                        "interest"
                    ].sum(),
                    loan_schedules_for_all_new_disbursements[
                        "consumer_pvt_loan_schedules"
                    ]["interest"].sum(),
                    loan_schedules_for_all_new_disbursements[
                        "consumer_ssb_loan_schedules"
                    ]["interest"].sum(),
                ]
            ),
        }
    )
    return interest_income_new_disbursements_df
