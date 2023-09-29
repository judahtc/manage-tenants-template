import numpy as np
import pandas as pd

from application.modeling import helper


def calculate_sme_disbursements(
    sme_number_of_credit_officers: pd.Series,
    sme_average_loan_size: pd.Series,
    sme_number_of_loans_per_credit_officer: pd.Series,
):
    return (
        sme_number_of_credit_officers
        * sme_average_loan_size
        * sme_number_of_loans_per_credit_officer
    )


def calculate_consumer_ssb_disbursements(
    consumer_ssb_number_of_agents: pd.Series,
    consumer_ssb_average_loan_size: pd.Series,
    consumer_ssb_number_of_loans_per_agent: pd.Series,
):
    return (
        consumer_ssb_number_of_agents
        * consumer_ssb_average_loan_size
        * consumer_ssb_number_of_loans_per_agent
    )


def calculate_consumer_pvt_disbursements(
    consumer_pvt_number_of_agents: pd.Series,
    consumer_pvt_average_loan_size: pd.Series,
    consumer_pvt_number_of_loans_per_agent: pd.Series,
):
    return (
        consumer_pvt_number_of_agents
        * consumer_pvt_average_loan_size
        * consumer_pvt_number_of_loans_per_agent
    )


def calculate_b2b_disbursements(
    b2b_disbursements: pd.Series, start_date: str, periods: int
):
    return helper.add_period_index(
        b2b_disbursements, start_date=start_date, periods=periods
    )


def calculate_new_disbursements(parameters: pd.DataFrame):
    # Todo: Try to use a loop

    sme_disbursements = calculate_sme_disbursements(
        sme_number_of_credit_officers=parameters.loc["SME_NUMBER_OF_CREDIT_OFFICERS"],
        sme_average_loan_size=parameters.loc["SME_AVERAGE_LOAN_SIZE"],
        sme_number_of_loans_per_credit_officer=parameters.loc[
            "SME_NUMBER_OF_LOANS_PER_CREDIT_OFFICER"
        ],
    )

    consumer_pvt_disbursements = calculate_consumer_pvt_disbursements(
        consumer_pvt_number_of_agents=parameters.loc["CONSUMER_PVT_NUMBER_OF_AGENTS"],
        consumer_pvt_average_loan_size=parameters.loc["CONSUMER_PVT_AVERAGE_LOAN_SIZE"],
        consumer_pvt_number_of_loans_per_agent=parameters.loc[
            "CONSUMER_PVT_NUMBER_OF_LOANS_PER_AGENT"
        ],
    )

    consumer_ssb_disbursements = calculate_consumer_ssb_disbursements(
        consumer_ssb_number_of_agents=parameters.loc["CONSUMER_SSB_NUMBER_OF_AGENTS"],
        consumer_ssb_average_loan_size=parameters.loc["CONSUMER_SSB_AVERAGE_LOAN_SIZE"],
        consumer_ssb_number_of_loans_per_agent=parameters.loc[
            "CONSUMER_SSB_NUMBER_OF_LOANS_PER_AGENT"
        ],
    )

    b2b_disbursements = parameters.loc["B2B_DISBURSEMENTS"]

    total_disbursements = helper.add_series(
        [
            sme_disbursements,
            consumer_pvt_disbursements,
            consumer_pvt_disbursements,
            b2b_disbursements,
        ]
    )

    return pd.DataFrame(
        {
            "b2b_disbursements": b2b_disbursements,
            "sme_disbursements": sme_disbursements,
            "consumer_ssb_disbursements": consumer_ssb_disbursements,
            "consumer_pvt_disbursements": consumer_pvt_disbursements,
            "total": total_disbursements,
        }
    )