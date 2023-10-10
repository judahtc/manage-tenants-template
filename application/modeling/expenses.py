import numpy as np
import pandas as pd

from application.modeling import helper


def calculate_agent_commission(
    consumer_pvt_disbursements: pd.Series,
    consumer_ssb_disbursements: pd.Series,
    agent_commission_percentage: pd.Series,
):
    agent_commission = agent_commission_percentage * (
        consumer_pvt_disbursements + consumer_ssb_disbursements
    )
    return agent_commission


def calculate_credit_officer_salaries(
    credit_officer_salary: pd.Series,
    sme_number_of_credit_officers: pd.Series,
):
    credit_officer_salaries = credit_officer_salary * sme_number_of_credit_officers
    return credit_officer_salaries


def calculate_credit_officer_commission(
    sme_disbursements: pd.Series,
    credit_officer_commission: pd.Series,
):
    credit_officer_commission = credit_officer_commission * sme_disbursements
    return credit_officer_commission


# def calculate_total_salaries(
#     agent_commission: pd.Series,
#     credit_officer_commission: pd.Series
#     credit_officer_salaries: pd.Series,
#     other_staff_salary: pd.Series,
#     months_to_forecast: int,
#     valuation_date: str,
# ):
#     total_salaries = agent_commission + credit_officer_salaries + other_staff_salary
#     total_salaries.index = helper.generate_columns(valuation_date, months_to_forecast)
#     return total_salaries


def calculate_provision_for_bad_debts(
    trade_receivables: pd.Series,
    trade_receivables_provision_for_bad_debts_percentage: float,
    valuation_date: str,
    months_to_forecast: int,
):
    provision_for_bad_debts = (
        trade_receivables * trade_receivables_provision_for_bad_debts_percentage
    )
    provision_for_bad_debts.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )
    return provision_for_bad_debts


def calculate_uncertain_expenses(
    expenses_uncertain: pd.DataFrame,
    other_parameters: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    inflation_rates = (other_parameters.loc["INFLATION_RATE"] + 1).cumprod()
    inflation_rates.index = helper.generate_columns(valuation_date, months_to_forecast)
    mean_expenses = pd.DataFrame(
        np.repeat(expenses_uncertain.T.mean(), 12).values.reshape(
            expenses_uncertain.index.shape[0], months_to_forecast
        ),
        index=expenses_uncertain.index,
    )
    mean_expenses.columns = helper.generate_columns(
        valuation_date, period=months_to_forecast
    )
    return mean_expenses * inflation_rates


def calculate_pensions_and_statutory_contributions(
    salaries: pd.Series,
    pensions_and_statutory_contributions_percentage: pd.Series,
    valuation_date: str,
    months_to_forecast: int,
):
    pensions_and_statutory_contributions_percentage.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )
    pensions_and_statutory_contributions = (
        salaries * pensions_and_statutory_contributions_percentage
    )
    return pensions_and_statutory_contributions


def calculate_change_in_provision_for_credit_loss(
    provision_for_credit_loss: pd.Series,
    provision_for_credit_loss_opening_balances: float,
    months_to_forecast: int,
    valuation_date: str,
):
    provision_for_credit_loss.index = pd.PeriodIndex(
        provision_for_credit_loss.index, freq="M"
    )

    provision_for_credit_loss.loc[provision_for_credit_loss.index[0] - 1] = float(
        provision_for_credit_loss_opening_balances
    )

    provision_for_credit_loss = provision_for_credit_loss.sort_index().diff().dropna()
    provision_for_credit_loss.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )
    return provision_for_credit_loss


def calculate_business_acqusition(
    business_acquisition_percent: float,
    agent_contribution_percent: float,
    consumer_ssb_disbursements: pd.Series,
    consumer_pvt_disbursements: pd.Series,
):
    consumer_ssb = business_acquisition_percent * consumer_ssb_disbursements
    agent_contribution = agent_contribution_percent * consumer_pvt_disbursements
    acquisition = agent_contribution * business_acquisition_percent
    total_acquisition = consumer_ssb + acquisition
    total_acquisition.index = pd.PeriodIndex(total_acquisition.index, freq="M")
    total_acquisition.index = total_acquisition.index.strftime("%b-%Y")

    return total_acquisition


def calculate_interest_expense(df, to_pad, idx):
    remaining_loan_term = df.loc[idx, "remaining_loan_term"]
    loan_interest = df.loc[idx, "interest"]
    interest = np.pad(
        np.repeat(loan_interest, remaining_loan_term), (0, to_pad - remaining_loan_term)
    )
    return interest


def convert_interest_array_to_series(interest_array, start_date):
    return pd.Series(
        interest_array, index=helper.generate_columns(start_date, len(interest_array))
    )


def reindex_columns(df, valuation_date, months_to_forecast):
    return df.reindex(
        helper.generate_columns(valuation_date, months_to_forecast),
        axis=1,
        fill_value=0,
    )


def determine_start_date(df, valuation_date, idx):
    if "loan_start_date" in df.columns:
        start_date = df.loc[idx, "loan_start_date"]
    else:
        start_date = valuation_date
    return start_date


def calculate_interest_expense_on_borrowing(
    details_of_borrowing, months_to_forecast, valuation_date
):
    interests = []
    temp = details_of_borrowing.copy()
    temp = temp.assign(
        interest=(
            details_of_borrowing["interest_rate"] * details_of_borrowing["principal"]
        )
        / temp["loan_term"]
    )
    to_pad = temp.loan_term.max()
    for i in range(len(temp)):
        start_date = determine_start_date(temp, valuation_date=valuation_date, idx=i)
        interest = calculate_interest_expense(temp, to_pad=to_pad, idx=i)
        interest = convert_interest_array_to_series(
            interest_array=interest, start_date=start_date
        )
        interests.append(interest)
    interest_expense = pd.concat(interests, axis=1).T.fillna(0)
    interest_expense = reindex_columns(
        interest_expense,
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )
    interest_expense.index = temp["company"]
    return interest_expense


def aggregate_interest_expense_short_term_borrowing(
    interest_expense_existing_short_term_borrowing,
    interest_expense_new_short_term_borrowing,
):
    return interest_expense_existing_short_term_borrowing.add(
        interest_expense_new_short_term_borrowing, fill_value=0
    )


def aggregate_interest_expense_long_term_borrowing(
    interest_expense_existing_long_term_borrowing,
    interest_expense_new_long_term_borrowing,
):
    return interest_expense_existing_long_term_borrowing.add(
        interest_expense_new_long_term_borrowing, fill_value=0
    )


def aggregate_interest_expense_on_borrowing(
    interest_expense_short_term_borrowing, interest_expense_long_term_borrowing
):
    return interest_expense_short_term_borrowing.add(
        interest_expense_long_term_borrowing, fill_value=0
    )


def calculate_provision_for_credit_loss(
    disbursements: pd.Series, provision_for_credit_loss: float
):
    return helper.change_period_index_to_strftime(
        (disbursements * provision_for_credit_loss)
    )


def calculate_provision_for_credit_loss_for_all_new_disbursements(
    new_disbursements_df: pd.DataFrame, disbursement_parameters: pd.DataFrame
):
    sme_provision_for_credit_loss = calculate_provision_for_credit_loss(
        new_disbursements_df["sme_disbursements"],
        disbursement_parameters.loc["SME_PROVISION_FOR_CREDIT_LOSS"],
    )
    b2b_provision_for_credit_loss = calculate_provision_for_credit_loss(
        new_disbursements_df["b2b_disbursements"],
        disbursement_parameters.loc["B2B_PROVISION_FOR_CREDIT_LOSS"],
    )
    consumer_ssb_provision_for_credit_loss = calculate_provision_for_credit_loss(
        new_disbursements_df["consumer_ssb_disbursements"],
        disbursement_parameters.loc["CONSUMER_SSB_PROVISION_FOR_CREDIT_LOSS"],
    )
    consumer_pvt_provision_for_credit_loss = calculate_provision_for_credit_loss(
        new_disbursements_df["consumer_pvt_disbursements"],
        disbursement_parameters.loc["CONSUMER_PVT_PROVISION_FOR_CREDIT_LOSS"],
    )

    provision_for_credit_loss_new_disbursements = helper.add_series(
        [
            sme_provision_for_credit_loss,
            b2b_provision_for_credit_loss,
            consumer_ssb_provision_for_credit_loss,
            consumer_pvt_provision_for_credit_loss,
        ]
    )

    return pd.DataFrame(
        {
            "sme_provision_for_credit_loss": sme_provision_for_credit_loss,
            "b2b_provision_for_credit_loss": b2b_provision_for_credit_loss,
            "consumer_ssb_provision_for_credit_loss": consumer_ssb_provision_for_credit_loss,
            "consumer_pvt_provision_for_credit_loss": consumer_pvt_provision_for_credit_loss,
            "total": provision_for_credit_loss_new_disbursements,
        }
    )


def calculate_salaries_and_pension_and_statutory_contributions(
    new_disbursements_df: pd.DataFrame,
    disbursement_parameters: pd.DataFrame,
    other_parameters: pd.DataFrame,
    months_to_forecast: int,
    valuation_date: str,
):
    other_staff_salary = other_parameters.loc["OTHER_STAFF_SALARY"]
    pensions_and_statutory_contributions_percentage = other_parameters.loc[
        "PENSION_AND_STATUROTY_CONTRIBUTIONS_PERCENT"
    ]

    agent_commission = calculate_agent_commission(
        consumer_pvt_disbursements=new_disbursements_df["consumer_pvt_disbursements"],
        consumer_ssb_disbursements=new_disbursements_df["consumer_ssb_disbursements"],
        agent_commission_percentage=disbursement_parameters.loc["AGENT_COMMISSION"],
    )

    credit_officer_salaries = calculate_credit_officer_salaries(
        credit_officer_salary=disbursement_parameters.loc["CREDIT_OFFICER_SALARY"],
        sme_number_of_credit_officers=disbursement_parameters.loc[
            "SME_NUMBER_OF_CREDIT_OFFICERS"
        ],
    )

    credit_officer_commission = calculate_credit_officer_commission(
        sme_disbursements=new_disbursements_df["sme_disbursements"],
        credit_officer_commission=disbursement_parameters.loc[
            "CREDIT_OFFICER_COMMISSION"
        ],
    )

    # total_salaries = calculate_total_salaries(
    #     agent_commission=agent_commission,
    #     credit_officer_commission=credit_officer_commission,
    #     credit_officer_salaries=credit_officer_salaries,
    #     other_staff_salary=parameters.loc["OTHER_STAFF_SALARY"],
    #     months_to_forecast=months_to_forecast,
    #     valuation_date=valuation_date,
    # )

    total_salaries = (
        agent_commission
        + credit_officer_salaries
        + other_staff_salary
        + credit_officer_commission
    )

    total_salaries = helper.change_period_index_to_strftime(total_salaries)

    pensions_and_statutory_contributions_percentage.index = helper.generate_columns(
        valuation_date, months_to_forecast
    )

    pensions_and_statutory_contributions = (
        credit_officer_salaries + other_staff_salary
    ) * pensions_and_statutory_contributions_percentage

    return pd.DataFrame(
        {
            "agent_commission": agent_commission.values,
            "credit_officer_salaries": credit_officer_salaries.values,
            "credit_officer_commission": credit_officer_commission.values,
            "other_staff_salaries": other_staff_salary.values,
            "total": total_salaries.values,
            "pensions_and_statutory_contributions": pensions_and_statutory_contributions,
        },
        index=total_salaries.index,
    )


def calculate_finance_costs(
    details_of_existing_long_term_borrowing: pd.DataFrame,
    details_of_existing_short_term_borrowing: pd.DataFrame,
    details_of_new_short_term_borrowing: pd.DataFrame,
    details_of_new_long_term_borrowing: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    interest_expense_existing_long_term_borrowing = (
        calculate_interest_expense_on_borrowing(
            details_of_borrowing=details_of_existing_long_term_borrowing,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
        )
    )

    interest_expense_existing_short_term_borrowing = (
        calculate_interest_expense_on_borrowing(
            details_of_borrowing=details_of_existing_short_term_borrowing,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
        )
    )

    interest_expense_new_short_term_borrowing = calculate_interest_expense_on_borrowing(
        details_of_borrowing=details_of_new_short_term_borrowing,
        months_to_forecast=months_to_forecast,
        valuation_date=valuation_date,
    )

    interest_expense_new_long_term_borrowing = calculate_interest_expense_on_borrowing(
        details_of_borrowing=details_of_new_long_term_borrowing,
        months_to_forecast=months_to_forecast,
        valuation_date=valuation_date,
    )

    interest_expense_short_term_borrowing = (
        aggregate_interest_expense_short_term_borrowing(
            interest_expense_existing_short_term_borrowing,
            interest_expense_new_short_term_borrowing,
        )
    )

    interest_expense_long_term_borrowing = (
        aggregate_interest_expense_long_term_borrowing(
            interest_expense_existing_long_term_borrowing,
            interest_expense_new_long_term_borrowing,
        )
    )

    interest_expense = aggregate_interest_expense_long_term_borrowing(
        interest_expense_short_term_borrowing, interest_expense_long_term_borrowing
    )
    interest_expense.loc["total"] = interest_expense.sum()

    return interest_expense
