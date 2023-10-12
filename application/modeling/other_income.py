import numpy as np
import pandas as pd

from application.modeling import borrowings, helper


def calculate_admin_fee_new_disbursements(
    disbursements: pd.Series,
    admin_fee_percentage: float,
    average_loan_term: int,
    months_to_forecast: int | pd.Series,
) -> pd.Series:
    average_loan_term = int(average_loan_term.mean())

    admin_fee = disbursements * admin_fee_percentage / average_loan_term

    admin_fee = np.repeat(admin_fee.values, average_loan_term).reshape(
        len(admin_fee), average_loan_term
    )
    admin_fee = pd.DataFrame(
        admin_fee,
        columns=helper.generate_columns(disbursements.index[0], average_loan_term),
        index=disbursements.index,
    )
    admin_fee = admin_fee.reindex(
        helper.generate_columns(disbursements.index[0], months_to_forecast), axis=1
    )
    return helper.shift(admin_fee).sum()


def calculate_credit_insurance_fee_new_disbursements(
    disbursements: pd.Series,
    credit_insurance_fee_percentage: float,
    average_loan_term: int,
    months_to_forecast: int | pd.Series,
) -> pd.Series:
    average_loan_term = int(average_loan_term.mean())

    credit_insurance_fee = (
        disbursements * credit_insurance_fee_percentage / average_loan_term
    )
    credit_insurance_fee = np.repeat(
        credit_insurance_fee.values, average_loan_term
    ).reshape(len(credit_insurance_fee), average_loan_term)
    credit_insurance_fee = pd.DataFrame(
        credit_insurance_fee,
        columns=helper.generate_columns(disbursements.index[0], average_loan_term),
        index=disbursements.index,
    )
    credit_insurance_fee = credit_insurance_fee.reindex(
        helper.generate_columns(disbursements.index[0], months_to_forecast), axis=1
    )
    return helper.shift(credit_insurance_fee).sum()


def calculate_admin_fee_existing_loans(
    loan_amount: pd.Series,
    admin_fee_percentage: pd.Series,
    loan_term_months: pd.Series,
    remaining_term_months: pd.Series,
    months_to_forecast: int,
    start_date: str,
):
    admin_fee_existing = loan_amount * admin_fee_percentage / loan_term_months

    admin_fees_existing_loans = []
    for i in admin_fee_existing.index:
        temp = pd.Series(
            np.repeat(
                admin_fee_existing.loc[i],
                remaining_term_months.loc[i],
            ),
            name=i,
        )
        admin_fees_existing_loans.append(temp)

    admin_fees_existing_loans = pd.concat(admin_fees_existing_loans, axis=1).T.fillna(0)

    admin_fees_existing_loans = admin_fees_existing_loans.iloc[:, :months_to_forecast]

    admin_fees_existing_loans.columns = helper.generate_columns(
        start_date, period=months_to_forecast
    )
    admin_fees_existing_loans

    return admin_fees_existing_loans.sum()


def calculate_credit_insurance_fee_existing_loans(
    loan_amount: pd.Series,
    credit_insurance_fee_percentage: pd.Series,
    loan_term_months: pd.Series,
    remaining_term_months: pd.Series,
    months_to_forecast: int,
    start_date: str,
):
    credit_insurance_fee_existing = (
        loan_amount * credit_insurance_fee_percentage / loan_term_months
    )

    credit_insurance_fees_existing_loans = []
    for i in credit_insurance_fee_existing.index:
        temp = pd.Series(
            np.repeat(
                credit_insurance_fee_existing.loc[i],
                remaining_term_months.loc[i],
            ),
            name=i,
        )
        credit_insurance_fees_existing_loans.append(temp)
    credit_insurance_fees_existing_loans = pd.concat(
        credit_insurance_fees_existing_loans, axis=1
    ).T.fillna(0)
    credit_insurance_fees_existing_loans = credit_insurance_fees_existing_loans.iloc[
        :, :months_to_forecast
    ]
    credit_insurance_fees_existing_loans.columns = helper.generate_columns(
        start_date, period=months_to_forecast
    )
    credit_insurance_fees_existing_loans

    return credit_insurance_fees_existing_loans.sum()


def aggregate_new_and_existing_loans_insurance_fee(
    insurance_fee_existing: pd.Series, insurance_fee_new: pd.Series
):
    return insurance_fee_new.add(insurance_fee_existing, fill_value=0)


def aggregate_other_income(
    total_insurance_fee: pd.Series, total_interest_income: pd.Series
):
    return total_insurance_fee.add(total_interest_income, fill_value=0)


def calculate_admin_fee_for_all_new_disbursements(
    new_disbursements_df: pd.DataFrame,
    disbursement_parameters: pd.DataFrame,
    months_to_forecast: int,
):
    sme_admin_fee = calculate_admin_fee_new_disbursements(
        disbursements=new_disbursements_df["sme_disbursements"],
        admin_fee_percentage=disbursement_parameters.loc["SME_ADMINISTRATION_FEE"],
        average_loan_term=disbursement_parameters.loc["SME_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    b2b_admin_fee = calculate_admin_fee_new_disbursements(
        disbursements=new_disbursements_df["b2b_disbursements"],
        admin_fee_percentage=disbursement_parameters.loc["B2B_ADMINISTRATION_FEE"],
        average_loan_term=disbursement_parameters.loc["B2B_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    consumer_ssb_admin_fee = calculate_admin_fee_new_disbursements(
        disbursements=new_disbursements_df["consumer_ssb_disbursements"],
        admin_fee_percentage=disbursement_parameters.loc[
            "CONSUMER_SSB_ADMINISTRATION_FEE"
        ],
        average_loan_term=disbursement_parameters.loc["CONSUMER_SSB_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    consumer_pvt_admin_fee = calculate_admin_fee_new_disbursements(
        disbursements=new_disbursements_df["consumer_pvt_disbursements"],
        admin_fee_percentage=disbursement_parameters.loc[
            "CONSUMER_PVT_ADMINISTRATION_FEE"
        ],
        average_loan_term=disbursement_parameters.loc["CONSUMER_PVT_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    admin_fee_new_disbursements = helper.add_series(
        [consumer_pvt_admin_fee, consumer_ssb_admin_fee, b2b_admin_fee, sme_admin_fee]
    )
    return pd.DataFrame(
        {
            "sme_admin_fee": sme_admin_fee,
            "b2b_admin_fee": b2b_admin_fee,
            "consumer_ssb_admin_fee": consumer_ssb_admin_fee,
            "consumer_pvt_admin_fee": consumer_pvt_admin_fee,
            "total": admin_fee_new_disbursements,
        }
    )


def calculate_credit_insurance_fee_for_all_new_disbursements(
    new_disbursements_df: pd.DataFrame,
    disbursement_parameters: pd.DataFrame,
    months_to_forecast: int,
):
    sme_credit_insurance_fee = calculate_credit_insurance_fee_new_disbursements(
        new_disbursements_df["sme_disbursements"],
        credit_insurance_fee_percentage=disbursement_parameters.loc[
            "SME_CREDIT_INSURANCE_FEE"
        ],
        average_loan_term=disbursement_parameters.loc["SME_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    b2b_credit_insurance_fee = calculate_credit_insurance_fee_new_disbursements(
        new_disbursements_df["b2b_disbursements"],
        credit_insurance_fee_percentage=disbursement_parameters.loc[
            "B2B_CREDIT_INSURANCE_FEE"
        ],
        average_loan_term=disbursement_parameters.loc["B2B_AVERAGE_LOAN_TERM"],
        months_to_forecast=months_to_forecast,
    )
    consumer_ssb_credit_insurance_fee = (
        calculate_credit_insurance_fee_new_disbursements(
            new_disbursements_df["consumer_ssb_disbursements"],
            credit_insurance_fee_percentage=disbursement_parameters.loc[
                "CONSUMER_SSB_CREDIT_INSURANCE_FEE"
            ],
            average_loan_term=disbursement_parameters.loc[
                "CONSUMER_SSB_AVERAGE_LOAN_TERM"
            ],
            months_to_forecast=months_to_forecast,
        )
    )
    consumer_pvt_credit_insurance_fee = (
        calculate_credit_insurance_fee_new_disbursements(
            new_disbursements_df["consumer_pvt_disbursements"],
            credit_insurance_fee_percentage=disbursement_parameters.loc[
                "CONSUMER_PVT_CREDIT_INSURANCE_FEE"
            ],
            average_loan_term=disbursement_parameters.loc[
                "CONSUMER_PVT_AVERAGE_LOAN_TERM"
            ],
            months_to_forecast=months_to_forecast,
        )
    )

    credit_insurance_fee_new_disbursements = helper.add_series(
        [
            consumer_pvt_credit_insurance_fee,
            consumer_ssb_credit_insurance_fee,
            b2b_credit_insurance_fee,
            sme_credit_insurance_fee,
        ]
    )

    return pd.DataFrame(
        {
            "sme_credit_insurance_fee": sme_credit_insurance_fee,
            "b2b_credit_insurance_fee": b2b_credit_insurance_fee,
            "consumer_ssb_credit_insurance_fee": consumer_ssb_credit_insurance_fee,
            "consumer_pvt_credit_insurance_fee": consumer_pvt_credit_insurance_fee,
            "total": credit_insurance_fee_new_disbursements,
        }
    )


def calculate_other_income_existing_loans(
    existing_loans: pd.DataFrame, start_date: str, months_to_forecast: int
):
    existing_loans = existing_loans.assign(
        remaining_term_months=existing_loans.apply(
            lambda row: np.maximum(
                0,
                (
                    helper.convert_to_datetime(row["disbursement_date"])
                    + np.timedelta64(row["loan_term"], "M")
                    - np.datetime64(start_date)
                )
                // np.timedelta64(1, "M"),
            ),
            axis=1,
        )
    )

    # admin_fee_existing_loans = calculate_admin_fee_existing_loans(
    #     loan_amount=existing_loans["loan_amount"],
    #     admin_fee_percentage=existing_loans["admin_fee"],
    #     loan_term_months=existing_loans["loan_term"],
    #     remaining_term_months=existing_loans["remaining_term_months"],
    #     start_date=start_date,
    #     months_to_forecast=months_to_forecast,
    # )

    admin_fee_existing_loans = borrowings.calculate_straight_line_payments(
        effective_dates=existing_loans["disbursement_date"],
        tenures=existing_loans["loan_term"],
        frequencies=existing_loans["admin_fee"] * 0 + 12,
        amounts=existing_loans["admin_fee"]
        * existing_loans["loan_amount"]
        / existing_loans["loan_term"],
        loan_identifiers=existing_loans["loan_number"],
    )

    admin_fee_existing_loans = admin_fee_existing_loans.sum().loc[
        helper.generate_columns(start_date, months_to_forecast)
    ]

    credit_insurance_fee_existing_loans = borrowings.calculate_straight_line_payments(
        effective_dates=existing_loans["disbursement_date"],
        tenures=existing_loans["loan_term"],
        frequencies=existing_loans["admin_fee"] * 0 + 12,
        amounts=(
            existing_loans["credit_insurance_fee"]
            * existing_loans["loan_amount"]
            / existing_loans["loan_term"]
        ),
        loan_identifiers=existing_loans["loan_number"],
    )

    credit_insurance_fee_existing_loans = credit_insurance_fee_existing_loans.sum().loc[
        helper.generate_columns(start_date, months_to_forecast)
    ]

    other_income_existing_loans = helper.add_series(
        [credit_insurance_fee_existing_loans, admin_fee_existing_loans]
    )

    return pd.DataFrame(
        {
            "credit_insurance_fee_existing_loans": credit_insurance_fee_existing_loans,
            "admin_fee_existing_loans": admin_fee_existing_loans,
            "total": other_income_existing_loans,
        }
    )


def aggregate_new_and_existing_loans_admin_fee(
    admin_fee_for_all_new_disbursements_df: pd.Series,
    admin_fee_existing_loans: pd.Series,
    start_date: str,
    months_to_forecast: int,
):
    return (
        admin_fee_for_all_new_disbursements_df.sum(axis=1)
        .add(admin_fee_existing_loans, fill_value=0)
        .reindex(helper.generate_columns(start_date, months_to_forecast))
    )


def aggregate_new_and_existing_loans_credit_insurance_fee(
    credit_insurance_fee_for_all_new_disbursements_df: pd.Series,
    credit_insurance_fee_existing_loans: pd.Series,
    start_date: str,
    months_to_forecast: int,
):
    return (
        credit_insurance_fee_for_all_new_disbursements_df.sum(axis=1)
        .add(credit_insurance_fee_existing_loans, fill_value=0)
        .reindex(helper.generate_columns(start_date, months_to_forecast))
    )


def aggregate_other_income(
    admin_fee_for_all_new_disbursements_df: pd.DataFrame,
    credit_insurance_fee_for_all_new_disbursements_df: pd.DataFrame,
    admin_fee_existing_loans: pd.Series,
    credit_insurance_fee_existing_loans: pd.Series,
    start_date: str,
    months_to_forecast: int,
):
    total_admin_fee = aggregate_new_and_existing_loans_admin_fee(
        admin_fee_for_all_new_disbursements_df=admin_fee_for_all_new_disbursements_df,
        admin_fee_existing_loans=admin_fee_existing_loans,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    total_credit_insurance_fee = aggregate_new_and_existing_loans_credit_insurance_fee(
        credit_insurance_fee_for_all_new_disbursements_df=credit_insurance_fee_for_all_new_disbursements_df,
        credit_insurance_fee_existing_loans=credit_insurance_fee_existing_loans,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    total_other_income = helper.add_series(
        [total_credit_insurance_fee, total_admin_fee]
    )

    return pd.DataFrame(
        {
            "admin_fee": total_admin_fee,
            "credit_insurance_fee": total_credit_insurance_fee,
            "total": total_other_income,
        }
    )
