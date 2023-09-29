import pandas as pd
from fastapi import APIRouter

from application.modeling import (
    constants,
    depreciation,
    direct_cashflow,
    disbursements,
    expenses,
    helper,
    interest_income,
    other_income,
)

router = APIRouter(tags=["Intermediate Calculations"])


@router.get("/{tenant_name}/{project_id}/calculate-new-disbursements")
def calculate_new_disbursements(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12
    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    new_disbursements_df = disbursements.calculate_new_disbursements(
        parameters=parameters
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=new_disbursements_df,
        file_name=constants.IntermediateFiles.new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/calculate-loan-schedules-new-disbursements")
def calculate_loan_schedules_new_disbursements(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    monthly_repayment_new_disbursements_df = (
        interest_income.calculate_monthly_repayments_new_disbursements(
            new_disbursements_df=new_disbursements_df, parameters=parameters
        )
    )

    monthly_repayment_new_disbursements_df = helper.convert_index(
        monthly_repayment_new_disbursements_df, True
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=monthly_repayment_new_disbursements_df,
        file_name=constants.IntermediateFiles.monthly_repayment_new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    loan_schedules_for_all_new_disbursements = interest_income.generate_loan_schedules_for_all_new_disbursements(
        new_disbursements_df=new_disbursements_df,
        parameters=parameters,
        monthly_repayment_new_disbursements_df=monthly_repayment_new_disbursements_df,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    capital_repayment_new_disbursements_df = interest_income.generate_capital_repayment_new_disbursements_df(
        loan_schedules_for_all_new_disbursements=loan_schedules_for_all_new_disbursements
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=capital_repayment_new_disbursements_df,
        file_name=constants.IntermediateFiles.capital_repayment_new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    interest_income_new_disbursement_df = interest_income.generate_interest_income_new_disbursements_df(
        loan_schedules_for_all_new_disbursements=loan_schedules_for_all_new_disbursements
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=interest_income_new_disbursement_df,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/calculate-loan-schedules-existing-loans")
def calculate_loan_schedules_existing_loans(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    existing_loans = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.existing_loans,
    )

    existing_loans_schedules = interest_income.generate_loan_schedules_existing_loans(
        outstanding_balance=existing_loans["outstanding_balance"],
        interest_rate_monthly=existing_loans["interest_rate"],
        repayment_amount_monthly=existing_loans["repayment_amount"],
        valuation_date=VALUATION_DATE,
        months_to_project=MONTHS_TO_FORECAST,
    )

    existing_loans_schedules_capital_repayments_df = existing_loans_schedules[
        "capital_repayment"
    ]

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_capital_repayments_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
        file_stage=constants.FileStage.intermediate,
    )

    existing_loans_schedules_interest_incomes_df = existing_loans_schedules["interest"]

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_interest_incomes_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/calculate-other-income")
def calculate_other_income(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    admin_fee_for_all_new_disbursements_df = (
        other_income.calculate_admin_fee_for_all_new_disbursements(
            new_disbursements_df=new_disbursements_df,
            parameters=parameters,
            months_to_forecast=MONTHS_TO_FORECAST,
        )
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=admin_fee_for_all_new_disbursements_df,
        file_name=constants.IntermediateFiles.admin_fee_for_all_new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    credit_insurance_fee_for_all_new_disbursements_df = (
        other_income.calculate_credit_insurance_fee_for_all_new_disbursements(
            new_disbursements_df=new_disbursements_df,
            parameters=parameters,
            months_to_forecast=MONTHS_TO_FORECAST,
        )
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=credit_insurance_fee_for_all_new_disbursements_df,
        file_name=constants.IntermediateFiles.credit_insurance_fee_for_all_new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    existing_loans = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.existing_loans,
    )

    other_income_existing_loans_df = other_income.calculate_other_income_existing_loans(
        existing_loans=existing_loans,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=other_income_existing_loans_df,
        file_name=constants.IntermediateFiles.other_income_existing_loans_df,
        file_stage=constants.FileStage.intermediate,
    )

    other_income_df = other_income.aggregate_other_income(
        admin_fee_for_all_new_disbursements_df=admin_fee_for_all_new_disbursements_df,
        admin_fee_existing_loans=other_income_existing_loans_df[
            "admin_fee_existing_loans"
        ],
        credit_insurance_fee_existing_loans=other_income_existing_loans_df[
            "credit_insurance_fee_existing_loans"
        ],
        credit_insurance_fee_for_all_new_disbursements_df=credit_insurance_fee_for_all_new_disbursements_df,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=other_income_df,
        file_name=constants.IntermediateFiles.other_income_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/calculate-depreciation")
def calculate_depreciation(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    details_of_existing_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_existing_assets,
        set_index=False,
    )

    details_of_new_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_assets,
        set_index=False,
    )

    depreciations_and_nbvs = depreciation.calculate_depreciations_and_nbvs(
        details_of_existing_assets=details_of_existing_assets,
        details_of_new_assets=details_of_new_assets,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    depreciations_df = depreciations_and_nbvs["dpns"]

    net_book_values_df = depreciations_and_nbvs["dpns"]

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=depreciations_df,
        file_name=constants.IntermediateFiles.depreciations_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=net_book_values_df,
        file_name=constants.IntermediateFiles.net_book_values_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get(
    "/{tenant_name}/{project_id}/calculate-salaries-and-pensions-and-statutory-contributions"
)
def calculate_salaries_and_pensions_and_statutory_contributions(
    tenant_name: str, project_id: str
):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    salaries_and_pension_and_statutory_contributions_df = (
        expenses.calculate_salaries_and_pension_and_statutory_contributions(
            new_disbursements_df=new_disbursements_df,
            parameters=parameters,
            months_to_forecast=MONTHS_TO_FORECAST,
            valuation_date=VALUATION_DATE,
        )
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=salaries_and_pension_and_statutory_contributions_df,
        file_name=constants.IntermediateFiles.salaries_and_pension_and_statutory_contributions_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"messages": "done"}


@router.get("/{tenant_name}/{project_id}/calculate-provisions")
def calculate_provisions(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
        set_index=False,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    trade_receivables_schedule_df = expenses.generate_trade_receivables_schedule(
        opening_trade_receivables=opening_balances["TRADE_RECEIVABLES"].iat[0],
        receipts_from_trade_receivables=parameters.loc[
            "RECEIPTS_FROM_TRADE_RECEIVABLES"
        ],
        new_trade_receivables=parameters.loc["NEW_TRADE_RECEIVABLES"],
        months_to_forecast=MONTHS_TO_FORECAST,
        valuation_date=VALUATION_DATE,
    )

    trade_receivables_schedule_df = helper.convert_columns(
        trade_receivables_schedule_df
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=trade_receivables_schedule_df,
        file_name=constants.IntermediateFiles.trade_receivables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    provision_for_credit_loss_for_all_new_disbursements_df = (
        expenses.calculate_provision_for_credit_loss_for_all_new_disbursements(
            new_disbursements_df=new_disbursements_df, parameters=parameters
        )
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=provision_for_credit_loss_for_all_new_disbursements_df,
        file_name=constants.IntermediateFiles.provision_for_credit_loss_for_all_new_disbursements_df,
        file_stage=constants.FileStage.intermediate,
    )

    provisions_df = expenses.calculate_provisions(
        trade_receivables_schedule=trade_receivables_schedule_df,
        provision_for_credit_loss_for_all_new_disbursements_df=provision_for_credit_loss_for_all_new_disbursements_df,
        parameters=parameters,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=provisions_df,
        file_name=constants.IntermediateFiles.provisions_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get(
    "/{tenant_name}/{project_id}/calculate-finance-costs-and-capital-repayment-on-borrowings"
)
def calculate_finance_costs_and_capital_repayment_on_borrowings(
    tenant_name: str, project_id: str
):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    details_of_new_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_short_term_borrowing,
        set_index=False,
    )
    details_of_existing_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_existing_short_term_borrowing,
        set_index=False,
    )
    details_of_new_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_short_term_borrowing,
        set_index=False,
    )
    details_of_existing_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_existing_short_term_borrowing,
        set_index=False,
    )

    finance_costs_df = expenses.calculate_finance_costs(
        details_of_existing_long_term_borrowing=details_of_existing_long_term_borrowing,
        details_of_existing_short_term_borrowing=details_of_existing_short_term_borrowing,
        details_of_new_short_term_borrowing=details_of_new_short_term_borrowing,
        details_of_new_long_term_borrowing=details_of_new_long_term_borrowing,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=finance_costs_df,
        file_name=constants.IntermediateFiles.finance_costs_df,
        file_stage=constants.FileStage.intermediate,
    )

    capital_repayment_on_borrowings_df = direct_cashflow.calculate_capital_repayment_on_borrowings(
        details_of_existing_long_term_borrowing=details_of_existing_long_term_borrowing,
        details_of_existing_short_term_borrowing=details_of_existing_short_term_borrowing,
        details_of_new_short_term_borrowing=details_of_new_short_term_borrowing,
        details_of_new_long_term_borrowing=details_of_new_long_term_borrowing,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=capital_repayment_on_borrowings_df,
        file_name=constants.IntermediateFiles.capital_repayment_on_borrowings_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}
