import awswrangler as wr
import pandas as pd
from fastapi import APIRouter

from application.modeling import (
    borrowings,
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
        set_index=False,
    )

    existing_loans = helper.columns_to_snake_case(existing_loans)

    existing_loans_schedules = borrowings.calculate_reducing_balance_loans_schedules(
        interest_rates=existing_loans["interest_rate"],
        effective_dates=existing_loans["disbursement_date"],
        frequencies=existing_loans["frequency"],
        loan_identifiers=existing_loans["loan_number"],
        tenures=existing_loans["loan_term"],
        amounts=existing_loans["loan_amount"],
    )

    existing_loans_schedules_capital_repayments_df = existing_loans_schedules[
        "capital_repayments"
    ]

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_capital_repayments_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
        file_stage=constants.FileStage.intermediate,
    )

    existing_loans_schedules_interest_incomes_df = existing_loans_schedules[
        "interest_payments"
    ]

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

    existing_loans = helper.columns_to_snake_case(existing_loans)

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

    details_of_existing_assets = helper.columns_to_snake_case(
        details_of_existing_assets
    )
    details_of_new_assets = helper.columns_to_snake_case(details_of_new_assets)

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

    details_of_existing_long_term_borrowing = helper.columns_to_snake_case(
        details_of_existing_long_term_borrowing
    )
    details_of_existing_short_term_borrowing = helper.columns_to_snake_case(
        details_of_existing_short_term_borrowing
    )
    details_of_new_short_term_borrowing = helper.columns_to_snake_case(
        details_of_new_short_term_borrowing
    )
    details_of_new_long_term_borrowing = helper.columns_to_snake_case(
        details_of_new_long_term_borrowing
    )

    details_of_long_term_borrowings = pd.concat(
        [details_of_existing_long_term_borrowing, details_of_new_long_term_borrowing]
    ).reset_index(drop=True)

    details_of_short_term_borrowings = pd.concat(
        [details_of_existing_short_term_borrowing, details_of_new_short_term_borrowing]
    ).reset_index(drop=True)

    long_term_borrowings_schedules = borrowings.calculate_borrowings_schedules(
        borrowings=details_of_long_term_borrowings
    )
    short_term_borrowings_schedules = borrowings.calculate_borrowings_schedules(
        borrowings=details_of_short_term_borrowings
    )

    capital_repayment_borrowings_df = pd.concat(
        [
            long_term_borrowings_schedules["capital_repayments"],
            short_term_borrowings_schedules["capital_repayments"],
        ],
    ).fillna(0)

    capital_repayment_borrowings_df.loc["total"] = capital_repayment_borrowings_df.sum()

    finance_costs_df = pd.concat(
        [
            long_term_borrowings_schedules["interest_payments"],
            short_term_borrowings_schedules["interest_payments"],
        ],
    ).fillna(0)

    finance_costs_df.loc["total"] = finance_costs_df.sum()

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=finance_costs_df,
        file_name=constants.IntermediateFiles.finance_costs_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=capital_repayment_borrowings_df,
        file_name=constants.IntermediateFiles.capital_repayment_borrowings_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/intermediate-filenames")
def get_intermediate_filenames(tenant_name: str, project_id: str):
    intermediate_files: list = wr.s3.list_objects(
        f"s3://{tenant_name}/project_{project_id}/{constants.FileStage.intermediate.value}",
        boto3_session=constants.MY_SESSION,
    )
    intermediate_files = list(map(lambda x: x.split("/")[-1], intermediate_files))
    intermediate_files = list(map(lambda x: x.split(".")[0], intermediate_files))
    return intermediate_files
