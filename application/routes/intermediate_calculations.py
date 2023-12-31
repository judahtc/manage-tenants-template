import awswrangler as wr
import pandas as pd
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from application.auth.security import get_current_active_user
from application.modeling import (
    borrowings,
    constants,
    depreciation,
    disbursements,
    expenses,
    helper,
    interest_income,
    other_income,
)
from application.routes.projects import crud as project_crud
from application.utils import models
from application.utils.database import SessionLocal, get_db

router = APIRouter(
    tags=["INTERMEDIATE CALCULATIONS"], dependencies=[Depends(get_current_active_user)]
)


@router.get("/projects/{project_id}/calculations/intermediate/new-disbursements")
def calculate_new_disbursements(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    new_disbursements_df = disbursements.calculate_new_disbursements(
        disbursement_parameters=disbursement_parameters
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


@router.get(
    "/projects/{project_id}/calculations/intermediate/loan-schedules-new-disbursements"
)
def calculate_loan_schedules_new_disbursements(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    monthly_repayment_new_disbursements_df = (
        interest_income.calculate_monthly_repayments_new_disbursements(
            new_disbursements_df=new_disbursements_df,
            disbursement_parameters=disbursement_parameters,
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
        disbursement_parameters=disbursement_parameters,
        monthly_repayment_new_disbursements_df=monthly_repayment_new_disbursements_df,
        months_to_forecast=months_to_forecast,
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


@router.get(
    "/projects/{project_id}/calculations/intermediate/loan-schedules-existing-loans"
)
def calculate_loan_schedules_existing_loans(
    project_id: str,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name
    existing_loans = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.existing_loans,
        set_index=False,
    )

    existing_loans = helper.columns_to_snake_case(existing_loans)
    # existing_loans = existing_loans.loc[existing_loans["closing_balance"] > 0]

    existing_loans_schedules = borrowings.calculate_reducing_balance_loans_schedules(
        interest_rates=existing_loans["interest_rate"],
        effective_dates=existing_loans["disbursement_date"],
        frequencies=(existing_loans["interest_rate"] * 0 + 12),
        loan_identifiers=existing_loans["loan_number"],
        tenures=existing_loans["loan_term"],
        amounts=existing_loans["loan_amount"],
        is_interest_rate_annual=False,
    )

    existing_loans_schedules_capital_repayments_df = existing_loans_schedules[
        "capital_repayments"
    ]

    existing_loans_schedules_interest_incomes_df = existing_loans_schedules[
        "interest_payments"
    ]

    existing_loans_schedules_outstanding_balances_df = existing_loans_schedules[
        "outstanding_balance_at_start"
    ]

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_capital_repayments_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_outstanding_balances_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_outstanding_balances_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=existing_loans_schedules_interest_incomes_df,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/projects/{project_id}/calculations/intermediate/other-income")
def calculate_other_income(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
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
            disbursement_parameters=disbursement_parameters,
            months_to_forecast=months_to_forecast,
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
            disbursement_parameters=disbursement_parameters,
            months_to_forecast=months_to_forecast,
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
        set_index=False,
    )

    existing_loans = helper.columns_to_snake_case(existing_loans)
    # existing_loans = existing_loans.loc[existing_loans["closing_balance"] > 0]

    other_income_existing_loans_df = other_income.calculate_other_income_existing_loans(
        existing_loans=existing_loans,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
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
        start_date=start_date,
        months_to_forecast=months_to_forecast,
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


@router.get("/projects/{project_id}/calculations/intermediate/depreciation")
def calculate_depreciation(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    details_of_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_assets,
        set_index=False,
    )

    details_of_assets = helper.columns_to_snake_case(details_of_assets)

    depreciations_and_nbvs = depreciation.calculate_depreciations_and_nbvs(
        details_of_assets=details_of_assets,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    depreciations_df = depreciations_and_nbvs["dpns"]
    net_book_values_df = depreciations_and_nbvs["nbvs"]

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
    "/projects/{project_id}/calculations/intermediate/salaries-and-pensions-and-statutory-contributions"
)
def calculate_salaries_and_pensions_and_statutory_contributions(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    other_parameters = helper.read_other_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
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
            disbursement_parameters=disbursement_parameters,
            other_parameters=other_parameters,
            months_to_forecast=months_to_forecast,
            start_date=start_date,
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


@router.get("/projects/{project_id}/calculations/intermediate/provisions")
def calculate_provisions(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    provision_for_credit_loss_for_all_new_disbursements_df = (
        expenses.calculate_provision_for_credit_loss_for_all_new_disbursements(
            new_disbursements_df=new_disbursements_df,
            disbursement_parameters=disbursement_parameters,
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
    "/projects/{project_id}/calculations/intermediate/finance-costs-and-capital-repayment-on-borrowings"
)
def calculate_finance_costs_and_capital_repayment_on_borrowings(
    project_id: str,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    details_of_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_long_term_borrowing,
        set_index=False,
    )

    details_of_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_short_term_borrowing,
        set_index=False,
    )

    details_of_long_term_borrowing = helper.columns_to_snake_case(
        details_of_long_term_borrowing
    )

    details_of_short_term_borrowing = helper.columns_to_snake_case(
        details_of_short_term_borrowing
    )

    long_term_borrowings_schedules = borrowings.calculate_borrowings_schedules(
        details_of_long_term_borrowing
    )
    short_term_borrowings_schedules = borrowings.calculate_borrowings_schedules(
        details_of_short_term_borrowing
    )

    capital_repayment_borrowings_df = pd.concat(
        [
            long_term_borrowings_schedules["capital_repayments"],
            short_term_borrowings_schedules["capital_repayments"],
        ],
    ).fillna(0)

    capital_repayment_borrowings_df.loc["total"] = capital_repayment_borrowings_df.sum()

    long_term_borrowings_capital_repayments_df = long_term_borrowings_schedules[
        "capital_repayments"
    ]

    short_term_borrowings_capital_repayments_df = short_term_borrowings_schedules[
        "capital_repayments"
    ]

    short_term_borrowings_capital_repayments_df.loc[
        "total"
    ] = short_term_borrowings_capital_repayments_df.sum()

    long_term_borrowings_capital_repayments_df.loc[
        "total"
    ] = long_term_borrowings_capital_repayments_df.sum()

    finance_costs_df = pd.concat(
        [
            long_term_borrowings_schedules["interest_payments"],
            short_term_borrowings_schedules["interest_payments"],
        ],
    ).fillna(0)

    finance_costs_df.loc["total"] = finance_costs_df.sum()

    long_term_borrowings_schedules_outstanding_balances_df = (
        long_term_borrowings_schedules["outstanding_balance_at_start"]
    )
    short_term_borrowings_schedules_outstanding_balances_df = (
        short_term_borrowings_schedules["outstanding_balance_at_start"]
    )

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
        file=short_term_borrowings_schedules_outstanding_balances_df,
        file_name=constants.IntermediateFiles.short_term_borrowings_schedules_outstanding_balances_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=long_term_borrowings_schedules_outstanding_balances_df,
        file_name=constants.IntermediateFiles.long_term_borrowings_schedules_outstanding_balances_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=short_term_borrowings_capital_repayments_df,
        file_name=constants.IntermediateFiles.short_term_borrowings_capital_repayments_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=long_term_borrowings_capital_repayments_df,
        file_name=constants.IntermediateFiles.long_term_borrowings_capital_repayments_df,
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


@router.get("/projects/{project_id}/results/intermediate/filenames")
def get_intermediate_filenames(
    project_id: str,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    intermediate_files: list = wr.s3.list_objects(
        f"s3://{tenant_name}/project_{project_id}/{constants.FileStage.intermediate.value}",
        boto3_session=constants.MY_SESSION,
    )

    intermediate_files = list(
        map(lambda x: x.split("/")[-1].split(".")[0], intermediate_files)
    )

    return intermediate_files
