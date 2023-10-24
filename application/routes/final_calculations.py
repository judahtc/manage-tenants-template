import io

import awswrangler as wr
import pandas as pd
from fastapi import APIRouter, Depends
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.orm import Session

from application.auth.security import get_current_active_user
from application.modeling import (
    balance_sheet,
    constants,
    direct_cashflow,
    expenses,
    helper,
    income_statement,
    interest_income,
    loan_book,
    statement_of_cashflows,
)
from application.routes.projects import crud as project_crud
from application.utils import models, schemas
from application.utils.database import get_db

router = APIRouter(
    tags=["FINAL CALCULATIONS"], dependencies=[Depends(get_current_active_user)]
)


def read_files_for_generating_income(
    tenant_name: str,
    project_id: int,
    start_date: str,
    months_to_forecast: int,
    boto3_session,
):
    expenses_certain = helper.read_expenses_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.expenses_certain,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    expenses_uncertain = helper.read_expenses_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.expenses_uncertain,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    other_parameters = helper.read_other_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    disbursement_parameters = helper.read_disbursement_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.opening_balances,
    )

    depreciations_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.depreciations_df,
    )

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    provision_for_credit_loss_for_all_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.provision_for_credit_loss_for_all_new_disbursements_df,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    other_income_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.other_income_df,
    )

    interest_income_new_disbursement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
    )

    existing_loans_schedules_interest_incomes_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
    )

    salaries_and_pension_and_statutory_contributions_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.salaries_and_pension_and_statutory_contributions_df,
    )

    return (
        other_parameters,
        disbursement_parameters,
        expenses_certain,
        opening_balances,
        depreciations_df,
        finance_costs_df,
        expenses_uncertain,
        provision_for_credit_loss_for_all_new_disbursements_df,
        new_disbursements_df,
        other_income_df,
        interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df,
        salaries_and_pension_and_statutory_contributions_df,
    )


def aggregate_expenses_in_income_statement(income_statement_df: pd.DataFrame):
    income_statement_df = income_statement.aggregate_staff_costs(income_statement_df)
    income_statement_df = income_statement.aggregate_travel_and_entertainment(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_marketing_and_public_relations(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_office_costs(income_statement_df)
    income_statement_df = income_statement.aggregate_professional_fees(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_communication_costs(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_motor_vehicle_costs(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_other_costs(income_statement_df)
    income_statement_df = income_statement.aggregate_investment_income(
        income_statement_df
    )
    income_statement_df = income_statement.aggregate_finance_costs(income_statement_df)

    return income_statement_df


def calculate_variable_expenses_and_change_in_provision_for_credit_loss_and_business_aquisition_and_total_interest_income(
    expenses_uncertain: pd.DataFrame,
    other_parameters: pd.DataFrame,
    disbursement_parameters: pd.DataFrame,
    new_disbursements_df: pd.DataFrame,
    provision_for_credit_loss_for_all_new_disbursements_df: pd.DataFrame,
    opening_balances: pd.DataFrame,
    interest_income_new_disbursement_df: pd.DataFrame,
    existing_loans_schedules_interest_incomes_df: pd.DataFrame,
    start_date: str,
    months_to_forecast: int,
):
    uncertain_expenses = expenses.calculate_uncertain_expenses(
        expenses_uncertain=expenses_uncertain,
        other_parameters=other_parameters,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    business_acquisition = expenses.calculate_business_acqusition(
        business_acquisition_percent=disbursement_parameters.loc[
            "BUSINESS_ACQUISITION_PERCENT"
        ],
        agent_contribution_percent=disbursement_parameters.loc[
            "AGENT_CONTRIBUTION_PERCENT"
        ],
        consumer_ssb_disbursements=new_disbursements_df["consumer_ssb_disbursements"],
        consumer_pvt_disbursements=new_disbursements_df["consumer_pvt_disbursements"],
    )

    change_in_provision_for_credit_loss = expenses.calculate_change_in_provision_for_credit_loss(
        provision_for_credit_loss=provision_for_credit_loss_for_all_new_disbursements_df[
            "total"
        ],
        provision_for_credit_loss_opening_balances=float(
            opening_balances["PROVISION_FOR_CREDIT_LOSS"].iat[0]
        ),
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    total_interest_income = interest_income.aggregate_new_and_existing_loans_interest_income(
        interest_income_new_disbursements_df=interest_income_new_disbursement_df,
        interest_income_existing_loans=existing_loans_schedules_interest_incomes_df.sum(),
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    return (
        uncertain_expenses,
        business_acquisition,
        change_in_provision_for_credit_loss,
        total_interest_income,
    )


@router.get("/projects/{project_id}/calculations/income-statement")
def generate_income_statement(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    (
        other_parameters,
        disbursement_parameters,
        expenses_certain,
        opening_balances,
        depreciations_df,
        finance_costs_df,
        expenses_uncertain,
        provision_for_credit_loss_for_all_new_disbursements_df,
        new_disbursements_df,
        other_income_df,
        interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df,
        salaries_and_pension_and_statutory_contributions_df,
    ) = read_files_for_generating_income(
        tenant_name=tenant_name,
        project_id=project_id,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
        boto3_session=constants.MY_SESSION,
    )

    income_statement_df = income_statement.generate_income_statement_template(
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    (
        uncertain_expenses,
        business_acquisition,
        change_in_provision_for_credit_loss,
        total_interest_income,
    ) = calculate_variable_expenses_and_change_in_provision_for_credit_loss_and_business_aquisition_and_total_interest_income(
        expenses_uncertain=expenses_uncertain,
        other_parameters=other_parameters,
        disbursement_parameters=disbursement_parameters,
        new_disbursements_df=new_disbursements_df,
        provision_for_credit_loss_for_all_new_disbursements_df=provision_for_credit_loss_for_all_new_disbursements_df,
        interest_income_new_disbursement_df=interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df=existing_loans_schedules_interest_incomes_df,
        opening_balances=opening_balances,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    income_statement_df = income_statement.insert_revenue(
        income_statement=income_statement_df,
        interest_income=total_interest_income,
        other_income=other_income_df["total"],
    )

    income_statement_df = income_statement.insert_expenses(
        income_statement=income_statement_df,
        expenses_certain=expenses_certain,
        uncertain_expenses=uncertain_expenses,
    )

    income_statement_df = income_statement.insert_salaries_and_pensions_and_statutory_contributions(
        income_statement=income_statement_df,
        salaries_and_pension_and_statutory_contributions_df=salaries_and_pension_and_statutory_contributions_df,
    )

    income_statement_df = income_statement.insert_depreciation(
        income_statement=income_statement_df,
        depreciation=depreciations_df["total"],
    )

    income_statement_df = income_statement.insert_credit_loss_provision(
        income_statement=income_statement_df,
        change_in_provision_for_credit_loss=change_in_provision_for_credit_loss,
    )

    income_statement_df = income_statement.insert_business_acquisition(
        income_statement=income_statement_df, business_acquisition=business_acquisition
    )

    income_statement_df = aggregate_expenses_in_income_statement(
        income_statement_df=income_statement_df
    )

    income_statement_df = income_statement.calculate_total_expenses(income_statement_df)
    income_statement_df = income_statement.calculate_ebidta(income_statement_df)

    income_statement_df.loc["Finance Costs"] = finance_costs_df.loc["total"]

    income_statement_df = income_statement.aggregate_finance_costs(income_statement_df)

    income_statement_df = income_statement.calculate_profit_before_tax(
        income_statement_df
    )

    income_statement_df = income_statement.calculate_tax(
        income_statement=income_statement_df, tax_rate=other_parameters.loc["TAX_RATE"]
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=income_statement_df,
        file_name=constants.IntermediateFiles.income_statement_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


def read_files_for_generating_direct_cashflow(
    tenant_name: str,
    project_id: int,
    boto3_session,
    start_date: str,
    months_to_forecast: int,
):
    other_parameters = helper.read_other_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    interest_income_new_disbursement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
    )

    details_of_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_assets,
    )

    details_of_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_long_term_borrowing,
        set_index=False,
    )

    details_of_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_short_term_borrowing,
        set_index=False,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.opening_balances,
    )

    existing_loans_schedules_interest_incomes_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
    )

    other_income_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.other_income_df,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    capital_repayment_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.capital_repayment_new_disbursements_df,
    )

    existing_loans_schedules_capital_repayments_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
    )

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    income_statement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.income_statement_df,
    )

    capital_repayment_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.capital_repayment_borrowings_df,
    )

    return (
        capital_repayment_borrowings_df,
        income_statement_df,
        finance_costs_df,
        existing_loans_schedules_capital_repayments_df,
        capital_repayment_new_disbursements_df,
        other_parameters,
        new_disbursements_df,
        other_income_df,
        existing_loans_schedules_interest_incomes_df,
        opening_balances,
        interest_income_new_disbursement_df,
        details_of_assets,
        details_of_long_term_borrowing,
        details_of_short_term_borrowing,
    )


@router.get("/projects/{project_id}/calculations/direct-cashflow")
def generate_direct_cashflow(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    imtt = project.imtt
    tenant_name = current_user.tenant.company_name

    direct_cashflow_df = direct_cashflow.generate_direct_cashflow_template(
        start_date=start_date, months_to_forecast=months_to_forecast
    )

    (
        capital_repayment_borrowings_df,
        income_statement_df,
        finance_costs_df,
        existing_loans_schedules_capital_repayments_df,
        capital_repayment_new_disbursements_df,
        other_parameters,
        new_disbursements_df,
        other_income_df,
        existing_loans_schedules_interest_incomes_df,
        opening_balances,
        interest_income_new_disbursement_df,
        details_of_assets,
        details_of_long_term_borrowing,
        details_of_short_term_borrowing,
    ) = read_files_for_generating_direct_cashflow(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    print("reading files done")

    ## From Parameters
    direct_cashflow_df.loc[
        "Receipts From Receivables"
    ] = helper.change_period_index_to_strftime(
        helper.add_series(
            [
                other_parameters.loc["RECEIPTS_FROM_OTHER_RECEIVABLES"],
                other_parameters.loc["RECEIPTS_FROM_TRADE_RECEIVABLES"],
                other_parameters.loc["RECEIPTS_FROM_INTERGROUP_RECEIVABLES"],
            ]
        )
    )

    direct_cashflow_df.loc[
        "Purchase Of Inventory"
    ] = helper.change_period_index_to_strftime(other_parameters.loc["NEW_INVENTORY"])

    direct_cashflow_df.loc[
        "Payments To Payables"
    ] = -helper.change_period_index_to_strftime(
        helper.add_series(
            [
                other_parameters.loc["PAYMENTS_TO_TRADE_PAYABLES"],
                other_parameters.loc["PAYMENTS_TO_OTHER_PAYABLES"],
            ]
        )
    )

    direct_cashflow_df.loc["Dividend Paid"] = -helper.change_period_index_to_strftime(
        other_parameters.loc["DIVIDEND_PAID"]
    )

    ## From Calculations/Income Statement

    direct_cashflow_df.loc["Interest Income"] = income_statement_df.loc[
        "Interest Income"
    ]
    direct_cashflow_df.loc["Other Income"] = income_statement_df.loc["Other Income"]
    direct_cashflow_df.loc["Interest Expense"] = -income_statement_df.loc[
        "Finance Costs"
    ]
    direct_cashflow_df.loc["Disbursements"] = -helper.change_period_index_to_strftime(
        new_disbursements_df["total"]
    )

    ## Equity and Intercompany Loans

    direct_cashflow_df = direct_cashflow.add_equity_and_intercompany_loans(
        other_parameters=other_parameters, direct_cashflow_df=direct_cashflow_df
    )

    ## Other Assets

    direct_cashflow_df = direct_cashflow.add_other_assets(
        other_parameters=other_parameters, direct_cashflow_df=direct_cashflow_df
    )

    # Tax Paid

    opening_balances = helper.columns_to_screaming_snake_case(opening_balances)

    tax_schedule_df = direct_cashflow.generate_tax_schedule(
        taxation=income_statement_df.loc["Taxation"],
        opening_balance=opening_balances["DEFERED_TAXATION"].iat[0],
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    direct_cashflow_df.loc["Tax Paid"] = tax_schedule_df.loc["Tax Paid"]

    operating_expenses = direct_cashflow.calculate_operating_expenses(
        income_statement=income_statement_df
    )

    direct_cashflow_df.loc["Operating Expenses"] = -operating_expenses

    details_of_assets = helper.columns_to_snake_case(details_of_assets)

    capital_expenses = direct_cashflow.calculate_capital_expenses(
        details_of_assets=details_of_assets,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    direct_cashflow_df.loc["Capital Expenses"] = -capital_expenses

    details_of_long_term_borrowing = helper.columns_to_snake_case(
        details_of_long_term_borrowing
    )
    details_of_short_term_borrowing = helper.columns_to_snake_case(
        details_of_short_term_borrowing
    )

    long_and_short_term_borrowing_df = (
        direct_cashflow.calculate_long_and_short_term_borrowing_for_direct_cashflow(
            details_of_long_term_borrowing=details_of_long_term_borrowing,
            details_of_short_term_borrowing=details_of_short_term_borrowing,
            start_date=start_date,
            months_to_forecast=months_to_forecast,
        )
    )

    direct_cashflow_df.loc["Short Term Borrowing"] = long_and_short_term_borrowing_df[
        "short_term_borrowing"
    ]
    direct_cashflow_df.loc["Long Term Borrowing"] = long_and_short_term_borrowing_df[
        "long_term_borrowing"
    ]

    capital_repayment = helper.add_series(
        [
            existing_loans_schedules_capital_repayments_df.sum(),
            capital_repayment_new_disbursements_df["total"],
        ]
    )

    direct_cashflow_df.loc["Capital Repayment"] = capital_repayment

    direct_cashflow_df.loc[
        "Capital Repayment On Borrowings"
    ] = -capital_repayment_borrowings_df.loc["total"]

    direct_cashflow_df.loc["Total Cash Inflows"] = direct_cashflow_df.iloc[
        direct_cashflow_df.index.get_loc("CASH INFLOWS")
        + 1 : direct_cashflow_df.index.get_loc("Total Cash Inflows")
    ].sum()

    direct_cashflow_df.loc["Total Cash Outflows"] = direct_cashflow_df.iloc[
        direct_cashflow_df.index.get_loc("CASH OUTFLOWS")
        + 1 : direct_cashflow_df.index.get_loc("Total Cash Outflows")
    ].sum()

    direct_cashflow_df.loc["Net Increase/Decrease In Cash"] = (
        direct_cashflow_df.loc["Total Cash Inflows"]
        + direct_cashflow_df.loc["Total Cash Outflows"]
    )

    direct_cashflow_df = (
        direct_cashflow.calculate_opening_and_closing_balances_for_direct_cashflows(
            direct_cashflow=direct_cashflow_df,
            cash_on_hand_opening_balance=opening_balances["CASH_ON_HAND"].iat[0],
        )
    )

    direct_cashflow_yearly_df = direct_cashflow.calculate_direct_cashflow_yearly(
        direct_cashflow_df=direct_cashflow_df, opening_balances=opening_balances
    )

    income_statement_df.loc["2% Taxation"] = (
        direct_cashflow_df.loc["Total Cash Outflows"] * imtt
    )

    income_statement_df = income_statement.calculate_profit_or_loss_for_period(
        income_statement_df
    )

    income_statement_yearly_df = helper.group_next_year_on_wards(df=income_statement_df)

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=tax_schedule_df,
        file_name=constants.IntermediateFiles.tax_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=income_statement_yearly_df,
        file_name=constants.FinalFiles.income_statement_yearly_df,
        file_stage=constants.FileStage.final,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=direct_cashflow_yearly_df,
        file_name=constants.FinalFiles.direct_cashflow_yearly_df,
        file_stage=constants.FileStage.final,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=income_statement_df,
        file_name=constants.FinalFiles.income_statement_df,
        file_stage=constants.FileStage.final,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=long_and_short_term_borrowing_df,
        file_name=constants.IntermediateFiles.long_and_short_term_borrowing_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=direct_cashflow_df,
        file_name=constants.FinalFiles.direct_cashflow_df,
        file_stage=constants.FileStage.final,
    )

    print("Upload Direct Cashflow")

    return {"message": "done"}


@router.get("/projects/{project_id}/calculations/loan-book")
def generate_loan_book(
    project_id: str,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = project_crud.get_project_by_id(db=db, project_id=project_id)
    start_date = project.start_date
    months_to_forecast = project.months_to_forecast
    tenant_name = current_user.tenant.company_name

    capital_repayment_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.capital_repayment_new_disbursements_df,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
    )

    existing_loans_schedules_capital_repayments_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    existing_loans_schedules_outstanding_balances_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.existing_loans_schedules_outstanding_balances_df,
    )

    interest_income_new_disbursement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
    )

    existing_loans_schedules_interest_incomes_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
    )

    capital_repayment_existing_loans = (
        existing_loans_schedules_capital_repayments_df.sum()
    )

    loan_book_df = loan_book.generate_loan_book_template(
        start_date=start_date, months_to_forecast=months_to_forecast
    )

    total_capital_repayments = loan_book.aggregate_new_and_existing_loans_capital_repayments(
        capital_repayments_new_disbursements_df=capital_repayment_new_disbursements_df,
        capital_repayments_existing_loans=capital_repayment_existing_loans,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    total_interest_income = interest_income.aggregate_new_and_existing_loans_interest_income(
        interest_income_new_disbursements_df=interest_income_new_disbursement_df,
        interest_income_existing_loans=existing_loans_schedules_interest_incomes_df.sum(),
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    opening_balances = helper.columns_to_screaming_snake_case(opening_balances)

    loan_book_df = loan_book.insert_loan_book_items(
        loan_book=loan_book_df,
        opening_balance_on_loan_book=existing_loans_schedules_outstanding_balances_df.sum()[
            pd.Timestamp(start_date).strftime("%b-%Y")
        ],
        total_interest_income=total_interest_income,
        total_capital_repayments=total_capital_repayments,
        disbursements=helper.change_period_index_to_strftime(
            new_disbursements_df["total"]
        ),
    )

    loan_book_df = helper.calculate_opening_and_closing_balances(loan_book_df)

    loan_book_yearly_df = loan_book.calculate_loan_book_yearly(loan_book=loan_book_df)

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=loan_book_df,
        file_name=constants.FinalFiles.loan_book_df,
        file_stage=constants.FileStage.final,
    )
    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=loan_book_yearly_df,
        file_name=constants.FinalFiles.loan_book_yearly_df,
        file_stage=constants.FileStage.final,
    )

    return {"message": "done"}


@router.get("/projects/{project_id}/calculations/balance-sheet")
def generate_balance_sheet(
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

    net_book_values_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.net_book_values_df,
    )

    long_and_short_term_borrowing_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.long_and_short_term_borrowing_df,
    )

    tax_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.tax_schedule_df,
    )

    long_term_borrowings_capital_repayments_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.long_term_borrowings_capital_repayments_df,
    )

    short_term_borrowings_capital_repayments_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.short_term_borrowings_capital_repayments_df,
    )

    loan_book_df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.FinalFiles.loan_book_df,
    )

    direct_cashflow_df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.FinalFiles.direct_cashflow_df,
    )

    income_statement_df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.FinalFiles.income_statement_df,
    )

    balance_sheet_df = balance_sheet.generate_balance_sheet_template(
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
        set_index=False,
    )

    short_term_borrowings_schedules_outstanding_balances_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.short_term_borrowings_schedules_outstanding_balances_df,
    )

    long_term_borrowings_schedules_outstanding_balances_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.long_term_borrowings_schedules_outstanding_balances_df,
    )

    provision_for_credit_loss_for_all_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.provision_for_credit_loss_for_all_new_disbursements_df,
    )

    balance_sheet_df.loc["Property Plant And Equipment"] = net_book_values_df["total"]

    balance_sheet_df.loc["Loan Book"] = loan_book_df.loc["Closing Balance"]

    balance_sheet_df.loc["Cash On Hand"] = direct_cashflow_df.loc["Closing Balance"]

    balance_sheet_df.loc[
        "Provisions"
    ] = provision_for_credit_loss_for_all_new_disbursements_df["total"]

    balance_sheet_df.loc[
        "Provision For Taxation"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["PROVISION_FOR_TAX"]
    )

    opening_balances = helper.columns_to_screaming_snake_case(opening_balances)

    short_term_loans_schedules_df = balance_sheet.calculate_short_term_loans_schedules(
        long_and_short_term_borrowing_df=long_and_short_term_borrowing_df,
        capital_repayment_on_borrowings_df=short_term_borrowings_capital_repayments_df.loc[
            "total"
        ],
        opening_balance_on_short_term_loans=short_term_borrowings_schedules_outstanding_balances_df.sum()[
            (pd.Timestamp(start_date) - pd.DateOffset(months=1)).strftime("%b-%Y")
        ],
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    long_term_loans_schedules_df = balance_sheet.calculate_long_term_loans_schedules(
        long_and_short_term_borrowing_df=long_and_short_term_borrowing_df,
        capital_repayment_on_borrowings_df=long_term_borrowings_capital_repayments_df.loc[
            "total"
        ],
        opening_balance_on_long_term_loans=long_term_borrowings_schedules_outstanding_balances_df.sum()[
            pd.Timestamp(start_date).strftime("%b-%Y")
        ],
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    balance_sheet_df.loc["Loans"] = long_term_loans_schedules_df.loc["Closing Balance"]
    balance_sheet_df.loc["Borrowings"] = short_term_loans_schedules_df.loc[
        "Closing Balance"
    ]

    # Receivables and Payables
    trade_receivables_schedule_df = balance_sheet.generate_receivables_schedule(
        opening_receivables=opening_balances["TRADE_RECEIVABLES"].iat[0],
        receipts_from_receivables=other_parameters.loc[
            "RECEIPTS_FROM_TRADE_RECEIVABLES"
        ],
        new_receivables=other_parameters.loc["NEW_TRADE_RECEIVABLES"],
        months_to_forecast=months_to_forecast,
        start_date=start_date,
    )

    other_receivables_schedule_df = balance_sheet.generate_receivables_schedule(
        opening_receivables=opening_balances["OTHER_RECEIVABLES"].iat[0],
        receipts_from_receivables=other_parameters.loc[
            "RECEIPTS_FROM_OTHER_RECEIVABLES"
        ],
        new_receivables=other_parameters.loc["NEW_OTHER_RECEIVABLES"],
        months_to_forecast=months_to_forecast,
        start_date=start_date,
    )

    intergroup_receivables_schedule_df = balance_sheet.generate_receivables_schedule(
        opening_receivables=opening_balances["INTERGROUP_RECEIVABLES"].iat[0],
        receipts_from_receivables=other_parameters.loc[
            "RECEIPTS_FROM_INTERGROUP_RECEIVABLES"
        ],
        new_receivables=other_parameters.loc["NEW_INTERGROUP_RECEIVABLES"],
        months_to_forecast=months_to_forecast,
        start_date=start_date,
    )

    trade_payables_schedule_df = balance_sheet.generate_payables_schedule(
        opening_payables=opening_balances["TRADE_PAYABLES"].iat[0],
        payments_to_payables=other_parameters.loc["PAYMENTS_TO_TRADE_PAYABLES"],
        new_payables=other_parameters.loc["NEW_TRADE_PAYABLES"],
        months_to_forecast=months_to_forecast,
        start_date=start_date,
    )

    other_payables_schedule_df = balance_sheet.generate_payables_schedule(
        opening_payables=opening_balances["OTHER_PAYABLES"].iat[0],
        payments_to_payables=other_parameters.loc["PAYMENTS_TO_OTHER_PAYABLES"],
        new_payables=other_parameters.loc["NEW_OTHER_PAYABLES"],
        months_to_forecast=months_to_forecast,
        start_date=start_date,
    )

    balance_sheet_df.loc["Trade Payables"] = trade_payables_schedule_df.loc[
        "Closing Balance"
    ]
    balance_sheet_df.loc["Other Payables"] = other_payables_schedule_df.loc[
        "Closing Balance"
    ]

    balance_sheet_df.loc["Trade Receivables"] = trade_receivables_schedule_df.loc[
        "Closing Balance"
    ]

    balance_sheet_df.loc["Other Receivables"] = other_receivables_schedule_df.loc[
        "Closing Balance"
    ]

    balance_sheet_df.loc[
        "Intergroup Receivables"
    ] = intergroup_receivables_schedule_df.loc["Closing Balance"]

    balance_sheet_df.loc["Deferred Taxation"] = tax_schedule_df.loc["Closing Balance"]

    inventories_schedule = balance_sheet.generate_inventories_schedule(
        opening_inventories=opening_balances["INVENTORIES"].iat[0],
        new_inventories=other_parameters.loc["NEW_INVENTORY"],
        inventories_used=other_parameters.loc["INVENTORY_USED"],
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    balance_sheet_df.loc["Inventories"] = inventories_schedule.loc["Closing Balance"]

    # Equity and Intercompany loans
    balance_sheet_df.loc[
        "Issued Share Capital"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["SHARE_CAPITAL"].cumsum()
        + opening_balances["ISSUED_SHARE_CAPITAL"].iat[0]
    )

    balance_sheet_df.loc["Intercompany Loans"] = helper.change_period_index_to_strftime(
        other_parameters.loc["INTERCOMPANY_LOANS"].cumsum()
        + opening_balances["INTERCOMPANY_LOANS"].iat[0]
    )

    balance_sheet_df.loc["Share Premium"] = helper.change_period_index_to_strftime(
        other_parameters.loc["SHARE_PREMIUM"].cumsum()
        + opening_balances["SHARE_PREMIUM"].iat[0]
    )

    balance_sheet_df.loc[
        "Other Components Of Equity"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["OTHER_COMPONENTS_OF_EQUITY"].cumsum()
        + opening_balances["OTHER_COMPONENTS_OF_EQUITY"].iat[0]
    )

    balance_sheet_df.loc["Treasury Shares"] = helper.change_period_index_to_strftime(
        other_parameters.loc["TREASURY_SHARES"].cumsum()
        + opening_balances["TREASURY_SHARES"].iat[0]
    )

    balance_sheet_df.loc["Retained Earnings"] = (
        income_statement_df.loc["PROFIT/(LOSS) FOR PERIOD"]
        - helper.change_period_index_to_strftime(other_parameters.loc["DIVIDEND_PAID"])
    ).cumsum()

    balance_sheet_df.loc["Capital And Reserves"] = balance_sheet_df.loc[
        "Issued Share Capital":"Retained Earnings"
    ].sum()

    # Other Assets
    balance_sheet_df.loc["Intangible Assets"] = helper.change_period_index_to_strftime(
        other_parameters.loc["INTANGIBLE_ASSETS"].cumsum()
        + opening_balances["INTANGIBLE_ASSETS"].iat[0]
    )

    balance_sheet_df.loc[
        "Investment In Subsidiaries"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["INVESTMENT_IN_SUBSIDIARIES"].cumsum()
        + opening_balances["INVESTMENT_IN_SUBSIDIARIES"].iat[0]
    )

    balance_sheet_df.loc[
        "Investment In Associates"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["INVESTMENT_IN_ASSOCIATES"].cumsum()
        + opening_balances["INVESTMENT_IN_ASSOCIATES"].iat[0]
    )

    balance_sheet_df.loc[
        "Investment Properties"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["INVESTMENT_PROPERTIES"].cumsum()
        + opening_balances["INVESTMENT_PROPERTIES"].iat[0]
    )

    balance_sheet_df.loc["Equity Investments"] = helper.change_period_index_to_strftime(
        other_parameters.loc["EQUITY_INVESTMENTS"].cumsum()
        + opening_balances["EQUITY_INVESTMENTS"].iat[0]
    )

    balance_sheet_df.loc[
        "Long Term Money Market Investments"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["LONG_TERM_MONEY_MARKET_INVESTMENTS"].cumsum()
        + opening_balances["LONG_TERM_MONEY_MARKET_INVESTMENTS"].iat[0]
    )

    balance_sheet_df.loc[
        "Short Term Money Market Investments"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["SHORT_TERM_MONEY_MARKET_INVESTMENTS"].cumsum()
        + opening_balances["SHORT_TERM_MONEY_MARKET_INVESTMENTS"].iat[0]
    )

    balance_sheet_df.loc[
        "Loans To Related Entities"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["LOANS_TO_RELATED_ENTITIES"].cumsum()
        + opening_balances["LOANS_TO_RELATED_ENTITIES"].iat[0]
    )

    # Calculating Totals
    balance_sheet_df = balance_sheet.sum_financial_statements_totals(balance_sheet_df)
    balance_sheet_df = balance_sheet.calculate_final_balances(
        balance_sheet_df=balance_sheet_df
    )

    balance_sheet_yearly_df = balance_sheet.calculate_balance_sheet_yearly(
        balance_sheet_df=balance_sheet_df
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=other_payables_schedule_df,
        file_name=constants.IntermediateFiles.other_payables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=intergroup_receivables_schedule_df,
        file_name=constants.IntermediateFiles.intergroup_receivables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=other_receivables_schedule_df,
        file_name=constants.IntermediateFiles.other_receivables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=long_term_loans_schedules_df,
        file_name=constants.IntermediateFiles.long_term_loans_schedules_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=short_term_loans_schedules_df,
        file_name=constants.IntermediateFiles.short_term_loans_schedules_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=trade_payables_schedule_df,
        file_name=constants.IntermediateFiles.trade_payables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=balance_sheet_df,
        file_name=constants.FinalFiles.balance_sheet_df,
        file_stage=constants.FileStage.final,
    )
    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=balance_sheet_yearly_df,
        file_name=constants.FinalFiles.balance_sheet_yearly_df,
        file_stage=constants.FileStage.final,
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=trade_receivables_schedule_df,
        file_name=constants.IntermediateFiles.trade_receivables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/projects/{project_id}/calculations/statement-of-cashflows")
def generate_statement_of_cashflows(
    project_id: int,
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

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
    )

    details_of_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_assets,
    )

    tax_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.tax_schedule_df,
    )
    other_receivables_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.other_receivables_schedule_df,
    )

    trade_payables_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.trade_payables_schedule_df,
    )

    trade_receivables_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.trade_receivables_schedule_df,
    )

    intergroup_receivables_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.intergroup_receivables_schedule_df,
    )

    other_payables_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.other_payables_schedule_df,
    )

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    capital_repayment_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.capital_repayment_borrowings_df,
    )

    short_term_loans_schedules_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.short_term_loans_schedules_df,
    )

    long_term_loans_schedules_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.long_term_loans_schedules_df,
    )

    income_statement_df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.FinalFiles.income_statement_df,
    )

    loan_book_df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.FinalFiles.loan_book_df,
    )

    statement_of_cashflow_df = (
        statement_of_cashflows.generate_statement_of_cashflow_template(
            start_date, months_to_forecast
        )
    )

    statement_of_cashflow_df.loc["Profit/(loss) Per I/S"] = income_statement_df.loc[
        "PROFIT/(LOSS) FOR PERIOD"
    ]
    statement_of_cashflow_df.loc["Depreciation"] = income_statement_df.loc[
        "Depreciation"
    ]
    statement_of_cashflow_df.loc[
        "Dividend Paid"
    ] = helper.change_period_index_to_strftime(other_parameters.loc["DIVIDEND_PAID"])
    statement_of_cashflow_df.loc[
        "Treasury Movements"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["TREASURY_MOVEMENTS"]
    )
    statement_of_cashflow_df.loc[
        "Interest Expense Accrued"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["INTEREST_EXPENSE_ACCRUED"]
    )
    statement_of_cashflow_df.loc[
        "Other Non-Cash Items"
    ] = helper.change_period_index_to_strftime(
        other_parameters.loc["OTHER_NON_CASH_ITEMS"]
    )
    statement_of_cashflow_df.loc["Interest Paid"] = -finance_costs_df.loc["total"]
    statement_of_cashflow_df.loc["Tax Paid"] = tax_schedule_df.loc["Tax Paid"]
    statement_of_cashflow_df.loc[
        "Repayment Of Borrowings"
    ] = capital_repayment_borrowings_df.loc["total"]

    details_of_assets = helper.columns_to_snake_case(details_of_assets)

    capital_expenses = direct_cashflow.calculate_capital_expenses(
        details_of_assets=details_of_assets,
        start_date=start_date,
        months_to_forecast=months_to_forecast,
    )

    statement_of_cashflow_df.loc["Purchase Of Fixed Assets"] = capital_expenses
    statement_of_cashflow_df.loc["Increase/(Decrease) In Short Term Borrowings"] = (
        short_term_loans_schedules_df.loc["Closing Balance"]
        - short_term_loans_schedules_df.loc["Opening Balance"]
    )
    statement_of_cashflow_df.loc["Increase/(Decrease) In Long Term Borrowings"] = (
        long_term_loans_schedules_df.loc["Closing Balance"]
        - long_term_loans_schedules_df.loc["Opening Balance"]
    )

    statement_of_cashflow_df.loc[
        "Cash From Operations Before WC"
    ] = statement_of_cashflow_df.iloc[
        1 : statement_of_cashflow_df.index.get_loc("Cash From Operations Before WC")
    ].sum()

    change_in_receivables = (
        other_receivables_schedule_df.loc["Closing Balance"]
        - other_receivables_schedule_df.loc["Opening Balance"]
        + trade_receivables_schedule_df.loc["Closing Balance"]
        - trade_receivables_schedule_df.loc["Opening Balance"]
        + intergroup_receivables_schedule_df.loc["Closing Balance"]
        - intergroup_receivables_schedule_df.loc["Opening Balance"]
    )

    change_in_payables = (
        trade_payables_schedule_df.loc["Closing Balance"]
        - trade_payables_schedule_df.loc["Opening Balance"]
        + other_payables_schedule_df.loc["Closing Balance"]
        - other_payables_schedule_df.loc["Opening Balance"]
    )

    change_in_loan_book_principle = loan_book_df.loc[
        "New Disbursements":"Repayments"
    ].sum()

    change_in_loan_book_interest = loan_book_df.loc["Interest Income"]

    borrowings_schedule = long_term_loans_schedules_df + short_term_loans_schedules_df
    change_in_borrowings = (
        borrowings_schedule.loc["Closing Balance"]
        - borrowings_schedule.loc["Opening Balance"]
    )

    statement_of_cashflow_df.loc[
        "(Increase)/Decrease In Receivables"
    ] = change_in_receivables
    statement_of_cashflow_df.loc["Increase/(Decrease) In Payables"] = change_in_payables
    statement_of_cashflow_df.loc[
        "(Increase)/Decrease In Loan Book (Principle)"
    ] = change_in_loan_book_principle
    statement_of_cashflow_df.loc[
        "(Increase)/Decrease In Loan Book (Interest)"
    ] = change_in_loan_book_interest
    statement_of_cashflow_df.loc[
        "Increase/(Decrease) In Borrowings"
    ] = change_in_borrowings

    statement_of_cashflow_df.loc["Cash From Operations After WC"] = (
        statement_of_cashflow_df.loc["Cash From Operations Before WC"]
        + statement_of_cashflow_df.iloc[
            statement_of_cashflow_df.index.get_loc("Working Capital Movements")
            + 1 : statement_of_cashflow_df.index.get_loc(
                "Cash From Operations After WC"
            )
        ].sum()
    )

    statement_of_cashflow_df.loc[
        "Net Cash Flow From Operations"
    ] = statement_of_cashflow_df.loc["Cash From Operations After WC":"Tax Paid"].sum()

    statement_of_cashflow_df = balance_sheet.sum_financial_statements_totals(
        statement_of_cashflow_df
    )

    statement_of_cashflow_df.loc["Net Increase/(Decrease) In Cash"] = (
        statement_of_cashflow_df.loc["Net Cash Flow From Operations"]
        + statement_of_cashflow_df.loc["Cash Flow From Investing Activities"]
        + statement_of_cashflow_df.loc["Cash Flow From Financing Activities"]
    )

    opening_balances = helper.columns_to_screaming_snake_case(opening_balances)

    statement_of_cashflow_df = (
        statement_of_cashflows.calculate_cash_at_end_and_beginning_of_period(
            statement_of_cashflow_df=statement_of_cashflow_df,
            opening_balances=opening_balances,
        )
    )

    statement_of_cashflow_yearly_df = (
        statement_of_cashflows.calculate_statement_of_cashflow_yearly_df(
            statement_of_cashflow_df=statement_of_cashflow_df,
            opening_balances=opening_balances,
        )
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=statement_of_cashflow_df,
        file_name=constants.FinalFiles.statement_of_cashflow_df,
        file_stage=constants.FileStage.final,
    )
    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=statement_of_cashflow_yearly_df,
        file_name=constants.FinalFiles.statement_of_cashflow_yearly_df,
        file_stage=constants.FileStage.final,
    )
    project_crud.update_project_status(
        project_id=project_id, status=schemas.ProjectStatus.COMPLETED, db=db
    )
    return {"message": "done"}


@router.get("/projects/{project_id}/results")
def download_final_file(
    project_id: str,
    file_name: constants.FinalFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    stream = io.StringIO()
    df.to_csv(stream, index=True)

    response = Response(
        content=stream.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={file_name.value}",
        },
    )
    return response


@router.get("/projects/{project_id}/results/download")
def download_only_final_file(
    project_id: str,
    file_name: constants.FinalFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    return Response(
        content=df.to_csv(index=True),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.csv"',
            "Content-Type": "application/octet-stream",
        },
    )



@router.get("/projects/{project_id}/results/intermediate")
def download_intermediate_file(
    project_id: int,
    file_name: constants.IntermediateFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    if file_name in [
        constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
        constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
        constants.IntermediateFiles.existing_loans_schedules_outstanding_balances_df,
    ]:
        df = df.head(100)

    stream = io.StringIO()

    df.to_csv(stream, index=True)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers[
        "Content-Disposition"
    ] = f"attachment; file_name={file_name.value}.csv"
    return response


@router.get("/projects/{project_id}/results/intermediate/download")
def download_intermediate_file_only(
    project_id: int,
    file_name: constants.IntermediateFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    return Response(
        content=df.to_csv(index=True),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.csv"',
            "Content-Type": "application/octet-stream",
        },
    )


@router.get("/projects/{project_id}/results/intermediate/view")
def view_intermediate_file(
    project_id: int,
    file_name: constants.IntermediateFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    if file_name in [
        constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
        constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
        constants.IntermediateFiles.existing_loans_schedules_outstanding_balances_df,
    ]:
        df = df.head(100)

    return Response(
        content=df.to_csv(index=True),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.csv"',
            "Content-Type": "text/csv",
        },
    )


@router.get("/projects/{project_id}/results/view")
def view_final_file(
    project_id: str,
    file_name: constants.FinalFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    df = helper.read_final_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    return Response(
        content=df.to_csv(index=True),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.csv"',
            "Content-Type": "text/csv",
        },
    )


@router.get("/projects/{project_id}/results/filenames")
def get_final_filenames(
    project_id: int,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    final_files: list = wr.s3.list_objects(
        f"s3://{tenant_name}/project_{project_id}/{constants.FileStage.final.value}",
        boto3_session=constants.MY_SESSION,
    )

    final_files = list(map(lambda x: x.split("/")[-1].split(".")[0], final_files))

    return final_files
