import io

import awswrangler as wr
import pandas as pd
from fastapi import APIRouter, Depends
from fastapi.responses import Response, StreamingResponse

from application.auth.jwt_bearer import JwtBearer
from application.aws_helper.helper import S3_CLIENT
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
from application.utils import models
from application.utils.database import get_db

router = APIRouter(tags=["Final Calculations"])


def read_files_for_generating_income(
    tenant_name: str,
    project_id: int,
    valuation_date: str,
    boto3_session,
):
    income_statement_index = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.income_statement_index,
        set_index=False,
    )
    variable_inputs_income_statement = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.variable_inputs_income_statement,
    )

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        valuation_date=valuation_date,
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

    static_inputs_income_statement = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.static_inputs_income_statement,
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
        parameters,
        income_statement_index,
        variable_inputs_income_statement,
        opening_balances,
        depreciations_df,
        finance_costs_df,
        static_inputs_income_statement,
        provision_for_credit_loss_for_all_new_disbursements_df,
        new_disbursements_df,
        other_income_df,
        interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df,
        salaries_and_pension_and_statutory_contributions_df,
    )


def aggregate_expenses_in_income_statement(income_statement_df):
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
    variable_inputs_income_statement: pd.DataFrame,
    parameters: pd.DataFrame,
    new_disbursements_df: pd.DataFrame,
    provision_for_credit_loss_for_all_new_disbursements_df: pd.DataFrame,
    opening_balances: pd.DataFrame,
    interest_income_new_disbursement_df: pd.DataFrame,
    existing_loans_schedules_interest_incomes_df: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    variable_expenses = expenses.calculate_variable_expenses(
        variable_inputs_income_statement=variable_inputs_income_statement,
        parameters=parameters,
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )

    business_acquisition = expenses.calculate_business_acqusition(
        business_acquisition_percent=parameters.loc["BUSINESS_ACQUISITION_PERCENT"],
        agent_contribution_percent=parameters.loc["AGENT_CONTRIBUTION_PERCENT"],
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
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )

    total_interest_income = interest_income.aggregate_new_and_existing_loans_interest_income(
        interest_income_new_disbursements_df=interest_income_new_disbursement_df,
        interest_income_existing_loans=existing_loans_schedules_interest_incomes_df.sum(),
        valuation_date=valuation_date,
        months_to_forecast=months_to_forecast,
    )

    return (
        variable_expenses,
        business_acquisition,
        change_in_provision_for_credit_loss,
        total_interest_income,
    )


@router.get("/{tenant_name}/{project_id}/generate-income-statement")
def generate_income_statement(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    (
        parameters,
        income_statement_index,
        variable_inputs_income_statement,
        opening_balances,
        depreciations_df,
        finance_costs_df,
        static_inputs_income_statement,
        provision_for_credit_loss_for_all_new_disbursements_df,
        new_disbursements_df,
        other_income_df,
        interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df,
        salaries_and_pension_and_statutory_contributions_df,
    ) = read_files_for_generating_income(
        tenant_name=tenant_name,
        project_id=project_id,
        valuation_date=VALUATION_DATE,
        boto3_session=constants.MY_SESSION,
    )

    income_statement_df = income_statement.generate_income_statement_template(
        income_statement_index=income_statement_index,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    (
        variable_expenses,
        business_acquisition,
        change_in_provision_for_credit_loss,
        total_interest_income,
    ) = calculate_variable_expenses_and_change_in_provision_for_credit_loss_and_business_aquisition_and_total_interest_income(
        variable_inputs_income_statement=variable_inputs_income_statement,
        parameters=parameters,
        new_disbursements_df=new_disbursements_df,
        provision_for_credit_loss_for_all_new_disbursements_df=provision_for_credit_loss_for_all_new_disbursements_df,
        interest_income_new_disbursement_df=interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df=existing_loans_schedules_interest_incomes_df,
        opening_balances=opening_balances,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    income_statement_df = income_statement.insert_revenue(
        income_statement=income_statement_df,
        interest_income=total_interest_income,
        other_income=other_income_df["total"],
    )

    income_statement_df = income_statement.insert_static_and_variable_inputs(
        income_statement=income_statement_df,
        static_inputs_income_statement=static_inputs_income_statement,
        variable_expenses=variable_expenses,
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
        change_in_provisin_for_credit_loss=change_in_provision_for_credit_loss,
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
        income_statement=income_statement_df, tax_rate=parameters.loc["TAX_RATE"]
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
    valuation_date: str,
):
    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        valuation_date=valuation_date,
    )

    interest_income_new_disbursement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
    )

    details_of_new_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_new_assets,
    )

    details_of_new_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_new_long_term_borrowing,
    )

    details_of_new_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.RawFiles.details_of_new_short_term_borrowing,
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

    capital_repayment_on_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=boto3_session,
        file_name=constants.IntermediateFiles.capital_repayment_on_borrowings_df,
    )

    return (
        capital_repayment_on_borrowings_df,
        income_statement_df,
        finance_costs_df,
        existing_loans_schedules_capital_repayments_df,
        capital_repayment_new_disbursements_df,
        parameters,
        new_disbursements_df,
        other_income_df,
        existing_loans_schedules_interest_incomes_df,
        opening_balances,
        interest_income_new_disbursement_df,
        details_of_new_assets,
        details_of_new_long_term_borrowing,
        details_of_new_short_term_borrowing,
    )


def get_total_disbursements(new_disbursements_df: pd.DataFrame):
    total_disbursements = new_disbursements_df["total"]
    total_disbursements.index = pd.PeriodIndex(
        total_disbursements.index, freq="M"
    ).strftime("%b-%Y")
    return total_disbursements


@router.get("/{tenant_name}/{project_id}/generate-direct-cashflow")
def generate_direct_cashflow(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12
    IMTT = 0.02

    direct_cashflow_df = direct_cashflow.generate_direct_cashflow_template(
        valuation_date=VALUATION_DATE, months_to_forecast=MONTHS_TO_FORECAST
    )

    (
        capital_repayment_on_borrowings_df,
        income_statement_df,
        finance_costs_df,
        existing_loans_schedules_capital_repayments_df,
        capital_repayment_new_disbursements_df,
        parameters,
        new_disbursements_df,
        other_income_df,
        existing_loans_schedules_interest_incomes_df,
        opening_balances,
        interest_income_new_disbursement_df,
        details_of_new_assets,
        details_of_new_long_term_borrowing,
        details_of_new_short_term_borrowing,
    ) = read_files_for_generating_direct_cashflow(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    parameters.columns = pd.PeriodIndex(parameters.columns, freq="M").strftime("%b-%Y")

    direct_cashflow_df.loc["Receipts From Trade Receivables"] = parameters.loc[
        "RECEIPTS_FROM_TRADE_RECEIVABLES"
    ]
    direct_cashflow_df.loc["Issue Of Shares"] = parameters.loc["ISSUE_OF_SHARES"]
    direct_cashflow_df.loc["Payments To Trade Payables"] = -parameters.loc[
        "PAYMENTS_TO_TRADE_PAYABLES"
    ]
    direct_cashflow_df.loc["Dividend Paid"] = -parameters.loc["DIVIDEND_PAID"]

    direct_cashflow_df.loc["Interest Income"] = income_statement_df.loc[
        "Interest Income"
    ]
    direct_cashflow_df.loc["Other Income"] = income_statement_df.loc["Other Income"]
    direct_cashflow_df.loc["Interest Expense"] = -income_statement_df.loc[
        "Finance Costs"
    ]
    direct_cashflow_df.loc["Disbursements"] = -new_disbursements_df["total"].reindex(
        pd.PeriodIndex(new_disbursements_df["total"].index, freq="M").strftime("%b-%Y")
    )

    tax_schedule_df = direct_cashflow.generate_tax_schedule(
        taxation=income_statement_df.loc["Taxation"],
        opening_balance=opening_balances["DEFERED_TAXATION"].iat[0],
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    direct_cashflow_df.loc["Tax Paid"] = tax_schedule_df.loc["Tax Paid"]

    operating_expenses = direct_cashflow.calculate_operating_expenses(
        income_statement=income_statement_df
    )

    direct_cashflow_df.loc["Operating Expenses"] = -operating_expenses

    capital_expenses = direct_cashflow.calculate_capital_expenses(
        details_of_new_assets=details_of_new_assets,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    direct_cashflow_df.loc["Capital Expenses"] = -capital_expenses

    long_and_short_term_borrowing_df = (
        direct_cashflow.calculate_long_and_short_term_borrowing_for_direct_cashflow(
            details_of_new_long_term_borrowing=details_of_new_long_term_borrowing,
            details_of_new_short_term_borrowing=details_of_new_short_term_borrowing,
            valuation_date=VALUATION_DATE,
            months_to_forecast=MONTHS_TO_FORECAST,
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
    ] = -capital_repayment_on_borrowings_df["total"]

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

    income_statement_df.loc["2% Taxation"] = (
        -direct_cashflow_df.loc["Total Cash Outflows"] * IMTT
    )

    income_statement_df = income_statement.calculate_profit_or_loss_for_period(
        income_statement_df
    )

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

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/generate-loan-book")
def generate_loan_book(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

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
        valuation_date=VALUATION_DATE, months_to_forecast=MONTHS_TO_FORECAST
    )

    total_capital_repayments = loan_book.aggregate_new_and_existing_loans_capital_repayments(
        capital_repayments_new_disbursements_df=capital_repayment_new_disbursements_df,
        capital_repayments_existing_loans=capital_repayment_existing_loans,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    total_interest_income = interest_income.aggregate_new_and_existing_loans_interest_income(
        interest_income_new_disbursements_df=interest_income_new_disbursement_df,
        interest_income_existing_loans=existing_loans_schedules_interest_incomes_df.sum(),
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    loan_book_df = loan_book.insert_loan_book_items(
        loan_book=loan_book_df,
        opening_balance_on_loan_book=float(opening_balances["LOAN_BOOK"].iat[0]),
        total_interest_income=total_interest_income,
        total_capital_repayments=total_capital_repayments,
        disbursements=helper.change_period_index_to_strftime(
            new_disbursements_df["total"]
        ),
    )

    loan_book_df = helper.calculate_opening_and_closing_balances(loan_book_df)

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=loan_book_df,
        file_name=constants.FinalFiles.loan_book_df,
        file_stage=constants.FileStage.final,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/generate-balance-sheet")
def generate_balance_sheet(tenant_name: str, project_id: str):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=VALUATION_DATE,
    )

    net_book_values_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.net_book_values_df,
    )

    capital_repayment_on_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.capital_repayment_on_borrowings_df,
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
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
        set_index=False,
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

    short_term_loans_schedules_df = balance_sheet.calculate_short_term_loans_schedules(
        long_and_short_term_borrowing_df=long_and_short_term_borrowing_df,
        capital_repayment_on_borrowings_df=capital_repayment_on_borrowings_df,
        opening_balances=opening_balances,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    long_term_loans_schedules_df = balance_sheet.calculate_long_term_loans_schedules(
        long_and_short_term_borrowing_df=long_and_short_term_borrowing_df,
        capital_repayment_on_borrowings_df=capital_repayment_on_borrowings_df,
        opening_balances=opening_balances,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    balance_sheet_df.loc["Loans"] = long_term_loans_schedules_df.loc["Closing Balance"]
    balance_sheet_df.loc["Borrowings"] = short_term_loans_schedules_df.loc[
        "Closing Balance"
    ]

    balance_sheet_df.loc["Issued Share Capital"] = (
        parameters.loc["ISSUE_OF_SHARES"].cumsum()
        + opening_balances["ISSUED_SHARE_CAPITAL"].iat[0]
    )

    balance_sheet_df.loc[
        "Provision For Taxation"
    ] = helper.change_period_index_to_strftime(parameters.loc["PROVISION_FOR_TAX"])

    balance_sheet_df.loc["Other Payables"] = helper.change_period_index_to_strftime(
        parameters.loc["OTHER_PAYABLES"]
    )
    balance_sheet_df.loc["Intercompany Loans"] = helper.change_period_index_to_strftime(
        parameters.loc["INTERCOMPANY_LOANS"]
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

    balance_sheet_df.loc["Deferred Taxation"] = tax_schedule_df.loc["Closing Balance"]
    trade_payables_schedule_df = balance_sheet.generate_trade_payables_schedule(
        opening_trade_payables=opening_balances["TRADE_PAYABLES"].iat[0],
        payments_to_trade_payables=parameters.loc["PAYMENTS_TO_TRADE_PAYABLES"],
        new_trade_payables=parameters.loc["NEW_TRADE_PAYABLES"],
        months_to_forecast=MONTHS_TO_FORECAST,
        valuation_date=VALUATION_DATE,
    )

    balance_sheet_df.loc["Trade Payables"] = trade_payables_schedule_df.loc[
        "Closing Balance"
    ]
    balance_sheet_df.loc["Trade Receivables"] = trade_receivables_schedule_df.loc[
        "Closing Balance"
    ]
    balance_sheet_df

    balance_sheet_df.loc["Issued Share Capital"] = (
        parameters.loc["ISSUE_OF_SHARES"].cumsum()
        + opening_balances["ISSUED_SHARE_CAPITAL"].iat[0]
    )
    balance_sheet_df.loc["Share Premium"] = opening_balances["SHARE_PREMIUM"].iat[0]
    balance_sheet_df.loc["Other Components Of Equity"] = opening_balances[
        "OTHER_COMPONENTS_OF_EQUITY"
    ].iat[0]
    balance_sheet_df.loc["Treasury Shares"] = opening_balances["TREASURY_SHARES"].iat[0]
    balance_sheet_df.loc["Retained Earnings"] = (
        income_statement_df.loc["PROFIT/(LOSS) FOR PERIOD"]
        - helper.change_period_index_to_strftime(parameters.loc["DIVIDEND_PAID"])
    ).cumsum()
    balance_sheet_df.loc["Capital And Reserves"] = balance_sheet_df.loc[
        "Issued Share Capital":"Retained Earnings"
    ].sum()

    balance_sheet_df = balance_sheet.calculate_other_assets(
        balance_sheet_df=balance_sheet_df,
        parameters=parameters,
        opening_balances=opening_balances,
    )

    balance_sheet_df = balance_sheet.sum_financial_statements_totals(balance_sheet_df)
    balance_sheet_df = balance_sheet.calculate_final_balances(
        balance_sheet_df=balance_sheet_df
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
        file=trade_receivables_schedule_df,
        file_name=constants.IntermediateFiles.trade_receivables_schedule_df,
        file_stage=constants.FileStage.intermediate,
    )

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/generate-statement-of-cashflows")
def generate_statement_of_cashflows(tenant_name: str, project_id: str):
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
    )

    details_of_new_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_assets,
    )

    tax_schedule_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.tax_schedule_df,
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

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    capital_repayment_on_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.capital_repayment_on_borrowings_df,
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
            VALUATION_DATE, MONTHS_TO_FORECAST
        )
    )
    change_in_trade_payables = (
        trade_payables_schedule_df.loc["Closing Balance"]
        - trade_payables_schedule_df.loc["Opening Balance"]
    )
    change_in_trade_receivables = (
        trade_receivables_schedule_df.loc["Opening Balance"]
        - trade_receivables_schedule_df.loc["Closing Balance"]
    )
    change_in_loan_book_principle = (
        loan_book_df.loc["Opening Balance"] - loan_book_df.loc["Closing Balance"]
    )

    statement_of_cashflow_df.loc["Profit/(loss) per I/S"] = income_statement_df.loc[
        "PROFIT/(LOSS) FOR PERIOD"
    ]
    statement_of_cashflow_df.loc["Depreciation"] = income_statement_df.loc[
        "Depreciation"
    ]
    statement_of_cashflow_df.loc[
        "(Increase)/Decrease in Receivables"
    ] = change_in_trade_receivables
    statement_of_cashflow_df.loc[
        "Increase/(Decrease) in Payables"
    ] = change_in_trade_payables
    statement_of_cashflow_df.loc[
        "(Increase)/Decrease in Loan Book (Principle)"
    ] = change_in_loan_book_principle
    statement_of_cashflow_df.loc[
        "Dividend Paid"
    ] = helper.change_period_index_to_strftime(parameters.loc["DIVIDEND_PAID"])

    statement_of_cashflow_df.loc["Interest Paid"] = finance_costs_df.loc["total"]
    statement_of_cashflow_df.loc["Tax Paid"] = tax_schedule_df.loc["Tax Paid"]
    statement_of_cashflow_df.loc[
        "Repayment of Borrowings"
    ] = capital_repayment_on_borrowings_df["total"]

    capital_expenses = direct_cashflow.calculate_capital_expenses(
        details_of_new_assets=details_of_new_assets,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )
    statement_of_cashflow_df.loc["Purchase of Fixed Assets"] = capital_expenses
    statement_of_cashflow_df.loc["Increase/(Decrease) in Short Term Borrowings"] = (
        short_term_loans_schedules_df.loc["Closing Balance"]
        - short_term_loans_schedules_df.loc["Opening Balance"]
    )

    statement_of_cashflow_df.loc["Increase/(Decrease) in Long Term Borrowings"] = (
        long_term_loans_schedules_df.loc["Closing Balance"]
        - long_term_loans_schedules_df.loc["Opening Balance"]
    )

    statement_of_cashflow_df = balance_sheet.sum_financial_statements_totals(
        statement_of_cashflow_df
    )

    statement_of_cashflow_df.loc["Net Increase/(Decrease) in Cash"] = (
        statement_of_cashflow_df.loc["Net Cash Flow From Operations"]
        + statement_of_cashflow_df.loc["Cash Flow From Investing Activities"]
        + statement_of_cashflow_df.loc["Cash Flow From Financing Activities"]
    )

    statement_of_cashflow_df = (
        statement_of_cashflows.calculate_cash_at_end_and_beginning_of_period(
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

    return {"message": "done"}


@router.get("/{tenant_name}/{project_id}/download-final-file")
def download_final_file(
    tenant_name: str, project_id: str, file_name: constants.FinalFiles
):
    # Todo : Get valuation_date and months_to_forecast from the database using project_id

    VALUATION_DATE = "2023-01"
    MONTHS_TO_FORECAST = 12

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


@router.get("/{tenant_name}/{project_id}/download-intermediate-file")
def download_intermediate_file(
    tenant_name: str, project_id: str, file_name: constants.IntermediateFiles
):
    df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    stream = io.StringIO()
    df.to_csv(stream, index=True)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers[
        "Content-Disposition"
    ] = f"attachment; file_name={file_name.value}.csv"
    return response


@router.get("/{tenant_name}/{project_id}/final-filenames")
def get_final_filenames(tenant_name: str, project_id: str):
    final_files: list = wr.s3.list_objects(
        f"s3://{tenant_name}/project_{project_id}/{constants.FileStage.final.value}",
        boto3_session=constants.MY_SESSION,
    )

    final_files = list(map(lambda x: x.split("/")[-1], final_files))
    final_files = list(map(lambda x: x.split(".")[0], final_files))
    return final_files
