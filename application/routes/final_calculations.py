import io

import pandas as pd
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from application.modeling import (
    constants,
    direct_cashflow,
    expenses,
    helper,
    income_statement,
    interest_income,
)

router = APIRouter(tags=["Final Calculations"])


def read_files_for_generating_income(
    tenant_name: str, project_id: int, valuation_date: str
):
    income_statement_index = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.income_statement_index,
        set_index=False,
    )
    variable_inputs_income_statement = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.variable_inputs_income_statement,
    )

    parameters = helper.read_parameters_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        valuation_date=valuation_date,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
    )

    depreciations_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.depreciations_df,
    )

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    static_inputs_income_statement = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.static_inputs_income_statement,
    )

    provision_for_credit_loss_for_all_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.provision_for_credit_loss_for_all_new_disbursements_df,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    provisions_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.provisions_df,
    )

    other_income_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.other_income_df,
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

    salaries_and_pension_and_statutory_contributions_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
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
        provisions_df,
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

    change_in_provisin_for_credit_loss = expenses.calculate_change_in_provision_for_credit_loss(
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
        change_in_provisin_for_credit_loss,
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
        provisions_df,
        other_income_df,
        interest_income_new_disbursement_df,
        existing_loans_schedules_interest_incomes_df,
        salaries_and_pension_and_statutory_contributions_df,
    ) = read_files_for_generating_income(
        tenant_name=tenant_name, project_id=project_id, valuation_date=VALUATION_DATE
    )

    income_statement_df = income_statement.generate_income_statement_template(
        income_statement_index=income_statement_index,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    (
        variable_expenses,
        business_acquisition,
        change_in_provisin_for_credit_loss,
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

    income_statement_df = income_statement.insert_bad_debts_provision(
        income_statement=income_statement_df,
        provision_for_bad_debts=provisions_df["provision_for_bad_debts"],
        change_in_provisin_for_credit_loss=change_in_provisin_for_credit_loss,
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
        boto3_session=constants.MY_SESSION,
        valuation_date=valuation_date,
    )

    interest_income_new_disbursement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.interest_income_new_disbursement_df,
    )

    details_of_new_assets = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_assets,
    )

    details_of_new_long_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_long_term_borrowing,
    )

    details_of_new_short_term_borrowing = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.details_of_new_short_term_borrowing,
    )

    opening_balances = helper.read_raw_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.RawFiles.opening_balances,
    )

    existing_loans_schedules_interest_incomes_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.existing_loans_schedules_interest_incomes_df,
    )

    other_income_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.other_income_df,
    )

    new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.new_disbursements_df,
    )

    capital_repayment_new_disbursements_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.capital_repayment_new_disbursements_df,
    )

    existing_loans_schedules_capital_repayments_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.existing_loans_schedules_capital_repayments_df,
    )

    finance_costs_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.finance_costs_df,
    )

    income_statement_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=constants.IntermediateFiles.income_statement_df,
    )

    capital_repayment_on_borrowings_df = helper.read_intermediate_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
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
    direct_cashflow_df.loc["Tax Paid"] = parameters.loc["TAX_PAID"]
    direct_cashflow_df.loc["Dividend Paid"] = parameters.loc["DIVIDEND_PAID"]

    total_interest_income = interest_income.aggregate_new_and_existing_loans_interest_income(
        interest_income_new_disbursements_df=interest_income_new_disbursement_df,
        interest_income_existing_loans=existing_loans_schedules_interest_incomes_df.sum(),
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    direct_cashflow_df.loc["Interest Income"] = total_interest_income
    direct_cashflow_df.loc["Other Income"] = other_income_df["total"]
    direct_cashflow_df.loc["Interest Expense"] = finance_costs_df.loc["total"]
    direct_cashflow_df.loc["Disbursements"] = get_total_disbursements(
        new_disbursements_df=new_disbursements_df
    )
    operating_expenses = direct_cashflow.calculate_operating_expenses(
        income_statement=income_statement_df
    )

    direct_cashflow_df.loc["Operating Expenses"] = operating_expenses

    capital_expenses = direct_cashflow.calculate_capital_expenses(
        details_of_new_assets=details_of_new_assets,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    direct_cashflow_df.loc["Capital Expenses"] = capital_expenses

    short_term_borrowing = direct_cashflow.calculate_direct_cashflow_borrowing(
        details_of_new_borrowing=details_of_new_short_term_borrowing,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    long_term_borrowing = direct_cashflow.calculate_direct_cashflow_borrowing(
        details_of_new_borrowing=details_of_new_long_term_borrowing,
        valuation_date=VALUATION_DATE,
        months_to_forecast=MONTHS_TO_FORECAST,
    )

    direct_cashflow_df.loc["Short Term Borrowing"] = short_term_borrowing
    direct_cashflow_df.loc["Long Term Borrowing"] = long_term_borrowing

    capital_repayment = helper.add_series(
        [
            existing_loans_schedules_capital_repayments_df.sum(),
            capital_repayment_new_disbursements_df["total"],
        ]
    )

    direct_cashflow_df.loc["Capital Repayment"] = capital_repayment

    direct_cashflow_df.loc[
        "Capital Repayment On Borrowings"
    ] = capital_repayment_on_borrowings_df["total"]

    direct_cashflow_df.loc["Total Cash Inflows"] = direct_cashflow_df.loc[
        "Receipts From Trade Receivables":"Long Term Borrowing"
    ].sum()

    direct_cashflow_df.loc["Total Cash Outflows"] = direct_cashflow_df.loc[
        "Disbursements":"Tax Paid"
    ].sum()

    direct_cashflow_df.loc["Net Increase/Decrease In Cash"] = (
        direct_cashflow_df.loc["Total Cash Inflows"]
        - direct_cashflow_df.loc["Total Cash Outflows"]
    )

    direct_cashflow_df = (
        direct_cashflow.calculate_opening_and_closing_balances_for_direct_cashflows(
            direct_cashflow=direct_cashflow_df,
            cash_on_hand_opening_balance=opening_balances["CASH_ON_HAND"].iat[0],
        )
    )

    income_statement_df.loc["2% Taxation"] = (
        direct_cashflow_df.loc["Total Cash Outflows"] * IMTT
    )

    income_statement_df = income_statement.calculate_profit_or_loss_for_period(
        income_statement_df
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

    capital_repayment = helper.add_series(
        [
            existing_loans_schedules_capital_repayments_df.sum(),
            capital_repayment_new_disbursements_df["total"],
        ]
    )

    loan_book_df = direct_cashflow.generate_loan_book_template(
        valuation_date=VALUATION_DATE, months_to_forecast=MONTHS_TO_FORECAST
    )

    loan_book_df = direct_cashflow.insert_loan_book_items(
        loan_book=loan_book_df,
        opening_balance_on_loan_book=float(opening_balances["LOAN_BOOK"].iat[0]),
        capital_repayment=capital_repayment,
        disbursements=get_total_disbursements(
            new_disbursements_df=new_disbursements_df
        ),
    )

    loan_book_df = direct_cashflow.calculate_opening_and_closing_balances_for_loan_book(
        loan_book_df
    )

    helper.upload_file(
        tenant_name=tenant_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file=loan_book_df,
        file_name=constants.FinalFiles.loan_book_df,
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
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; file_name={file_name}.csv"
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
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = f"attachment; file_name={file_name}.csv"
    return response
