from enum import Enum

import boto3
from decouple import config

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")

S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)

MY_SESSION = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)


class RawFiles(str, Enum):
    details_of_assets = "details_of_assets"
    details_of_long_term_borrowing = "details_of_long_term_borrowing"
    details_of_short_term_borrowing = "details_of_short_term_borrowing"
    income_statement_index = "income_statement_index"
    static_inputs_income_statement = "static_inputs_income_statement"
    variable_inputs_income_statement = "variable_inputs_income_statement"
    parameters = "parameters"
    existing_loans = "existing_loans"
    opening_balances = "opening_balances"


class IntermediateFiles(str, Enum):
    new_disbursements_df = "new_disbursements_df"
    monthly_repayment_new_disbursements_df = "monthly_repayment_new_disbursements_df"
    capital_repayment_new_disbursements_df = "capital_repayment_new_disbursements_df"
    interest_income_new_disbursement_df = "interest_income_new_disbursement_df"
    admin_fee_for_all_new_disbursements_df = "admin_fee_for_all_new_disbursements_df"
    credit_insurance_fee_for_all_new_disbursements_df = (
        "credit_insurance_fee_for_all_new_disbursements_df"
    )
    other_payables_schedule_df = "other_payables_schedule_df"

    existing_loans_schedules_capital_repayments_df = (
        "existing_loans_schedules_capital_repayments_df"
    )
    existing_loans_schedules_interest_incomes_df = (
        "existing_loans_schedules_interest_incomes_df"
    )
    other_income_existing_loans_df = "other_income_existing_loans_df"
    other_income_df = "other_income_df"
    depreciations_df = "depreciations_df"
    net_book_values_df = "net_book_values_df"
    salaries_and_pension_and_statutory_contributions_df = (
        "salaries_and_pension_and_statutory_contributions_df"
    )
    trade_receivables_schedule_df = "trade_receivables_schedule_df"
    provisions_df = "provisions_df"
    provision_for_credit_loss_for_all_new_disbursements_df = (
        "provision_for_credit_loss_for_all_new_disbursements_df"
    )
    finance_costs_df = "finance_costs_df"
    capital_repayment_borrowings_df = "capital_repayment_borrowings_df"
    income_statement_df = "income_statement_df"
    direct_cashflow_df = "direct_cashflow_df"
    loan_book_df = "loan_book_df"
    tax_schedule_df = "tax_schedule_df"
    long_and_short_term_borrowing_df = "long_and_short_term_borrowing_df"
    trade_payables_schedule_df = "trade_payables_schedule_df"

    short_term_loans_schedules_df = "short_term_loans_schedules_df"
    long_term_loans_schedules_df = "long_term_loans_schedules_df"
    other_receivables_schedule_df = "other_receivables_schedule_df"
    intergroup_receivables_schedule_df = "intergroup_receivables_schedule_df"
    long_term_borrowings_capital_repayments_df = (
        "long_term_borrowings_capital_repayments_df"
    )
    short_term_borrowings_capital_repayments_df = (
        "short_term_borrowings_capital_repayments_df"
    )
    existing_loans_schedules_outstanding_balances_df = (
        "existing_loans_schedules_outstanding_balances_df"
    )


class FinalFiles(str, Enum):
    income_statement_df = "income_statement_df"
    direct_cashflow_df = "direct_cashflow_df"
    loan_book_df = "loan_book_df"
    balance_sheet_df = "balance_sheet_df"
    statement_of_cashflow_df = "statement_of_cashflow_df"


class FileStage(str, Enum):
    intermediate = "intermediate"
    raw = "raw"
    final = "final"


import boto3
from decouple import config

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")

S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)

MY_SESSION = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)
