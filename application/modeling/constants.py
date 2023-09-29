from enum import Enum

import boto3
from decouple import config







class RawFiles(str, Enum):
    details_of_existing_assets = "details_of_existing_assets"
    details_of_new_assets = "details_of_new_assets"
    details_of_existing_long_term_borrowing = "details_of_existing_long_term_borrowing"
    details_of_existing_short_term_borrowing = (
        "details_of_existing_short_term_borrowing"
    )
    details_of_new_short_term_borrowing = "details_of_new_short_term_borrowing"
    details_of_new_long_term_borrowing = "details_of_new_long_term_borrowing"
    income_statement_index = "income_statement_index"
    static_inputs_income_statement = "static_inputs_income_statement"
    variable_inputs_income_statement = "variable_inputs_income_statement"
    disbursements = "disbursements"
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
    capital_repayment_on_borrowings_df = "capital_repayment_on_borrowings_df"
    income_statement_df = "income_statement_df"
    direct_cashflow_df = "direct_cashflow_df"
    loan_book_df = "loan_book_df"


class FinalFiles(str, Enum):
    income_statement_df = "income_statement_df"
    direct_cashflow_df = "direct_cashflow_df"
    loan_book_df = "loan_book_df"


class FileStage(str, Enum):
    intermediate = "intermediate"
    raw = "raw"
    final = "final"
import boto3
from decouple import config

