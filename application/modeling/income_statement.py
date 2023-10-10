import numpy as np
import pandas as pd

from application.modeling import helper


def generate_income_statement_template(valuation_date: str, months_to_forecast: int):
    income_statement_template = pd.DataFrame(
        columns=helper.generate_columns(valuation_date, months_to_forecast),
        index=pd.Index(
            [
                "Interest Income",
                "Other Income",
                "Total Revenue",
                "MANAGEMENT EXPENSES",
                "STAFF COSTS",
                "Salaries",
                "Pensions & Statutory Contributions",
                "Training",
                "Bonus Provision",
                "Retrenchments",
                "Staff Welfare",
                "CILL",
                "Total Staff Costs",
                "TRAVEL & ENTERTAINMENT",
                "Travel Costs",
                "Entertainment",
                "Total Travel & Entertainment",
                "MARKETING AND PUBLIC RELATIONS",
                "Marketing Costs",
                "Group Marketing Costs",
                "Donations",
                "Total Marketing And Public Relations",
                "OFFICE COSTS",
                "Rental Costs",
                "Subscriptions",
                "Insurance",
                "Repairs And Maintenance",
                "Utilities",
                "Stationery",
                "Admin Costs",
                "IT Costs",
                "Masawara Mgt Fee",
                "Fines And Penalties",
                "Total Office Costs",
                "PROFESSIONAL FEES",
                "Auditors Remuneration",
                "Legal Fees",
                "Strategic Expenses",
                "Directors Fees",
                "Consultancy Fees",
                "Total Professional Fees",
                "COMMUNICATION COSTS",
                "Telephones",
                "Cellphones",
                "Internet",
                "Courier",
                "Total Communication Costs",
                "MOTOR VEHICLE COSTS",
                "Fuel",
                "Motor Vehicle Maintenance Costs",
                "Total Motor Vehicle Costs",
                "OTHER COSTS",
                "Depreciation",
                "Bank Charges",
                "Zimnat Grp Shared Costs Recovery Exp",
                "Provisions",
                "Business Acquisition",
                "Total Other Costs",
                "TOTAL EXPENSES",
                "EBIDTA",
                "INVESTMENT INCOME",
                "Rental Income",
                "Interest Received",
                "Dividends Received",
                "Fair Value Adjustments",
                "Realised Gains On Disposal Of Inv Properties",
                "Realised Gains On Disposal Of Equities",
                "Exchange Gains/(Losses)",
                "Admin Fees",
                "Total Investment Income",
                "FINANCE COSTS",
                "Finance Costs",
                "Third Party",
                "Total Finance Costs",
                "PROFIT / (LOSS) BEFORE TAX",
                "Taxation",
                "2% Taxation",
                "PROFIT/(LOSS) FOR PERIOD",
            ],
            name="INCOME_STATEMENT",
        ),
    )

    return income_statement_template


def remove_na_from_income_statement_headings(income_statement: pd.DataFrame):
    non_heading_columns_in_caps = [
        "PROFIT/(LOSS) FOR PERIOD",
        "FINANCE COSTS",
        "TOTAL EXPENSES",
        "EBIDTA",
        "INVESTMENT INCOME",
        "PROFIT / (LOSS) BEFORE TAX",
    ]

    headings = income_statement.index[
        income_statement.index.str.isupper()
        & ~income_statement.index.isin(non_heading_columns_in_caps)
    ]
    income_statement.loc[headings] = ""
    return income_statement


def insert_revenue(
    income_statement: pd.DataFrame, interest_income: pd.Series, other_income: pd.Series
):
    income_statement.loc["Interest Income"] = interest_income

    income_statement.loc["Other Income"] = other_income
    income_statement.loc["Total Revenue"] = (
        income_statement.loc["Interest Income"] + income_statement.loc["Other Income"]
    )
    return income_statement


def insert_expenses(
    income_statement: pd.DataFrame,
    expenses_certain: pd.DataFrame,
    uncertain_expenses: pd.DataFrame,
):
    income_statement.loc[expenses_certain.index] = expenses_certain
    income_statement.loc[uncertain_expenses.index] = uncertain_expenses
    return income_statement


def insert_salaries_and_pensions_and_statutory_contributions(
    income_statement: pd.DataFrame,
    salaries_and_pension_and_statutory_contributions_df: pd.DataFrame,
):
    income_statement.loc[
        "Pensions & Statutory Contributions"
    ] = salaries_and_pension_and_statutory_contributions_df[
        "pensions_and_statutory_contributions"
    ]
    income_statement.loc[
        "Salaries"
    ] = salaries_and_pension_and_statutory_contributions_df["total"]

    return income_statement


def insert_depreciation(income_statement: pd.DataFrame, depreciation: pd.Series):
    income_statement.loc["Depreciation"] = depreciation
    return income_statement


def insert_credit_loss_provision(income_statement, change_in_provision_for_credit_loss):
    income_statement.loc["Provisions"] = change_in_provision_for_credit_loss
    return income_statement


def insert_business_acquisition(
    income_statement: pd.DataFrame, business_acquisition: pd.Series
):
    income_statement.loc["Business Acquisition"] = business_acquisition
    return income_statement


def aggregate_staff_costs(income_statement):
    income_statement.loc["Total Staff Costs"] = (
        income_statement.loc["Salaries":"CILL"].fillna(0).sum()
    )
    return income_statement


def aggregate_travel_and_entertainment(income_statement):
    income_statement.loc["Total Travel & Entertainment"] = (
        income_statement.loc["Travel Costs":"Entertainment"].fillna(0).sum()
    )
    return income_statement


def aggregate_marketing_and_public_relations(income_statement):
    income_statement.loc["Total Marketing And Public Relations"] = (
        income_statement.loc["Marketing Costs":"Donations"].fillna(0).sum()
    )
    return income_statement


def aggregate_office_costs(income_statement):
    income_statement.loc["Total Office Costs"] = (
        income_statement.loc["Rental Costs":"Fines And Penalties"].fillna(0).sum()
    )
    return income_statement


def aggregate_professional_fees(income_statement):
    income_statement.loc["Total Professional Fees"] = (
        income_statement.loc["Auditors Remuneration":"Consultancy Fees"].fillna(0).sum()
    )
    return income_statement


def aggregate_communication_costs(income_statement):
    income_statement.loc["Total Communication Costs"] = (
        income_statement.loc["Telephones":"Courier"].fillna(0).sum()
    )
    return income_statement


def aggregate_motor_vehicle_costs(income_statement):
    income_statement.loc["Total Motor Vehicle Costs"] = (
        income_statement.loc["Fuel":"Motor Vehicle Maintenance Costs"].fillna(0).sum()
    )
    return income_statement


def aggregate_other_costs(income_statement):
    income_statement.loc["Total Other Costs"] = (
        income_statement.loc["Depreciation":"Business Acquisition"].fillna(0).sum()
    )
    return income_statement


def aggregate_investment_income(income_statement):
    income_statement.loc["Total Investment Income"] = (
        income_statement.loc["Rental Income":"Admin Fees"].fillna(0).sum()
    )
    return income_statement


def aggregate_finance_costs(income_statement):
    income_statement.loc["Total Finance Costs"] = (
        income_statement.loc["Finance Costs":"Third Party"].fillna(0).sum()
    )
    return income_statement


def calculate_total_expenses(income_statement):
    income_statement.loc["TOTAL EXPENSES"] = (
        income_statement.loc["Total Staff Costs"]
        + income_statement.loc["Total Travel & Entertainment"]
        + income_statement.loc["Total Marketing And Public Relations"]
        + income_statement.loc["Total Office Costs"]
        + income_statement.loc["Total Professional Fees"]
        + income_statement.loc["Total Communication Costs"]
        + income_statement.loc["Total Motor Vehicle Costs"]
        + income_statement.loc["Total Other Costs"]
    )
    return income_statement


def calculate_ebidta(income_statement):
    income_statement.loc["EBIDTA"] = (
        income_statement.loc["Total Revenue"] - income_statement.loc["TOTAL EXPENSES"]
    )
    return income_statement


def calculate_profit_before_tax(income_statement):
    # print(
    #     income_statement.loc["EBIDTA"].dtype,
    #     income_statement.loc["Total Investment Income"].dtype,
    #     income_statement.loc["Total Finance Costs"].dtype,
    # )

    income_statement.loc["PROFIT / (LOSS) BEFORE TAX"] = (
        income_statement.loc["EBIDTA"]
        + income_statement.loc["Total Investment Income"]
        - income_statement.loc["Total Finance Costs"]
    )
    return income_statement


def calculate_tax(income_statement, tax_rate):
    tax_rate = helper.change_period_index_to_strftime(tax_rate)

    income_statement.loc["Taxation"] = np.maximum(
        income_statement.loc["PROFIT / (LOSS) BEFORE TAX"] * tax_rate, 0
    )
    return income_statement


def calculate_profit_or_loss_for_period(income_statement):
    income_statement.loc["PROFIT/(LOSS) FOR PERIOD"] = (
        income_statement.loc["PROFIT / (LOSS) BEFORE TAX"]
        - income_statement.loc["Taxation"]
        - income_statement.loc["2% Taxation"]
    )
    return income_statement
