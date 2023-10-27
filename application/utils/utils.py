import secrets
import string

import pyotp
import qrcode

from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0


def generate_random_password(length=12):
    """
    Generates a random password of a specified length with letters, strings and punctuation characters
    """

    characters = string.ascii_letters + string.digits + string.punctuation
    password = "".join(secrets.choice(characters) for _ in range(length))

    return password


def generate_random_key():
    secret_key = pyotp.random_base32()
    return secret_key


def check_otp(otp: str, secret_key: str, user_id: str):
    try:
        verify_otp = pyotp.TOTP(secret_key).verify(otp)

        if verify_otp:
            token = signJWT(user_id)
            return {"response": "Authenticated", "token": token["access_token"]}
        else:
            return {"response": "Incorrect OTP"}
    except:
        return {"message": "key does not exist"}


def get_endpoint_details(variable: int):
    return {
        "/": "Read Root",
        "/health": "Health",
        "/v1/login": "Login V1",
        "/login": "Login",
        "/login-verification": "Verified Login",
        "/reset-password": "Reset Password",
        "/add-audit-trial": "Add Audit Trail",
        "/extract-audit-trail": "Extracted Audit Trial",
        "/tenants": "Retrieved All Companys",
        "/tenant/tenant_name": "Retrieved Company Details",
        "/tenants/tenant_name": "Deleted Company",
        "tenants/tenant_name/toggle-active": "Changed The Active State For The Tenant",
        "/users/": "Retrieve All Users For Company",
        "/users/": "Created A User",
        "/users/forgot-password": "Sent Password Email",
        f"/users/{variable}": f"Retrieved Details For A User {variable}",
        f"/users/{variable}": f"Updated Details For A User {variable}",
        f"/users/{variable}": f"Deleted User {variable}",
        f"/users/{variable}/toggle-active": f"Changed The Active State For A User {variable}",
        "/projects/": "Created A Project",
        f"/projects/{variable}/upload-files": f"Uploaded File(s) For Project {variable}",
        f"/projects/{variable}/raw/data": f"Downloaded Raw Data File(s) For Project {variable}",
        f"/projects/{variable}/raw/data/view": f"Viewed Raw Data File(s) For Project {variable} ",
        f"/projects/{variable}/raw/filenames": f"Retrieved Raw Data Filenames For Project {variable}",
        "/projects": "Retrieved All Projects",
        "/projects/current_user": f"Retrieved Projects Made By Login User",
        f"/projects/{variable}": f"Retrieved Project {variable}",
        f"/projects/{variable}": f"Updated Project By {variable}",
        f"/projects/{variable}": f"Deleted Project {variable}",
        f"/projects/{variable}/calculations/intermediate/new-disbursements": f"Calculated New Disbursements For Project {variable}",
        f"/projects/{variable}/calculations/intermediate/loan-schedules-new-disbursements": f"Calculated Loan Schedules New Disbursements For Project {variable}",
        f"/projects/{variable}/calculations/intermediate/loan-schedules-existing-loans": f"Calculated Loan Schedules Existing Loan For Project { variable}",
        f"/projects/{variable}/calculations/intermediate/other-income": f"Calculated Other Income For Project {variable}",
        f"/projects/{variable}/calculations/intermediate/depreciation": f"Calculated Depreciation For Project {variable}",
        f"projects/{variable}/calculations/intermediate/salaries-and-pensions-and-statutory-contributions": f"Calcualated Salaries and Pensions And Statutory Contributions For Project {variable}",
        f"/projects/{variable}/calculations/intermediate/provisions": f"Calculated Provisions For Project {variable}",
        f"/projects/{variable}/calculations/intermediate/finance-costs-and-capital-repayment-on-borrowings": f"Calculated Finance Costs And Capital Repayment On Borrowings For Project {variable}",
        f"/projects/{variable}/results/intermediate/filenames": f"Retrived Intermediate Filenames For Project {variable}",
        f"/projects/{variable}/calculations/income-statement": f"Generated Income Statement For Project {variable}",
        f"/projects/{variable}/calculations/direct-cashflow": f"Generated Direct Cashflow For Project {variable}",
        f"/projects/{variable}/calculations/loan-book": f"Generated Loan Book For Project {variable}",
        f"/projects/{variable}/calculations/balance-sheet": f"Generated Balance Sheet For Project {variable}",
        f"projects/{variable}/calculations/statement-of-cashflows": f"Generated Statement of Cashflows For Project {variable}",
        f"/projects/{variable}/results": f"Downloaded Final File For Project {variable}",
        f"/projects/{variable}/results/download": f"Downloaded Final File For Project {variable}",
        f"/projects/{variable}/results/intermediate": f"Downloaded Intermediate File For Project {variable}",
        f"/projects/{variable}/results/intermediate/download": f"Downloaded Intermediate File For Project {variable}",
        f"/projects/{variable}/results/intermediate/view": f"Viewed Intermediate File For Project {variable}",
        f"/projects/{variable}/results/view": f"Viewed Final File For Project {variable}",
        f"/projects/{variable}/results/filenames": f"Retrived Final Filenames For Project {variable}",
    }


def safe_dict_lookup(key1: str, key2: str, my_dict: dict):
    return my_dict.get(key1, my_dict.get(key2))
