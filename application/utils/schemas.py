from datetime import date, datetime
from enum import Enum
from typing import List, Optional, Set, Union

from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import Column, Integer
from sqlalchemy.types import Enum as SQLAlchemyEnum

# from enum import Enum


class ProjectStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class UserRole(str, Enum):
    ADMIN = "ADMIN"
    SUPERADMIN = "SUPERADMIN"
    USER = "USER"


class otp(BaseModel):
    otp: str


class otp_login(BaseModel):
    email: str
    user_id: str
    otp: str


class passwordResetSchema(BaseModel):
    # email: str
    password: str


class EmailSchema(BaseModel):
    email: List[EmailStr]


class EmailSchemaPost(BaseModel):
    email: str


class UserBase(BaseModel):
    email: str
    id: int
    is_active: bool
    url: str


class UserCreate(UserBase):
    password: str


class ProjectCreate(BaseModel):
    project_name: str
    description: str
    start_date: date
    months_to_forecast: int
    imtt: float


class ProjectResponse(BaseModel):
    tenant_id: int
    user_id: int
    project_id: int
    project_name: str
    start_date: str
    months_to_forecast: int
    project_status: str
    imtt: float
    description: str
    created_at: datetime
    updated_at: datetime


class UserResponse(BaseModel):
    user_id: int
    tenant_id: int
    email: str
    first_name: str
    last_name: str
    is_active: bool
    role: str
    created_at: datetime
    phone_number: str
    updated_at: datetime
    created_at: datetime

    class Config:
        orm_mode = True


class UserLoginResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: int
    tenant_id: int
    email: str
    first_name: str
    last_name: str
    is_active: bool
    role: str
    created_at: datetime
    phone_number: str
    updated_at: datetime
    created_at: datetime

    class Config:
        orm_mode = True


class ResetPassword(BaseModel):
    new_password: str


class ForgotPassword(BaseModel):
    email: str

    class Config:
        orm_mode = True


# -------------------------------------------------------  FROM PARDON -----------------------------------


class TenantBaseResponse(BaseModel):
    tenant_id: int
    admin_email: str
    first_name: str
    last_name: str
    company_name: str
    physical_address: str
    phone_number: str

    class Config:
        orm_mode = True


class TenantBaseCreate(BaseModel):
    admin_email: str
    first_name: str
    last_name: str
    company_name: str
    physical_address: str
    phone_number: str

    class Config:
        orm_mode = True


class UsersBase(BaseModel):
    email: str
    user_id: int
    tenant_id: int
    first_name: str
    updated_at: str
    last_name: str
    is_active: bool
    is_admin: bool
    is_viewer: bool

    class Config:
        orm_mode = True


class UserBase(BaseModel):
    email: str
    first_name: str
    last_name: str
    phone_number: str
    work_address: str
    token: str


class UserUpdate(BaseModel):
    email: str
    first_name: str
    last_name: str
    phone_number: str
    work_address: str


class MakeAdmin(BaseModel):
    is_admin: bool


class TenantUpdate(BaseModel):
    admin_email: str
    first_name: str
    last_name: str
    phone_number: str
    physical_address: str


class UsersBaseCreate(BaseModel):
    email: str
    role: str
    first_name: str
    last_name: str
    phone_number: str

    class Config:
        orm_mode = True


class UserLogin(BaseModel):
    email: str
    password: str

    class Config:
        the_schema = {"user_signUp": {"email": "juloh@gmail.com", "password": "juloh"}}


class ProjectUpdate(BaseModel):
    project_name: str
    description: str
    start_date: date
    imtt: float
    months_to_forecast: int


class ProjectStatusUpdate(BaseModel):
    project_status: object


class AuditTrailBase(BaseModel):
    action: str
    details: Optional[str] = None


class AuditTrailResponse(AuditTrailBase):
    id: int
    timestamp: datetime

    class Config:
        orm_mode = True


class ExtractAuditTrail(BaseModel):
    start_date: datetime
    end_date: datetime


class NewFunding(BaseModel):
    effective_date: str
    institution: str
    nominal_amount: float
    interest_rate: float
    tenure: int
    repayment_frequency: str
    frequency: int
    method: str
