from datetime import datetime
from typing import List, Optional, Set, Union
import enum
from sqlalchemy import Column, Integer
from sqlalchemy.types import Enum as SQLAlchemyEnum
from pydantic import BaseModel, EmailStr, Field
# from enum import Enum


class ProjectStatusEnum(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


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


class UserCreate(UserBase):
    password: str


class User(UserBase):
    class Config:
        orm_mode = True


class ProjectsBase(BaseModel):
    project_id: int
    project_name: str
    start_date: str
    user_id: int
    description: str
    project_status: object


class ProjectsCreate(ProjectsBase):
    project_id: int
    tenant_id: str


class Projects(ProjectsBase):
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
    tenant_id: int
    admin_email: str
    first_name: str
    last_name: str
    company_name: str
    physical_address: str
    phone_number: str
    created_at: str
    password: str

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


class UserBaseRead(BaseModel):
    tenant_id: int
    email: str
    last_name: str
    is_active: bool
    is_creator: bool
    created_at: datetime
    phone_number: str
    user_id: int
    first_name: str
    is_admin: bool
    is_viewer: bool
    updated_at: datetime
    work_address: str

    class Config:
        orm_mode = True


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


class UsersBaseCreate(UserBase):
    password: str
    url: str

    class Config:
        orm_mode = True


# ---------------------------Security Schemas-----------------------------


class Token(BaseModel):
    access_token: str
    token_type: str


class UseToken(BaseModel):
    token: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class UserLoginSchema(BaseModel):
    email: str
    password: str

    class Config:
        the_schema = {"user_signUp": {
            "email": "juloh@gmail.com", "password": "juloh"}}


class ProjectUpdate(BaseModel):
    project_name: str

    description: str


class AuditTrailAL(BaseModel):
    id: int
    timestamp: datetime = datetime.now()
    token: str
    action: str

    details: Optional[str] = None


class ExtractAuditTrail(BaseModel):
    start_date: str
    end_date: str
    token: str


class AuditTrail(BaseModel):
    id: int
    timestamp: datetime = datetime.now()
    email_address: str
    action: str
    details: Optional[str] = None
