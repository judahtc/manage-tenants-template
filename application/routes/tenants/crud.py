import base64
from datetime import datetime

# from modeling import helper
from io import BytesIO

import boto3
import qrcode
from decouple import config
from fastapi import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy.orm import Session

# import emails_helper
import main as main
from application.auth import security
from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0
from application.aws_helper import helper
from application.utils import models, schemas, utils

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
    region_name="af-south-1",
)

ses_client = boto3.client(
    "ses",
    aws_access_key_id=config("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1",
)


def create_tenant(
    db: Session, tenant: schemas.TenantBaseResponse, password: str, secret_key: str
):
    user = (
        db.query(models.Users).filter(models.Users.email == tenant.admin_email).first()
    )

    if user is not None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=f"User already exist"
        )

    response = helper.make_bucket(tenant_name=tenant.company_name, s3_client=s3_client)

    hashed_password = security.get_password_hash(password=password)

    db_tenant = models.Tenant(
        admin_email=tenant.admin_email,
        first_name=tenant.first_name,
        last_name=tenant.last_name,
        company_name=tenant.company_name,
        physical_address=tenant.physical_address,
        phone_number=tenant.phone_number,
    )

    db.add(db_tenant)
    db.commit()
    db.refresh(db_tenant)

    db_user = models.Users(
        tenant_id=db_tenant.tenant_id,
        email=tenant.admin_email,
        hashed_password=hashed_password,
        first_name=tenant.first_name,
        last_name=tenant.last_name,
        phone_number=tenant.phone_number,
        secret_key=secret_key,
        role=schemas.UserRole.ADMIN,
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return response


def create_base64_qrcode_image(uri: str):
    qrcode_image = qrcode.make(uri)
    buffer = BytesIO()
    qrcode_image.save(buffer)
    image_bytes = buffer.getvalue()
    qrcode_image_base64 = base64.b64encode(image_bytes).decode("utf-8")

    return qrcode_image_base64


def get_tenants(db: Session):
    tenants = db.query(models.Tenant).all()
    return tenants


def get_tenant_by_tenant_name(tenant_name: str, db: Session):
    return (
        db.query(models.Tenant)
        .filter(models.Tenant.company_name == tenant_name)
        .first()
    )


def get_tenant_by_id(db: Session, tenant_id: int):
    return db.query(models.Tenant).get(tenant_id)


def delete_tenant_by_tenant_name(db: Session, tenant_name: str):
    tenant = get_tenant_by_tenant_name(db=db, tenant_name=tenant_name)
    db.delete(tenant)
    db.commit()
    return tenant


def update_Tenant(tenant_name: str, edit_tenant: schemas.TenantUpdate, db: Session):
    # try:
    tenant = get_tenant_by_tenant_name(db=db, tenant_name=tenant_name)

    tenant.admin_email = edit_tenant.admin_email
    tenant.first_name = edit_tenant.first_name
    tenant.last_name = edit_tenant.last_name
    tenant.phone_number = edit_tenant.phone_number
    tenant.physical_address = edit_tenant.physical_address
    db.commit()

    return {"response": "tenant successfully updated"}
