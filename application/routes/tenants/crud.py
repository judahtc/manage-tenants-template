from datetime import datetime

import boto3
from decouple import config
from fastapi import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy.orm import Session
# import emails_helper
import main
from application.utils import models
from application.utils  import schemas
from application.utils  import google_auth
from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0
# from modeling import helper
from io import BytesIO
import base64
import qrcode

from application.aws_helper import helper

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


def create_tenant(db: Session, tenant: schemas.TenantBaseResponse, password: str, secret_key: str):
    user = (
        db.query(models.Users).filter(
            models.Users.email == tenant.admin_email).first()
    )

    if user is not None:
        # return "user exist"
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=f"User already exist"
        )

    response = helper.make_bucket(
        tenant_name=tenant.company_name, s3_client=s3_client)
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    my_hashed_password = pwd_context.hash(password)
    fake_hashed_password = my_hashed_password
    db_tenant = models.Tenant(
        admin_email=tenant.admin_email,
        password=fake_hashed_password,
        first_name=tenant.first_name,
        last_name=tenant.last_name,
        company_name=tenant.company_name,
        physical_address=tenant.physical_address,
        phone_number=tenant.phone_number,

    )
    db.add(db_tenant)
    db.commit()

    db.refresh(db_tenant)
    try:
        result = main.engine.execute(
            "SELECT MAX(tenant_id) FROM tenants").fetchall()
        tenants_count = result[0].max
        # tenants_count=db.query(models.Tenant).count()
        db_user = models.Users(
            email=tenant.admin_email,
            hashed_password=my_hashed_password,
            first_name=tenant.first_name,
            last_name=tenant.last_name,
            tenant_id=tenants_count,
            is_admin=True,
            is_active=False,
            phone_number=tenant.phone_number,
            work_address=tenant.physical_address,
            secret_key=secret_key
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return {"s3": response}

    except:
        tenants_count = 1
        db_user = models.Users(
            email=tenant.admin_email,
            hashed_password=my_hashed_password,
            first_name=tenant.first_name,
            last_name=tenant.last_name,
            tenant_id=tenants_count,
            is_admin=True,
            is_active=False,
            phone_number=tenant.phone_number,
            work_address=tenant.physical_address,
            secret_key=secret_key
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        status_code = 200
        return status_code


def create_base64_qrcode_image(uri: str):

    qrcode_image = qrcode.make(uri)

    buffer = BytesIO()

    qrcode_image.save(buffer)

    image_bytes = buffer.getvalue()

    qrcode_image_base64 = base64.b64encode(image_bytes).decode('utf-8')

    return qrcode_image_base64


def get_tenants(db: Session):
    tenants = db.query(models.Tenant).all()
    return tenants


def get_tenant_by_tenant_name(tenant_name: str, db: Session):
    try:
        tenant = (
            db.query(models.Tenant).filter(
                models.Tenant.company_name == tenant_name).first()
        )

        if tenant is not None:
            return tenant
        else:
            return {"response": "tenant does not exist"}

    except:
        return {"response": "tenant does not exist"}


def delete_tenant(db: Session, tenant_name: str):
    try:
        tenant = get_tenant_by_tenant_name(db=db, tenant_name=tenant_name)
        if tenant is not None:
            db.delete(tenant)
            db.commit()
            return {"response": "tenant successfully deleted "}
        else:
            return {"response": "tenant does not exist "}
    except:
        return {"response": "tenant does not exist "}


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