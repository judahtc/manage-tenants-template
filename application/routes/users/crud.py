import base64
from datetime import datetime
from io import BytesIO

import boto3
import qrcode
from decouple import config
from fastapi import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy.orm import Session

import main as main
from application.auth import security
from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0
from application.aws_helper import helper
from application.utils import models, schemas, utils


def get_users(db: Session, tenant_id: str):
    return db.query(models.Users).filter(models.Users.tenant_id == tenant_id).all()


def get_tenant_name(tenant_id: str, db: Session):
    tenant_name = (
        db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()
    )
    return tenant_name.company_name


def get_user_by_email(db: Session, email: str) -> models.Users:
    return db.query(models.Users).filter(models.Users.email == email).first()


def get_user_by_id(db: Session, user_id: int) -> models.Users:
    return db.query(models.Users).get(user_id)


def delete_by_email(db: Session, email: str) -> models.Users:
    user = get_user_by_email(db=db, email=email)
    db.delete(user)
    db.commit()
    return user


def update_by_email(email: str, edit_user: schemas.UserUpdate, db: Session):
    user = get_user_by_email(db=db, email=email)

    user.email = edit_user.email
    user.first_name = edit_user.first_name
    user.last_name = edit_user.last_name
    user.phone_number = edit_user.phone_number
    user.work_address = edit_user.work_address
    db.commit()
    context = {"response": "successfully updated", "status_code": status.HTTP_200_OK}
    return context


def get_email_domain(email):
    # Extract the domain from the email address
    return email.split("@")[1]


def get_user(db: Session, user_id: int, email: str):
    try:
        user = (
            db.query(models.Users)
            .filter((models.Users.user_id == user_id) & (models.Users.email == email))
            .first()
        )

        if user:
            tenant_name = get_tenant_name(db=db, tenant_id=user.tenant_id)
            user.tenant_name = tenant_name
            return user
        else:
            return {
                "response": "token expired",
                "statusCode": status.HTTP_403_FORBIDDEN,
            }
    except:
        return {"response": "token expired", "statusCode": status.HTTP_403_FORBIDDEN}


def decrypt(db: Session, token: str):
    payload = decodeJWT(token)

    try:
        users = get_user(db=db, user_id=payload["user_id"], email=payload["email"])
        return users
    except:
        return {"response": "token expired"}


def create_user(
    db: Session,
    user: schemas.UsersBaseCreate,
    secret_key: str,
    password: str,
    admin,
) -> models.Users:
    db_user = db.query(models.Users).filter(models.Users.email == user.email).first()

    if db_user is not None:
        raise HTTPException(
            detail="User already exists", status_code=status.HTTP_409_CONFLICT
        )

    db_user = models.Users(
        email=user.email,
        hashed_password=security.get_password_hash(password),
        first_name=user.first_name,
        last_name=user.last_name,
        tenant_id=admin.tenant_id,
        phone_number=user.phone_number,
        role=schemas.UserRole.USER,
        is_active=True,
        secret_key=secret_key,
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user


def create_base64_qrcode_image(uri: str):
    qrcode_image = qrcode.make(uri)

    buffer = BytesIO()

    qrcode_image.save(buffer)

    image_bytes = buffer.getvalue()

    qrcode_image_base64 = base64.b64encode(image_bytes).decode("utf-8")

    return qrcode_image_base64
