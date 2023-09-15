from datetime import datetime

import boto3
from decouple import config
from fastapi import HTTPException, status
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy.orm import Session
# import emails_helper
import main
import models
import schemas
import google_auth
from auth.jwt_handler import decodeJWT, signJWT, signJWT0
# from modeling import helper
from io import BytesIO
import base64
import qrcode

from aws_helper import helper


def get_users(db: Session, tenant_id: str):
    return db.query(models.Users).filter(models.Users.tenant_id == tenant_id).all()


def get_tenant_name(tenant_id: str, db: Session):
    tenant_name = (
        db.query(models.Tenant).filter(
            models.Tenant.tenant_id == tenant_id).first()
    )
    return tenant_name.company_name


def get_user_by_email(db: Session, email: str):
    try:
        user = db.query(models.Users).filter(
            models.Users.email == email).first()
        if user is not None:
            return user
        else:
            return {"response": "user does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}
    except:
        return {"response": "user does not exist ", "statusCode": status.HTTP_404_NOT_FOUND}


def delete_by_email(db: Session, email: str):
    try:
        user = get_user_by_email(db=db, email=email)
        db.delete(user)
        db.commit()
        context = {"response": "user successfully deleted ",
                   "statusCode": status.HTTP_200_OK}, status.HTTP_200_OK
        return context
    except:
        context = {"response": "user does not exist ",
                   "statusCode": status.HTTP_404_NOT_FOUND}
        return context


def update_by_email(email: str, edit_user: schemas.UserUpdate, db: Session):
    # try:
    user = get_user_by_email(db=db, email=email)

    user.email = edit_user.email
    user.first_name = edit_user.first_name
    user.last_name = edit_user.last_name
    user.phone_number = edit_user.phone_number
    user.work_address = edit_user.work_address
    db.commit()
    context = {"response": "successfully updated",
               "status_code": status.HTTP_200_OK}
    return context


def get_email_domain(email):
    # Extract the domain from the email address
    return email.split("@")[1]


def get_user(db: Session, user_id: int, email: str):
    try:
        user = db.query(models.Users).filter(
            (models.Users.user_id == user_id) & (models.Users.email == email)
        ).first()

        if user:
            tenant_name = get_tenant_name(db=db, tenant_id=user.tenant_id)
            user.tenant_name = tenant_name
            return user
        else:
            return {"response": "token expired", "statusCode": status.HTTP_403_FORBIDDEN}
    except:
        return {"response": "token expired", "statusCode": status.HTTP_403_FORBIDDEN}


def decrypt(db: Session, token: str):
    payload = decodeJWT(token)

    try:
        users = get_user(
            db=db, user_id=payload["user_id"], email=payload["email"])
        return users
    except:
        return {"response": "token expired"}


def create_user(db: Session, user: schemas.UsersBaseCreate, password: str, secret_key: str):
    admin = decrypt(db=db, token=user.token)
    registrar_domain = get_email_domain(admin.email)
    new_user_domain = get_email_domain(user.email)

    # if registrar_domain == new_user_domain:
    try:
        # tenant= db.query(models.Tenant).filter(models.Tenant.tenant_id == admin.tenant_id).first()

        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        my_hashed_password = pwd_context.hash(password)

        try:
            users = (
                db.query(models.Users)
                .filter(models.Users.email == user.email)
                .first()
            )
            if users is None:
                db_user = models.Users(
                    email=user.email,
                    hashed_password=my_hashed_password,
                    first_name=user.first_name,
                    last_name=user.last_name,
                    tenant_id=admin.tenant_id,
                    phone_number=user.phone_number,
                    work_address=user.work_address,
                    is_active=False,
                    secret_key=secret_key

                )
                db.add(db_user)
                db.commit()
                db.refresh(db_user)
                # activate_user()
                return {"response": "user successfully added"}
            else:
                return {"response": "user already exist"}
        except:
            return {"response": "tenant does not exist"}
        # else:
        # return {"response":"tenant does not exist"}
    except:
        return {"response": "token expired"}


def create_base64_qrcode_image(uri: str):

    qrcode_image = qrcode.make(uri)

    buffer = BytesIO()

    qrcode_image.save(buffer)

    image_bytes = buffer.getvalue()

    qrcode_image_base64 = base64.b64encode(image_bytes).decode('utf-8')

    return qrcode_image_base64
