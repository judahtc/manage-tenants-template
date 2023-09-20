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


def verify_password(plain_password, hashed_password):
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    return pwd_context.hash(password)


def check_user(db: Session, user: schemas.UserLoginSchema):
    user = db.query(models.Users).filter(
        models.Users.email == user.email).first()

    if user is None:
        raise HTTPException(status_code=404, detail="user not found")

    if not verify_password(user.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="not authorized")

    response = signJWT0(user.user_id, user.email)
    response['phone'] = user.phone_number
    response['is_active'] = user.is_active

    return response
