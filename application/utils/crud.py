import base64
from datetime import datetime

# from modeling import helper
from io import BytesIO

import boto3
import qrcode
from decouple import config
from fastapi import HTTPException, status
from passlib.context import CryptContext
from sqlalchemy.orm import Session

# import emails_helper
import main as main
from application.auth.jwt_handler import decodeJWT, signJWT, signJWT0
from application.utils import models, schemas, utils


def verify_password(plain_password, hashed_password):
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    return pwd_context.hash(password)


def check_user(db: Session, user: schemas.UserLogin):
    db_user = db.query(models.Users).filter(models.Users.email == user.email).first()

    if db_user is None:
        return False

    if not verify_password(user.password, db_user.hashed_password):
        return False

    response = signJWT0(db_user.user_id, user.email)
    response["email"] = db_user.email
    response["first_name"] = db_user.first_name
    response["last_name"] = db_user.last_name
    response["is_active"] = db_user.is_active

    return response
