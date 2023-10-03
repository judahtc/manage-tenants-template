import json
from application.utils import schemas
from fastapi import APIRouter, HTTPException, status, Response
import pandas as pd
import boto3
from typing import Optional
from typing import Union
import awswrangler as wr
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from application.utils import models
from application.auth.jwt_handler import signJWT, decodeJWT
from fastapi import HTTPException, status
from passlib.context import CryptContext
from fastapi.responses import JSONResponse
import boto3
from decouple import config
import main
import random
import string
from fastapi import FastAPI, HTTPException, status, File, UploadFile, Depends, Form, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib
from application.auth.jwt_bearer import JwtBearer
from decouple import config
from botocore.exceptions import ClientError
import awswrangler as wr
import boto3
import json
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from fastapi.responses import JSONResponse
import random
import string
import datetime
from fastapi.responses import StreamingResponse
from fastapi_mail import FastMail, MessageSchema, ConnectionConfig
from fastapi.responses import FileResponse
import os
import io
from application.utils.database import SessionLocal, engine
from application.routes.users import crud
from application.utils import google_auth
import pyotp
import qrcode
router = APIRouter(tags=["USER MANAGEMENT"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/users/")
async def create_user(user: schemas.UsersBaseCreate, db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())):
    # try:
    print(current_user)
    email = current_user["email"]
    body = user.email
    # url = "user.url"
    print(body)
    characters = string.ascii_letters + string.digits + string.punctuation

    # Generate the random string
    random_string = "".join(random.choice(characters) for i in range(8))
    encryption_key = random_string

    # Generate the random google auth string
    secret_key = google_auth.generate_random_key()
    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name='CBS Budgetting')

    qrcode_image = crud.create_base64_qrcode_image(uri)
    response = crud.create_user(
        db=db, user=user, password=encryption_key, secret_key=secret_key, email=email)
    # if response["response"] == "user successfully added":
    #     return await crud.user_reg_email_sendgrid(body, password=encryption_key, url=url, qrcode_image=qrcode_image)
    # else:
    #     return response

    # comment the next statement when you activate emails by uncommenting the above commented code
    return response


@router.get("/users/")
# dependencies=[Depends(JWTBearer())] ,
def read_users(
    db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    user_id = current_user.get("user_id")
    # user_id = 1
    admin = db.query(models.Users).filter(
        models.Users.user_id == user_id).first()
    tenant_id = admin.tenant_id
    # tenant_id='5'
    users = crud.get_users(db, tenant_id=tenant_id)
    print(users)

    return users


@router.get("/users/{email}")
async def read_user_by_email(email: str, db: Session = Depends(get_db)):
    try:
        users = crud.get_user_by_email(db=db, email=email)
        return users
    except:
        return {"response": "user does not exist "}


@router.delete("/users/{email}")
async def delete_user_by_email(
    email: str, db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
) -> dict:
    try:
        return crud.delete_by_email(db=db, email=email)
    except:
        return {"response": "user does not exist "}


@router.put("/users/{email}")
def update_user(
    email: str,
    edit_user: schemas.UserUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    return crud.update_by_email(email, edit_user=edit_user, db=db)
