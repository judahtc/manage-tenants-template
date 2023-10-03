
import json
from typing import List, Union
from application.utils import schemas
from fastapi import APIRouter, HTTPException, status, Response
import pandas as pd
import boto3

import awswrangler as wr
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from application.utils import models
from application.auth.jwt_handler import signJWT
from fastapi import HTTPException, status
from passlib.context import CryptContext
from fastapi.responses import JSONResponse
import boto3
from decouple import config
import main
import random
import string
from application.auth.jwt_handler import signJWT, decodeJWT
from fastapi import FastAPI, HTTPException, status, File, UploadFile, Depends, Form, Header, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from application.utils import models
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
from application.routes.tenants import crud
from application.utils import google_auth
import pyotp
import qrcode


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


router = APIRouter(tags=["TENANTS MANAGEMENT"])


def generate_login_creds():
    secret_key = google_auth.generate_random_key()
    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name='CBS IFRS17')
    qrcode_image = crud.create_base64_qrcode_image(uri)

    return secret_key, qrcode_image


@router.post("/tenants/")
async def create_tenant(
    tenant: schemas.TenantBaseCreate, db: Session = Depends(get_db)
):
    body = tenant.admin_email
    characters = string.ascii_letters + string.digits + string.punctuation
    random_string = "".join(random.choice(characters) for i in range(8))
    encryption_key = random_string

    # generate random google auth key
    secret_key = google_auth.generate_random_key()
    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name='CBS Budgetting')
    qrcode_image = crud.create_base64_qrcode_image(uri)
    # try:
    crud.create_tenant(db=db, tenant=tenant,
                       password=encryption_key, secret_key=secret_key)
    # return await crud.activate_admin_sendgrid(body, password=encryption_key, url=url, qrcode_image=qrcode_image)
    return "tenant successfully created"
    # except:
    #     return {"response":"tenant not created"}


@router.get("/tenants/", response_model=List[schemas.TenantBaseResponse])
def read_tenants(
    db: Session = Depends(get_db), current_user: dict = Depends(JwtBearer())
):
    tenants = crud.get_tenants(db)
    return tenants


@router.get("/tenant/{tenant_name}")
def get_tenant(tenant_name: str, db: Session = Depends(get_db)) -> Union[schemas.TenantBaseResponse, dict, None]:
    # tenant_id=1
    return crud.get_tenant_by_tenant_name(tenant_name=tenant_name, db=db)


@router.delete("/tenants/{tenant_name}")
async def delete_tenant(tenant_name: str, db: Session = Depends(get_db)):
    try:
        return crud.delete_tenant(db=db, tenant_name=tenant_name)
    except:
        return {"response": "tenant does not exist "}


@router.put("/tenants/{tenant_name}")
def update_tenant(
    tenant_name: str,
    edit_tenant: schemas.TenantUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(JwtBearer()),
):
    return crud.update_Tenant(tenant_name=tenant_name, edit_tenant=edit_tenant, db=db)
