import datetime
import io
import json
import os
import random
import string
import urllib
from datetime import datetime, timedelta
from typing import List, Union

import awswrangler as wr
import boto3
import pandas as pd
import pyotp
import qrcode
from botocore.exceptions import ClientError
from decouple import config
from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from passlib.context import CryptContext
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

import main as main
from application.auth.jwt_bearer import JwtBearer
from application.auth.jwt_handler import decodeJWT, signJWT
from application.auth.security import get_current_active_user
from application.routes.tenants import crud
from application.utils import models, schemas, utils
from application.utils.database import SessionLocal, engine, get_db

router = APIRouter(tags=["TENANTS MANAGEMENT"])


def generate_login_creds():
    secret_key = utils.generate_random_key()
    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name="CBS IFRS17"
    )
    qrcode_image = crud.create_base64_qrcode_image(uri)

    return secret_key, qrcode_image


@router.post("/tenants/")
async def create_tenant(
    tenant: schemas.TenantBaseCreate, db: Session = Depends(get_db)
):
    random_password = utils.generate_random_password()
    secret_key = pyotp.random_base32()

    uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
        name="Claxon", issuer_name="CBS Budgetting"
    )

    print(random_password)

    qrcode_image = crud.create_base64_qrcode_image(uri)
    # try:

    crud.create_tenant(
        db=db, tenant=tenant, password=random_password, secret_key=secret_key
    )

    # return await crud.activate_admin_sendgrid(body, password=encryption_key, url=url, qrcode_image=qrcode_image)
    return "tenant successfully created"
    # except:
    #     return {"response":"tenant not created"}


@router.get("/tenant/{tenant_name}")
def get_tenant(
    tenant_name: str,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
) -> Union[schemas.TenantBaseResponse, dict, None]:
    # tenant_id=1
    return crud.get_tenant_by_tenant_name(tenant_name=tenant_name, db=db)


@router.get("/tenants/", response_model=List[schemas.TenantBaseResponse])
def get_tenants(
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    tenants = crud.get_tenants(db)
    return tenants


@router.delete("/tenants/{tenant_name}")
async def delete_tenant(
    tenant_name: str,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    if current_user.role != schemas.UserRole.SUPERADMIN:
        raise HTTPException(
            detail="You are not authorized to perform action",
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    db_tenant = crud.get_tenant_by_tenant_name(db=db, tenant_name=tenant_name)

    if db_tenant is None:
        raise HTTPException(
            detail="Tenant does not exist", status_code=status.HTTP_404_NOT_FOUND
        )

    crud.delete_tenant_by_tenant_name(db=db, tenant_name=tenant_name)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.put("/tenants/{tenant_name}")
def update_tenant(
    tenant_name: str,
    edit_tenant: schemas.TenantUpdate,
    db: Session = Depends(get_db),
    current_user: schemas.UserLoginResponse = Depends(get_current_active_user),
):
    return crud.update_Tenant(tenant_name=tenant_name, edit_tenant=edit_tenant, db=db)
