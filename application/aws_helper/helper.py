import os
from enum import Enum
from functools import reduce
from io import BytesIO
from typing import Union

import awswrangler as wr
import boto3
import pandas as pd
from boto3.session import Session
from botocore.exceptions import BotoCoreError, ClientError
from decouple import config
from fastapi import HTTPException, status

AWS_ACCESS_KEY_ID = config("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config("AWS_SECRET_ACCESS_KEY")

S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)

MY_SESSION = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="af-south-1",
)

SNS_CLIENT = boto3.client(
    "sns",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="us-east-1",
)

SES_CLIENT = boto3.client(
    "ses",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name="us-east-1",
)


def make_bucket(tenant_name: str, s3_client: Session):
    if not "-budgeting" in tenant_name:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="tenant name shoud end with -budgeting",
        )
    response = {}
    all_buckets_info = s3_client.list_buckets()
    all_buckets = all_buckets_info["Buckets"]
    for bucket in all_buckets:
        if tenant_name == bucket["Name"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN, detail="Tenant already exist"
            )
    location = {"LocationConstraint": "af-south-1"}
    try:
        s3_client.create_bucket(Bucket=tenant_name, CreateBucketConfiguration=location)
        response["message"] = f"{tenant_name} has been registered successifully"
        return response
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))



