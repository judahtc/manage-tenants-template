from enum import Enum
from typing import List

import awswrangler as wr
import pandas as pd
from botocore.exceptions import ClientError
from fastapi import File, HTTPException, Response, UploadFile, status

from application.modeling import constants


def get_tenant_name(tenant_name: str):
    """Appends the '-budgeting' suffix to the tenant name. This is to match the bucket name in s3 since that was also added when a tenant was registered"""
    if "-budgeting" in tenant_name:
        return tenant_name
    return tenant_name + "-budgeting"


def make_bucket(tenant_name: str, s3_client):
    tenant_name = get_tenant_name(tenant_name)
    response = {}
    # all_buckets_info = s3_client.list_buckets()
    # all_buckets = all_buckets_info["Buckets"]
    # for bucket in all_buckets:
    #     if tenant_name == bucket["Name"]:
    #         raise HTTPException(
    #             status_code=status.HTTP_403_FORBIDDEN, detail="Tenant already exist"
    #         )
    location = {"LocationConstraint": "af-south-1"}
    try:
        s3_client.create_bucket(Bucket=tenant_name, CreateBucketConfiguration=location)
        response["message"] = f"{tenant_name} has been registered successifully"
        return response
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def upload_multiple_files(
    project_id: int,
    tenant_name: str,
    my_session,
    files: List[UploadFile] = File(...),
):
    filenames = get_list_from_string_enum(constants.RawFiles)

    for i in files:
        for j in filenames:
            if i.filename.startswith(j):
                print(i.filename)
                temp = pd.read_csv(i.file)
                temp.columns = temp.columns.str.strip()
                wr.s3.to_parquet(
                    df=temp,
                    path=f"s3://{tenant_name}/project_{project_id}/raw/{j}.parquet",
                    boto3_session=my_session,
                )
    return {"message": "done"}


def upload_file(
    project_id: int,
    tenant_name: str,
    boto3_session,
    file: pd.DataFrame,
    file_name: Enum,
    file_stage: constants.FileStage,
):
    try:
        wr.s3.to_parquet(
            df=file,
            path=f"s3://{tenant_name}/project_{project_id}/{file_stage.value}/{file_name.value}.parquet",
            boto3_session=boto3_session,
            index=True,
        )

        print(
            f"s3://{tenant_name}/project_{project_id}/{file_stage.value}/{file_name.value}.parquet"
        )

        return {"message": "Files uploaded successfully"}
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_expenses_file(
    tenant_name: str,
    project_id: int,
    months_to_forecast: int,
    start_date: str,
    boto3_session,
    file_name: Enum,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/raw/{file_name.value}.parquet",
            boto3_session=boto3_session,
        )

        df = df.set_index(df.columns[0])
        df.index.name = ""
        df = match_months_to_forecast_with_df(
            df=df,
            months_to_forecast=months_to_forecast,
            start_date=start_date,
        )

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_raw_file(
    tenant_name: str,
    project_id: int,
    boto3_session,
    file_name: Enum,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/raw/{file_name.value}.parquet",
            boto3_session=boto3_session,
        )

        # df = df.set_index(df.columns[0])
        # df.index.name = ""
        df.rename({"Unnamed:_0": ""}, axis=1, inplace=True)

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_disbursement_parameters_file(
    tenant_name: str,
    project_id: int,
    boto3_session,
    start_date: str,
    months_to_forecast: int,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/raw/disbursement_parameters.parquet",
            boto3_session=boto3_session,
        )

        df = df.set_index(df.columns[0])
        df.index.name = ""
        df = match_months_to_forecast_with_df(
            df=df,
            months_to_forecast=months_to_forecast,
            start_date=start_date,
        )

        df.columns = list(map(str, df.columns))

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_other_parameters_file(
    tenant_name: str,
    project_id: int,
    boto3_session,
    start_date: str,
    months_to_forecast: int,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/raw/other_parameters.parquet",
            boto3_session=boto3_session,
        )

        df = df.set_index(df.columns[0])
        df.index.name = ""

        df = match_months_to_forecast_with_df(
            df=df,
            months_to_forecast=months_to_forecast,
            start_date=start_date,
        )

        df.columns = list(map(str, df.columns))

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_intermediate_file(
    tenant_name: str,
    project_id: int,
    boto3_session,
    file_name: Enum,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/intermediate/{file_name.value}.parquet",
            boto3_session=boto3_session,
        )

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def read_final_file(
    tenant_name: str,
    project_id: int,
    boto3_session,
    file_name: Enum,
):
    try:
        df = wr.s3.read_parquet(
            f"s3://{tenant_name}/project_{project_id}/final/{file_name.value}.parquet",
            boto3_session=boto3_session,
        )

        return df
    except ClientError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))


def add_period_index(series: pd.Series, start_date: str, periods: int):
    index = pd.period_range(start=start_date, periods=periods, freq="M").strftime(
        "%b-%Y"
    )
    series.index = index
    return series


def shift(df: pd.DataFrame):
    for i in range(len(df)):
        df.iloc[i] = df.iloc[i].shift(i)
    return df.fillna(0)


def generate_columns(start_date: str, period: int):
    return pd.period_range(start=start_date, periods=period, freq="M").strftime("%b-%Y")


def add_series(list_of_series: list):
    return pd.concat(list_of_series, axis=1).sum(axis=1)


def change_period_index_to_strftime(input: pd.DataFrame | pd.Series):
    input.index = pd.PeriodIndex(input.index, freq="M")
    input.index = input.index.strftime("%b-%Y")
    return input


def convert_to_datetime(date: pd.Series):
    try:
        date = pd.to_datetime(date, format="%d/%m/%Y")
    except:
        date = pd.to_datetime(date, format="%m/%d/%Y")

    return date


def columns_to_snake_case(df: pd.DataFrame):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def columns_to_screaming_snake_case(df: pd.DataFrame):
    df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_")
    return df


def remove_na_from_headings(df: pd.DataFrame):
    headings = df.index[df.index.str.isupper()]
    df.loc[headings] = ""
    return df


def convert_index(df: pd.DataFrame, to_string: str = False):
    if to_string:
        df.index = list(map(str, df.index))
    else:
        df.index = pd.PeriodIndex(df.index, freq="M")
    return df


def convert_columns(df: pd.DataFrame):
    df.columns = list(map(str, df.columns))
    return df


def fix_index(df: pd.DataFrame):
    df.columns = [i.replace("_", " ") for i in df.columns]
    return df


def get_list_from_string_enum(enum: Enum):
    return [item.value for item in enum]


def calculate_opening_and_closing_balances(df: pd.DataFrame):
    for index, period in enumerate(df.columns):
        closing_balance_iloc = df.index.get_loc("Closing Balance")
        opening_balance_iloc = df.index.get_loc("Opening Balance")
        df.iloc[closing_balance_iloc, index] = df.iloc[
            opening_balance_iloc:closing_balance_iloc, index
        ].sum()

        if period == df.columns[-1]:
            break
        df.iloc[opening_balance_iloc, index + 1] = df.iloc[closing_balance_iloc, index]
    return df


def get_tenant_name(tenant_name: str):
    """Appends the '-budgeting' suffix to the tenant name. This is to match the bucket name in s3 since that was also added when a tenant was registered"""
    return tenant_name + "-ifrs-9"


def make_bucket(tenant_name: str, s3_client):
    tenant_name = get_tenant_name(tenant_name)
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


def match_months_to_forecast_with_df(
    df: pd.DataFrame, months_to_forecast: int, start_date: str
):
    number_of_months_in_df = df.columns.shape[0]

    new_columns = pd.period_range(
        start=start_date, periods=months_to_forecast, freq="M"
    ).strftime("%b-%Y")

    if number_of_months_in_df == months_to_forecast:
        df.columns = new_columns
        return df

    if number_of_months_in_df > months_to_forecast:
        df.columns = pd.date_range(
            start=start_date, periods=int(df.columns[-1]), freq="M"
        ).strftime("%b-%Y")

        return df.loc[:, new_columns]

    number_of_months_to_add = months_to_forecast - number_of_months_in_df
    repeated_last_column = df[df.columns[-1]].repeat(number_of_months_to_add)
    reshaped_repated_last_column_df = pd.DataFrame(
        repeated_last_column.values.reshape(-1, number_of_months_to_add),
        index=df.index,
    )

    new_df = pd.concat(
        [df, reshaped_repated_last_column_df],
        axis=1,
    )

    new_df.columns = new_columns

    return new_df


def group_next_year_on_wards(df: pd.DataFrame):
    df_copy = df.copy()
    df_copy.columns = pd.to_datetime(df_copy.columns, format="%b-%Y")
    df_yearly = df_copy.groupby(df_copy.columns.year, axis=1).sum()
    df_yearly.columns = df_yearly.columns.astype(str)

    # current_year = str(pd.to_datetime(df.columns[0]).year)
    # df_current_year_in_months = df.T.loc[df.columns.str.endswith(current_year)].T

    return df_yearly
