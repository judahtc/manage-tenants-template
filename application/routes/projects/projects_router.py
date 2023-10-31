import io
from typing import List

import awswrangler as wr
import pandas as pd
from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from application.auth.security import get_current_active_user
from application.aws_helper.helper import MY_SESSION
from application.modeling import constants, helper
from application.routes.projects import crud
from application.utils import models, schemas
from application.utils.database import get_db

router = APIRouter(
    tags=["PROJECTS MANAGEMENT"], dependencies=[Depends(get_current_active_user)]
)


@router.post("/projects/")
def create_project(
    project: schemas.ProjectCreate,
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = crud.create_projects(
        user_id=current_user.user_id,
        tenant_id=current_user.tenant_id,
        db=db,
        project=project,
    )

    return project


@router.post("/projects/{project_id}/upload-files")
def upload_project_files(
    project_id: int,
    files: List[UploadFile] = File(...),
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    project = crud.get_project_by_id(db=db, project_id=project_id)

    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project does not exist"
        )

    crud.update_project_status(
        project_id=project_id, status=schemas.ProjectStatus.IN_PROGRESS, db=db
    )

    return helper.upload_multiple_files(
        project_id=project_id,
        tenant_name=current_user.tenant.company_name,
        my_session=MY_SESSION,
        files=files,
    )


@router.get("/projects/{project_id}/raw/data")
def download_raw_file(
    project_id: str,
    file_name: constants.RawFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    df = helper.read_raw_file(
        tenant_name=current_user.tenant.company_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    if file_name == constants.RawFiles.existing_loans:
        df = df.head(50)

    stream = io.StringIO()
    df.to_csv(stream, index=True)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers[
        "Content-Disposition"
    ] = f"attachment; file_name={file_name.value}.csv"
    return response


@router.get("/projects/{project_id}/raw/data/view")
def view_raw_file(
    project_id: str,
    file_name: constants.RawFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    df = helper.read_raw_file(
        tenant_name=current_user.tenant.company_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    if file_name == constants.RawFiles.existing_loans:
        df = df.head(50)

    return Response(
        content=df.to_json(orient="table"),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.json"',
            "Content-Type": "application/json",
        },
    )


@router.get("/projects/{project_id}/raw/data/download")
def download_only_raw_file(
    project_id: str,
    file_name: constants.RawFiles,
    current_user: models.Users = Depends(get_current_active_user),
):
    df = helper.read_raw_file(
        tenant_name=current_user.tenant.company_name,
        project_id=project_id,
        boto3_session=constants.MY_SESSION,
        file_name=file_name,
    )

    if file_name == constants.RawFiles.existing_loans:
        df = df.head(50)

    return Response(
        content=df.to_csv(index=True),
        headers={
            "Content-Disposition": f'attachment; filename="{file_name.value}.csv"',
            "Content-Type": "application/octet-stream",
        },
    )


@router.get("/projects/{project_id}/raw/filenames")
def get_raw_filenames(
    project_id: str,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name
    raw_files: list = wr.s3.list_objects(
        f"s3://{tenant_name}/project_{project_id}/{constants.FileStage.raw.value}",
        boto3_session=constants.MY_SESSION,
    )

    raw_files = list(map(lambda x: x.split("/")[-1].split(".")[0], raw_files))

    return raw_files


@router.get("/projects")
def get_projects(
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    projects = crud.get_projects_by_tenant_id(db=db, tenant_id=current_user.tenant_id)

    return projects


@router.get("/projects/current_user")
async def get_projects_by_user_id(
    db: Session = Depends(get_db),
    current_user: models.Users = Depends(get_current_active_user),
):
    projects = crud.get_project_by_user_id(db=db, user_id=current_user.user_id)
    return projects


@router.get(
    "/projects/{project_id}",
)
def get_project_by_id(
    project_id: int,
    db: Session = Depends(get_db),
):
    db_project = crud.get_project_by_id(db, project_id=project_id)

    if db_project is None:
        raise HTTPException(
            detail="Project does not exist",
            status_code=status.HTTP_404_NOT_FOUND,
        )
    return db_project


@router.put("/projects/{project_id}")
def update_project_by_id(
    project_id: str,
    edit_project: schemas.ProjectUpdate,
    db: Session = Depends(get_db),
):
    return crud.update_project_by_id(
        project_id=project_id, edit_project=edit_project, db=db
    )


@router.delete("/projects/{project_id}")
async def delete_project_by_id(
    project_id: str,
    db: Session = Depends(get_db),
):
    db_project = crud.get_project_by_id(db, project_id=project_id)
    if db_project is None:
        raise HTTPException(
            detail="Project does not exist",
            status_code=status.HTTP_404_NOT_FOUND,
        )

    return crud.delete_project_by_id(db=db, project_id=project_id)


@router.post("/projects/{project_id}/add-new-funding")
def add_new_funding(
    new_funding: schemas.NewFunding,
    funding_term: constants.FundingTerm,
    project_id: int,
    current_user: models.Users = Depends(get_current_active_user),
):
    tenant_name = current_user.tenant.company_name

    if funding_term == constants.FundingTerm.long_term:
        details_of_long_term_borrowing = helper.read_raw_file(
            tenant_name=tenant_name,
            project_id=project_id,
            boto3_session=constants.MY_SESSION,
            file_name=constants.RawFiles.details_of_long_term_borrowing,
            set_index=False,
        )

        details_of_long_term_borrowing = helper.columns_to_snake_case(
            df=details_of_long_term_borrowing
        )

        new_funding = pd.DataFrame(
            new_funding.dict(), index=[details_of_long_term_borrowing.index[-1] + 1]
        )

        details_of_long_term_borrowing = pd.concat(
            [details_of_long_term_borrowing, new_funding]
        )

        helper.upload_file(
            tenant_name=tenant_name,
            project_id=project_id,
            boto3_session=constants.MY_SESSION,
            file=details_of_long_term_borrowing,
            file_name=constants.RawFiles.details_of_long_term_borrowing,
            file_stage=constants.FileStage.raw,
        )

    if funding_term == constants.FundingTerm.short_term:
        details_of_short_term_borrowing = helper.read_raw_file(
            tenant_name=tenant_name,
            project_id=project_id,
            boto3_session=constants.MY_SESSION,
            file_name=constants.RawFiles.details_of_short_term_borrowing,
            set_index=False,
        )

        details_of_short_term_borrowing = helper.columns_to_snake_case(
            df=details_of_short_term_borrowing
        )

        new_funding = pd.DataFrame(
            new_funding.dict(), index=[details_of_short_term_borrowing.index[-1] + 1]
        )

        details_of_short_term_borrowing = pd.concat(
            [details_of_short_term_borrowing, new_funding]
        )

        helper.upload_file(
            tenant_name=tenant_name,
            project_id=project_id,
            boto3_session=constants.MY_SESSION,
            file=details_of_short_term_borrowing,
            file_name=constants.RawFiles.details_of_short_term_borrowing,
            file_stage=constants.FileStage.raw,
        )

    return {"message": "done"}
