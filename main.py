from typing import List, Union

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from sqlalchemy.orm import Session

from application.auth.jwt_bearer import JwtBearer
from application.aws_helper.helper import MY_SESSION, S3_CLIENT, SNS_CLIENT
from application.modeling import helper
from application.routes import final_calculations, intermediate_calculations
from application.routes.projects import assumptions, projects_router
from application.routes.tenants import tenants_router
from application.routes.users import users_router
from application.utils import crud, database, models, schemas
from application.utils.database import engine, get_db

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
handler = Mangum(app)


@app.get("/")
def root():
    return {"message": "deployment to dev using pipeline"}


@app.get("/health")
def health():
    return {"status": "ok", "another": [1, 2, 3]}


database.Base.metadata.create_all(bind=engine)

origins = ["*"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "Hello": "World, l am CICD 2,the first one didnt work,pipeline failed on the second one but the code worked"
    }


@app.post("/user/login")
def login(user: schemas.UserLoginSchema, db: Session = Depends(get_db)):
    user = crud.check_user(db=db, user=user)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    return user


# @app.post("/{project_id}/upload-files")
# def upload_files(
#     project_id: int, files: List[UploadFile] = File(...), current_user: dict = Depends(JwtBearer()), db: Session = Depends(get_db)
# ):

#     user_id = current_user['user_id']
#     email = current_user['email']

#     user = db.query(models.Users).filter(
#         (models.Users.user_id == user_id) & (models.Users.email == email)).first()

#     tenant = db.query(models.Tenant).filter(
#         models.Tenant.tenant_id == user.tenant_id).first()
#     tenant_name = tenant.company_name
#     return helper.upload_multiple_files(
#         project_id=project_id,
#         tenant_name=tenant_name,
#         my_session=MY_SESSION,
#         files=files,
#     )

app.include_router(tenants_router.router)
app.include_router(users_router.router)
app.include_router(projects_router.router)
app.include_router(intermediate_calculations.router)
app.include_router(final_calculations.router)
# app.include_router(assumptions.router)
