import os

from databases import Database
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

SQLALCHEMY_DATABASE_URL = (
    "postgresql://{username}:{password}@{host}:{port}/{database}".format(
        username=os.getenv("RDS_USERNAME"),
        password=os.getenv("RDS_PASSWORD"),
        host=os.getenv("RDS_HOSTNAME"),
        port=os.getenv("RDS_PORT"),
        database=os.getenv("RDS_DB_NAME"),
    )
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
