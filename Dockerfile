FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY application ${LAMBDA_TASK_ROOT}/application
COPY main.py ${LAMBDA_TASK_ROOT}

RUN ls

CMD [ "main.handler" ]