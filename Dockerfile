FROM python:3.12-slim

RUN pip install --no-cache-dir --upgrade pip

WORKDIR /airbyte/integration_code

COPY setup.py .
COPY main.py .
COPY source_google_sheets_xlsx/ ./source_google_sheets_xlsx/

RUN pip install --no-cache-dir .

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.name=airbyte/source-google-sheets-xlsx
LABEL io.airbyte.version=1.0.0
