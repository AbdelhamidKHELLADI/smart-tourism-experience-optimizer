
FROM python:3.11-slim AS build

RUN apt-get update && apt-get install -y \
    build-essential \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN pip install --no-cache-dir awslambdaric --target /build/python

COPY src/lambda_package/requirements.txt .

RUN pip install --no-cache-dir \
    -r requirements.txt \
    --target /build/python


FROM python:3.11-slim

WORKDIR /function

COPY --from=build /build/python /function/

COPY src/lambda_package /function/
COPY src/utils/preprocess_utils.py /function/utils
COPY src/utils/s3_utils.py /function/utils


ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "handler.lambda_handler" ]
