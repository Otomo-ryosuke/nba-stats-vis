FROM python:3.12-slim

WORKDIR /usr/src/app

COPY . /usr/src/app
RUN pip install poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root
