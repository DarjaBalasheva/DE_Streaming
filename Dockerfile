FROM python:3.11

WORKDIR /app

# Установка зависимостей для компиляции пакетов
RUN apt-get update && apt-get install -y build-essential

#Установка pipenv
RUN pip install pipenv && pip install --upgrade pip

# Копирование Pipfile и Pipfile.lock
COPY Pipfile Pipfile.lock ./

# Установка зависимостей с помощью pipenv
RUN pipenv install --system --deploy

COPY . .
