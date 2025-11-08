FROM python:3.13-slim

WORKDIR /app

RUN apt update && apt install git -y

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 9979

CMD ["hypercorn", "app:app", "--bind", "0.0.0.0:9979"]