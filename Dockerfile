FROM python:3.8-slim-buster

ENV ACCEPT_EULA=Y
RUN apt-get update -y && apt-get update 

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .

ENV FLASK_RUN_HOST=0.0.0.0

CMD ["python3", "-m", "flask", "run"]
