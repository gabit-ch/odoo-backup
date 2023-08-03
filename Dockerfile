FROM python:3.11-alpine

WORKDIR /usr/app/src

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir "backups"

COPY backup.py .

CMD [ "python", "backup.py"]