FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
ENV PYTHONPATH=/stockdb
ENV FLASK_APP=asx.py
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV DATABASE=/stockdb/stockdb.db
EXPOSE 5000
CMD ["flask", "run", "--host=0.0.0.0"]
