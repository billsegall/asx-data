FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api.py .
ENV FLASK_APP=api.py
ENV PYTHONPATH=/stockdb
EXPOSE 8082
CMD ["flask", "run", "--host=0.0.0.0", "--port=8082"]
