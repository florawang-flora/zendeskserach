FROM python:3.11-slim
WORKDIR /app
RUN pip install pandas pymssql
COPY main.py .
CMD ["python", "load_to_sql.py"]