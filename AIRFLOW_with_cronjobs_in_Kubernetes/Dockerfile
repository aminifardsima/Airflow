# Use a base Python image with the version you need
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy your Python script (code.py) into the container
COPY code.py /app/

# Optionally, if your Python script has dependencies, create a requirements.txt file and add it here
# COPY requirements.txt /app/
# RUN pip install -r requirements.txt

# Install required dependencies, such as Airflow, MySQL libraries, and any other libraries in your script
RUN pip install pandas sqlalchemy pymysql apache-airflow

# Set the command to run your Python script when the container starts
CMD ["python", "code.py"]
