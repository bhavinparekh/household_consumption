FROM apache/airflow:latest

# Set the working directory in the container
WORKDIR /opt/airflow

# Copy the requirements.txt file to the container
COPY . .

# Install Python dependencies from requirements.txt
RUN pip install -r requirements.txt

# Copy the rest of the files from the local directory to the container
