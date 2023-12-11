# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /app

# Install Java Runtime Environment
RUN apt-get update && \
    apt-get install -y default-jdk

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Make port 5000 available to the world outside this container
EXPOSE 5000

# Define environment variable
ENV ZEPPELIN_URL https://10.206.26.42:9995
ENV USERZEP user50
ENV PASSWORD admin

# Run app.py when the container launches
CMD ["python", "app.py"]