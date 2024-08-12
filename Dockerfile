FROM python:3.8-slim

# Set the working directory

WORKDIR /app
COPY . /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port
EXPOSE 80


CMD ["python", "main.py"]

