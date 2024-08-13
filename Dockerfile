FROM python:3.11

# Set the working directory

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port
EXPOSE 80


CMD ["python", "main.py"]

