FROM python:3.11-slim

# Set the working directory

WORKDIR /app
COPY . /app

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port
EXPOSE 80

# Run the application
RUN pip install --force-reinstall 'requests<2.29.0' 'urllib3<2.0'

CMD ["python", "main.py"]

