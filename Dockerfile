# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Copy the requirements.txt file
COPY requirements.txt .

# Install any dependencies your consumer script might need
RUN pip install --no-cache-dir -r requirements.txt



# Make port 80 available to the world outside this container
# (Update this if your consumer script uses a different port)
EXPOSE 8080

# Run your script when the container launches
CMD ["python", "logistics_data_consumer.py"]
