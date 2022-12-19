FROM python:3.8

# Install pytest and other required packages
COPY requirements.txt .
RUN pip install -r requirements.txt

# Set the working directory
WORKDIR /app

# Copy the code and tests to the container
COPY . .

# Run the tests
CMD ["pytest app/test/test_snowflake_scd.py"]

#docker build -t test-image .
