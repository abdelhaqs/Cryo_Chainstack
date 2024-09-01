# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt file into the container's working directory
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .