import os
import subprocess
import shutil

# Set up paths
output_folder = "/Users/p/Desktop/lambda_openai_dependencies"
zip_file_path = "/Users/p/Desktop/lambda_openai_dependencies.zip"
dockerfile_path = "./Dockerfile"

# Dockerfile content for Ubuntu with Python 3.12 on x86_64
dockerfile_content = """
# Use Ubuntu as the base image
FROM ubuntu:latest

# Update and install Python 3.12, pip, and other tools
RUN apt update && \
    apt install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt update && \
    apt install -y python3.12 python3.12-venv python3-pip zip && \
    apt clean

# Set Python 3.12 as the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# Create working directory
WORKDIR /app

# Install dependencies in the /app/python directory
RUN python3 -m pip install requests pymongo openai -t /app/python

# Zip dependencies
RUN cd /app && zip -r lambda_openai_dependencies.zip python
"""

# Step 1: Write Dockerfile
with open(dockerfile_path, "w") as f:
    f.write(dockerfile_content)

# Step 2: Build Docker Image
docker_image_name = "lambda_openai_deps"
print("Building Docker image...")
subprocess.run(["docker", "build", "-t", docker_image_name, "."], check=True)

# Step 3: Run Docker Container and Copy Zip File
print("Running Docker container to generate zip file...")
subprocess.run([
    "docker", "run", "--rm", "-v", f"{os.getcwd()}:/output", docker_image_name,
    "cp", "/app/lambda_openai_dependencies.zip", "/output"
], check=True)

# Step 4: Move the zip file to the desired output path
if os.path.exists("lambda_openai_dependencies.zip"):
    shutil.move("lambda_openai_dependencies.zip", zip_file_path)
    print(f"Dependencies have been zipped and saved to {zip_file_path}")
else:
    print("Zip file generation failed.")

# Step 5: Clean up Dockerfile and temporary files
os.remove(dockerfile_path)
