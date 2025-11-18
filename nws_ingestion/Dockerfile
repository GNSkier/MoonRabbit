# 1. Base Image: Start with a lightweight, official Python image
FROM python:3.11-slim

# 2. Set Working Directory: Create a folder inside the container for our code
WORKDIR /app

# 3. Copy & Install Dependencies: 
# We copy requirements.txt *first* to leverage Docker's layer caching.
# This step only re-runs if your requirements.txt file changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy Source Code:
# Copy your main script and the utilities file that has your station list
COPY main.py .
COPY utilities.py . 
# If you used stations.json, add: COPY stations.json .

# 5. Set Command:
# Tell the container what to run when it starts.
# This executes `python main.py`
CMD ["python", "main.py"]