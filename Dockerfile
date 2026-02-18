FROM python:3-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY evict_ovn_router.py .

RUN chmod +x evict_ovn_router.py

ENTRYPOINT ["python", "evict_ovn_router.py"]
