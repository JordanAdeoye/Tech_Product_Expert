FROM python:3.11.14-slim
WORKDIR /tech_product_expert

RUN apt-get update && apt-get install -y curl && apt-get clean && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
COPY app.py rag_indexing_pipeline.py rag_retrieval.py   ./
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
