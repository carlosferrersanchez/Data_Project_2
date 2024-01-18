FROM python:3
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
COPY DP1_Creacion_BBDD.py .
COPY DP2_Creacion_de_clases.py .
COPY DP3_Ingestion_basica.py .
COPY ZZ_Auxiliar.py .
ENTRYPOINT ["python", "./main.py"]