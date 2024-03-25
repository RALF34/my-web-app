FROM python:3.11-slim

WORKDIR /app

COPY requirements.txtx .
RUN pip install -r requirements.txt

COPY ..

EXPOSE 8501

CMD streamlit run daily_air_quality.py
