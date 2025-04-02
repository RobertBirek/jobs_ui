import streamlit as st
import boto3
import json
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Konfiguracja DigitalOcean Spaces
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("BUCKET_NAME")

s3_client = boto3.client('s3', endpoint_url=ENDPOINT_URL)


st.title("Przeglądarka plików w S3")
st.markdown("""
Przeglądarka plików ofert zapisanych w S3.
Pliki zapisywane są na podtawie daty publikacji oferty, pola `publishedAt`. 

Wybierz datę, a następnie plik, aby zobaczyć zawartość.""")

prefix_filtr = st.radio("Wybierz dane z:", ["Roku", "Miesiąca", "Dnia"])
# Dodaj kalendarz na górze strony - użytkownik wybiera datę
selected_date = st.date_input("Wybierz datę:", datetime.date.today())

# Tworzenie prefixu na podstawie wybranej daty (format: jobs/rok/miesiąc/dzień/)
if prefix_filtr == "Roku":
    prefix = f"jobs/year={selected_date.year}/"
elif prefix_filtr == "Miesiąca":
    prefix = f"jobs/year={selected_date.year}/month={selected_date.month:02d}/"
else:
    prefix = f"jobs/year={selected_date.year}/month={selected_date.month:02d}/day={selected_date.day:02d}/"

    # key = f"jobs/year={year}/month={month}/day={day}/{output_filename}"


st.write(f"Wybrany prefix: {prefix}")

# Pobieramy listę obiektów z danego katalogu
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

if "Contents" in response:
    # Filtrujemy pliki z rozszerzeniami .json, .jsonl oraz .log
    file_list = [
        obj['Key'] 
        for obj in response["Contents"] 
        if obj['Key'].endswith(".json") or obj['Key'].endswith(".jsonl") or obj['Key'].endswith(".log")
    ]
    
    if file_list:
        selected_file = st.selectbox("Wybierz plik:", file_list)
        
        if st.button("Pokaż zawartość"):
            # Pobieramy zawartość wybranego pliku
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=selected_file)
            content = obj['Body'].read().decode("utf-8")
            
            if selected_file.endswith(".json"):
                try:
                    data = json.loads(content)
                    st.json(data)
                except json.JSONDecodeError:
                    st.error("Błąd dekodowania JSON")
            elif selected_file.endswith(".jsonl"):
                data = []
                for line in content.splitlines():
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        data.append({"raw": line})
                st.json(data,)
            elif selected_file.endswith(".log"):
                # Dla plików log wyświetlamy zawartość jako sformatowany tekst
                st.code(content, language="text")
    else:
        st.write("Brak plików JSON, JSONL lub LOG w wybranym katalogu.")
else:
    st.write("Brak plików w wybranym katalogu.")
