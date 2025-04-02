import streamlit as st
import os
import json
import boto3
import datetime
import logging
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

LOCAL_DATA_FOLDER = Path("data/")
os.makedirs(LOCAL_DATA_FOLDER, exist_ok=True)

ENDPOINT_URL = os.getenv("ENDPOINT_URL")
BUCKET_NAME = os.getenv("BUCKET_NAME")
LOG_FILE = "jobs.log"

s3_client = boto3.client('s3', endpoint_url=ENDPOINT_URL)

st.set_page_config(page_title="Jobs",page_icon=":male-factory-worker:")

########################################################
# Reset existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configure logging to write to app.log
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG if needed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
########################################################
def save_offers_to_s3(offers, bucket_name, s3_client):
    # Grupujemy oferty według daty publikacji
    offers_by_date = {}
    total_offers = len(offers)
    duplicate_count = 0

    for offer in offers:
        published_at_str = offer.get("publishedAt")
        slug = offer.get("slug")

        if not published_at_str or not slug:
            logging.error(f"Niepoprawna oferta: {offer}")
            continue
        
        try:
            # Konwersja daty ze standardu ISO
            published_date = datetime.datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).date()
            date_str = published_date.isoformat()  # Format: YYYY-MM-DD
        except Exception as e:
            logging.error(f"Błąd przetwarzania daty dla oferty: {offer}: {e}")
            continue
        
        offers_by_date.setdefault(date_str, []).append(offer)

    # Przetwarzamy oferty dla każdej daty osobno
    for date_str, offers_list in offers_by_date.items():
        year, month, day = date_str.split('-')
        # Ustalanie klucza na S3: np. jobs/2025/03/05/justjoinit.jsonl
        output_filename = f"justjoinit_{date_str}.jsonl"
        # key = f"jobs/{year}/{month}/{day}/{output_filename}"
        key = f"jobs/year={year}/month={month}/day={day}/{output_filename}"
        seen_slugs = set()
        existing_content = ""
        
        # Próba pobrania istniejącego obiektu z S3
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            existing_content = response['Body'].read().decode('utf-8')
            # Parsowanie istniejących ofert, aby wyłapać duplikaty
            for line in existing_content.splitlines():
                try:
                    existing_offer = json.loads(line)
                    existing_slug = existing_offer.get("slug")
                    if existing_slug:
                        seen_slugs.add(existing_slug)
                except Exception as e:
                    logging.error(f"Błąd przy wczytywaniu oferty z S3 {key}: {e}")
        except s3_client.exceptions.NoSuchKey:
            logging.info(f"Obiekt {key} nie istnieje. Zostanie utworzony nowy.")
        except Exception as e:
            logging.error(f"Błąd przy pobieraniu obiektu {key} z S3: {e}")

        new_lines = []
        for offer in offers_list:
            slug = offer.get("slug")
            if slug in seen_slugs:
                logging.info(f"Duplikat oferty '{slug}' dla daty {date_str} - pomijam.")
                duplicate_count += 1 # Zliczanie duplikatów
                continue
            seen_slugs.add(slug)
            new_lines.append(json.dumps(offer, ensure_ascii=False))

        if new_lines:
            if existing_content and not existing_content.endswith("\n"):
                existing_content += "\n"
            updated_content = existing_content + "\n".join(new_lines) + "\n"
            try:
                s3_client.put_object(Bucket=bucket_name, Key=key, Body=updated_content.encode('utf-8'))
                logging.info(f"Zapisano {len(new_lines)} nowych ofert do obiektu S3: {key}.")
            except Exception as e:
                logging.error(f"Błąd przy zapisywaniu obiektu {key} do S3: {e}")
        else:
            logging.info(f"Wszystkie oferty dla daty {date_str} są duplikatami.")

    return True, total_offers, duplicate_count 

########################################################

st.title("Upload plików do bazy JobsOffers")
st.markdown("""
Wgraj pliki ofert w formacie JSON lub JSON Lines. Program odczyta zawartość plików, 
przydzieli oferty do dat na podstawie pola `publishedAt`, a następnie uzupełni bazę S3, 
dodając tylko te oferty, które nie są jeszcze zapisane.
""")

# Umożliwiamy wgranie wielu plików
uploaded_files = st.file_uploader("Wybierz pliki ofert", type=["json","jsonl"], accept_multiple_files=True)

if uploaded_files:
    for uploaded_file in uploaded_files:
        file_path = LOCAL_DATA_FOLDER / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.read())
        st.success(f"Plik {uploaded_file.name} został zapisany w {LOCAL_DATA_FOLDER}")

st.markdown("### Pliki zapisane w lokalnym folderze:")
for file in LOCAL_DATA_FOLDER.iterdir():
    st.write(file.name)



if st.button("Aktualizuj S3",type="primary", use_container_width=True):
    # Odczytujemy wszystkie pliki z katalogu LOCAL_DATA_FOLDER
    total_offers = 0
    total_duplicates = 0
    for file_path in LOCAL_DATA_FOLDER.glob("*"):
        if file_path.suffix in [".json", ".jsonl"]:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            offers = []
            if file_path.suffix == ".json":
                try:
                    offers = json.loads(content)
                    if isinstance(offers, dict):
                        offers = [offers]
                except json.JSONDecodeError:
                    st.error(f"Błąd dekodowania JSON w pliku {file_path.name}")
                    continue
            elif file_path.suffix == ".jsonl":
                for line in content.splitlines():
                    try:
                        offers.append(json.loads(line))
                    except json.JSONDecodeError:
                        st.error(f"Błąd dekodowania linii w pliku {file_path.name}")
                        continue
            # Dla każdej oferty wywołujemy funkcję save_offers_s3_by_date
            # for offer in offers:
            saved, total, duplicates = save_offers_to_s3(offers, BUCKET_NAME, s3_client)
            total_offers += total
            total_duplicates += duplicates
            logging.info(f"W pliku: {file_path} Wszystkich ofert: {total} Duplikatów: {duplicates}")
        
        # Usuwanie pliku po przetworzeniu
        os.remove(file_path)

    logging.info(f"Wszystkich ofert: {total_offers} Duplikatów: {total_duplicates}")
    st.success(f"Przetwarzanie wszystkich ofert zakończone.ofert: {total_offers} Duplikatów: {total_duplicates}")