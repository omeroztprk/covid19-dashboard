import requests
import config
import os

def download_covid_data():
    try:
        os.makedirs(config.DATA_DIR, exist_ok=True)
        file_path = os.path.join(config.DATA_DIR, config.CSV_FILE_NAME)
        
        print(f"📥 Downloading data from {config.CSV_URL}")
        response = requests.get(config.CSV_URL, timeout=30)
        response.raise_for_status()
        
        with open(file_path, "w", encoding='utf-8') as f:
            f.write(response.text)
        
        print(f"✅ Data downloaded successfully: {file_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to download data: {e}")
        return False

if __name__ == "__main__":
    download_covid_data()