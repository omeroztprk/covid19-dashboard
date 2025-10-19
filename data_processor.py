import pandas as pd
import os
import config

def process_covid_data(file_path=None):
    try:
        if file_path is None:
            file_path = os.path.join(config.DATA_DIR, config.CSV_FILE_NAME)
        
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            print("💡 Run 'python3 data_fetcher.py' first")
            return []
        
        print(f"📂 Reading file: {file_path}")
        df = pd.read_csv(file_path)
        
        df_melted = df.melt(
            id_vars=["Province/State", "Country/Region", "Lat", "Long"],
            var_name="Date",
            value_name="Confirmed"
        )
        
        df_melted = df_melted[pd.notnull(df_melted["Lat"]) & pd.notnull(df_melted["Long"])]
        
        df_melted["Date"] = pd.to_datetime(df_melted["Date"], format="%m/%d/%y", errors="coerce")
        df_melted = df_melted[df_melted["Date"].notna()]
        df_melted["Date"] = df_melted["Date"].dt.strftime("%Y-%m-%d")
        
        df_melted["Province/State"].fillna("Unknown", inplace=True)
        
        df_melted["location"] = df_melted.apply(
            lambda row: {"lat": row["Lat"], "lon": row["Long"]}, axis=1
        )
        
        print(f"✅ {len(df_melted)} records processed")
        return df_melted.to_dict(orient="records")
        
    except Exception as e:
        print(f"❌ Data processing error: {e}")
        return []

if __name__ == "__main__":
    data = process_covid_data()
    print(f"📋 Sample record: {data[0] if data else 'No data'}")