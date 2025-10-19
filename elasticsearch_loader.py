from elasticsearch import Elasticsearch, helpers
from data_processor import process_covid_data
import config
import warnings
import time

warnings.filterwarnings('ignore')

def create_index_mapping(es):
    mapping = {
        "mappings": {
            "properties": {
                "Province/State": {"type": "keyword"},
                "Country/Region": {"type": "keyword"},
                "Date": {"type": "date"},
                "Confirmed": {"type": "integer"},
                "location": {"type": "geo_point"}
            }
        }
    }
    
    if es.indices.exists(index=config.INDEX_NAME):
        print(f"⚠️  Index '{config.INDEX_NAME}' already exists, deleting...")
        es.indices.delete(index=config.INDEX_NAME)
    
    es.indices.create(index=config.INDEX_NAME, body=mapping)
    print(f"✅ Index '{config.INDEX_NAME}' created")

def upload_to_elasticsearch():
    try:
        es = Elasticsearch(hosts=[config.ELASTIC_HOST])
        
        print("🔄 Connecting to Elasticsearch...")
        info = es.info()
        print(f"✅ Connected to cluster: {info['cluster_name']} (v{info['version']['number']})")
        
        create_index_mapping(es)
        
        print("📥 Processing data...")
        data = process_covid_data()
        
        if not data:
            print("⚠️  No data to upload")
            return False
        
        print(f"📦 {len(data)} records prepared")
        
        actions = [
            {
                "_index": config.INDEX_NAME,
                "_source": record
            }
            for record in data
        ]
        
        print("⬆️  Uploading to Elasticsearch...")
        success, failed = helpers.bulk(
            es, 
            actions, 
            raise_on_error=False,
            request_timeout=60,
            chunk_size=1000
        )
        
        print(f"✅ {success} records uploaded successfully")
        if failed:
            print(f"⚠️  {failed} records failed")
        
        es.indices.refresh(index=config.INDEX_NAME)
        time.sleep(1)
        
        count = es.count(index=config.INDEX_NAME)
        print(f"📊 Final record count: {count['count']}")
        
        if count['count'] == len(data):
            print("🎉 All records verified!")
        else:
            print(f"⚠️  Warning: Expected {len(data)} but got {count['count']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    upload_to_elasticsearch()