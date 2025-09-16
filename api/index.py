import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaException

app = Flask(__name__)

# --- Elasticsearch Configuration ---
es = None
try:
    es_cloud_id = os.environ.get('ELASTIC_CLOUD_ID')
    es_api_key = os.environ.get('ELASTIC_API_KEY')
    es_host = os.environ.get('ELASTIC_HOST', 'localhost')
    es_port = os.environ.get('ELASTIC_PORT', '9200')

    if es_cloud_id and es_api_key:
        es = Elasticsearch(
            cloud_id=es_cloud_id,
            api_key=es_api_key
        )
    else:
        # Fallback for local development
        es = Elasticsearch(
            hosts=[f"http://{es_host}:{es_port}"],
            # Disable TLS verification for local development if needed
            verify_certs=False
        )
    print("Elasticsearch inicializado com sucesso.")
except Exception as e:
    print(f"Erro ao inicializar Elasticsearch: {e}")

# --- Kafka Consumer Configuration (for indexing events) ---
consumer = None
try:
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': 'search_service_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    if kafka_conf['bootstrap.servers']:
        consumer = Consumer(kafka_conf)
        # Subscribe to relevant topics
        consumer.subscribe(['eventos_usuarios', 'eventos_produtos', 'eventos_lojas', 'eventos_ofertas'])
        print("Consumidor Kafka inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do Kafka não encontradas para o consumidor.")
except Exception as e:
    print(f"Erro ao inicializar Consumidor Kafka: {e}")

# --- API Routes ---

@app.route('/api/search', methods=['GET'])
def search():
    if not es:
        return jsonify({"error": "Elasticsearch não está inicializado."}), 503

    query = request.args.get('q', '')
    if not query:
        return jsonify({"error": "Parâmetro 'q' (query) é obrigatório."}), 400

    try:
        # Basic search across multiple indices/fields
        # In a real application, this would be more sophisticated
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["name^3", "description", "category", "email", "address"]
                }
            }
        }
        # Search across all relevant indices
        resp = es.search(index="users,products,stores,offers", body=search_body)
        
        hits = []
        for hit in resp['hits']['hits']:
            source = hit['_source']
            source['id'] = hit['_id']
            source['type'] = hit['_index'] # Add type to identify source
            hits.append(source)

        return jsonify({"results": hits}), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao realizar busca: {e}"}), 500

# --- Health Check (para Vercel) ---
@app.route('/api/health', methods=['GET'])
def health_check():
    status = {
        "elasticsearch": "ok" if es and es.ping() else "error",
        "kafka_consumer": "ok" if consumer else "error" # More robust check needed for Kafka
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    # For local development, you might want to run a separate thread for Kafka consumption
    # For Vercel, this will be handled by the serverless function invocation model
    app.run(debug=True)
