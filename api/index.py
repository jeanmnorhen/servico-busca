import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaException

app = Flask(__name__)
CORS(app)

# --- Elasticsearch Configuration (MODIFICADO) ---
es = None
try:
    # Prioriza a conexão via URL do Host (ELASTIC_HOST)
    es_host_url = os.environ.get('ELASTIC_HOST')
    es_api_key = os.environ.get('ELASTIC_API_KEY')
    es_cloud_id = os.environ.get('ELASTIC_CLOUD_ID')

    if es_host_url and es_api_key:
        es = Elasticsearch(
            hosts=[es_host_url],
            api_key=es_api_key
        )
        print("Elasticsearch inicializado com sucesso via ELASTIC_HOST.")
    elif es_cloud_id and es_api_key:
        # Mantém o método original como fallback
        es = Elasticsearch(
            cloud_id=es_cloud_id,
            api_key=es_api_key
        )
        print("Elasticsearch inicializado com sucesso via ELASTIC_CLOUD_ID.")
    else:
        print("Variáveis de ambiente para conexão cloud com Elasticsearch não encontradas.")

except Exception as e:
    print(f"Erro ao inicializar Elasticsearch: {e}")


# --- Kafka Consumer Configuration ---
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
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["name^3", "description", "category", "email", "address"]
                }
            }
        }
        resp = es.search(index="users,products,stores,offers", body=search_body)
        
        hits = []
        for hit in resp['hits']['hits']:
            source = hit['_source']
            source['id'] = hit['_id']
            source['type'] = hit['_index']
            hits.append(source)

        return jsonify({"results": hits}), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao realizar busca: {e}"}), 500

# --- Health Check ---
@app.route('/api/health', methods=['GET'])
def health_check():
    es_status = "error"
    if es:
        try:
            if es.ping():
                es_status = "ok"
        except Exception:
            pass
            
    status = {
        "elasticsearch": es_status,
        "kafka_consumer": "ok" if consumer else "error"
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)