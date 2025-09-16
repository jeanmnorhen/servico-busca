import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaException

# Importa o cliente Firestore do servico-produtos (ou de qualquer outro que o tenha)
# Em uma arquitetura mais madura, isso viria de uma camada de dados compartilhada.
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None

app = Flask(__name__)
CORS(app)

# --- Firebase Admin SDK Initialization ---
db = None
if firebase_admin:
    try:
        firebase_sdk_cred_base64 = os.environ.get('FIREBASE_ADMIN_SDK_BASE64')
        if firebase_sdk_cred_base64:
            import base64
            decoded_sdk = base64.b64decode(firebase_sdk_cred_base64).decode('utf-8')
            cred_dict = json.loads(decoded_sdk)
            cred = credentials.Certificate(cred_dict)
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            print("Firebase Admin SDK inicializado com sucesso.")
        else:
            print("Variável FIREBASE_ADMIN_SDK_BASE64 não encontrada para reindexação.")
    except Exception as e:
        print(f"Erro ao inicializar Firebase para reindexação: {e}")

# --- Elasticsearch Configuration ---
es = None
try:
    es_host_url = os.environ.get('ELASTIC_HOST')
    es_api_key = os.environ.get('ELASTIC_API_KEY')
    if es_host_url and es_api_key:
        es = Elasticsearch(hosts=[es_host_url], api_key=es_api_key)
        print("Elasticsearch inicializado com sucesso via ELASTIC_HOST.")
    else:
        print("Variáveis de ambiente para conexão cloud com Elasticsearch não encontradas.")
except Exception as e:
    print(f"Erro ao inicializar Elasticsearch: {e}")

# --- Kafka Consumer Configuration (será usado na Fase 2) ---
# ... (código do consumidor permanece o mesmo)

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
        resp = es.search(index="users,products,stores,offers", body=search_body, ignore_unavailable=True)
        hits = []
        for hit in resp['hits']['hits']:
            source = hit['_source']
            source['id'] = hit['_id']
            source['type'] = hit['_index']
            hits.append(source)
        return jsonify({"results": hits}), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao realizar busca: {e}"}), 500

@app.route('/api/search/reindex', methods=['POST', 'GET'])
def reindex():
    if not db or not es:
        return jsonify({"error": "Dependências (Firestore ou Elasticsearch) não inicializadas."}), 503

    collections_to_index = ["users", "stores", "products", "offers"]
    stats = {}

    for collection_name in collections_to_index:
        try:
            docs = db.collection(collection_name).stream()
            count = 0
            for doc in docs:
                doc_data = doc.to_dict()
                # Remove campos que não podem ser serializados para JSON
                for key, value in doc_data.items():
                    if hasattr(value, 'isoformat'): # Converte datetimes
                        doc_data[key] = value.isoformat()
                es.index(index=collection_name, id=doc.id, document=doc_data)
                count += 1
            stats[collection_name] = {"indexed_documents": count}
        except Exception as e:
            stats[collection_name] = {"error": str(e)}
    
    return jsonify({"status": "Reindexação concluída", "details": stats}), 200

# --- Health Check ---
@app.route('/api/health', methods=['GET'])
def health_check():
    es_status = "error"
    if es and es.ping():
        es_status = "ok"
            
    status = {
        "elasticsearch": es_status,
        # O consumidor não é uma dependência crítica para o health check da API
        # "kafka_consumer": "ok" if consumer else "error"
    }
    http_status = 200 if es_status == "ok" else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
