import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from confluent_kafka import Consumer, KafkaException

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
                for key, value in doc_data.items():
                    if hasattr(value, 'isoformat'):
                        doc_data[key] = value.isoformat()
                es.index(index=collection_name, id=doc.id, document=doc_data)
                count += 1
            stats[collection_name] = {"indexed_documents": count}
        except Exception as e:
            stats[collection_name] = {"error": str(e)}
    return jsonify({"status": "Reindexação concluída", "details": stats}), 200

@app.route('/api/search/consume', methods=['POST'])
def consume_events():
    # 1. Segurança: Protege o endpoint com um token secreto
    auth_header = request.headers.get('Authorization')
    cron_secret = os.environ.get('CRON_SECRET')
    if not cron_secret or auth_header != f'Bearer {cron_secret}':
        return jsonify({"error": "Unauthorized"}), 401

    # 2. Configuração do Consumidor Kafka
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'group.id': 'search_service_group_cron_v2', # Novo group.id para garantir que ele leia eventos não lidos
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    try:
        consumer = Consumer(kafka_conf)
        consumer.subscribe(['eventos_usuarios', 'eventos_produtos', 'eventos_lojas', 'eventos_ofertas'])
    except Exception as e:
        return jsonify({"error": f"Falha ao inicializar consumidor Kafka: {e}"}), 500

    # 3. Lógica de Consumo
    messages_processed = 0
    try:
        msgs = consumer.consume(num_messages=20, timeout=5.0) # Consome até 20 mensagens ou por 5 segundos
        if not msgs:
            return jsonify({"status": "No new messages to process"}), 200

        for msg in msgs:
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            
            event_data = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            index_name = topic.split('_')[1]
            
            # Extrai o ID e os dados do evento
            doc_id = event_data.get(f"{index_name[:-1]}_id") # ex: user_id, product_id
            data_to_index = event_data.get('data', {})

            if doc_id and data_to_index:
                es.index(index=index_name, id=doc_id, document=data_to_index)
                messages_processed += 1

    except Exception as e:
        return jsonify({"error": f"Erro durante o consumo de eventos: {e}"}), 500
    finally:
        consumer.close()

    return jsonify({"status": "ok", "messages_processed": messages_processed}), 200

# --- Health Check ---
@app.route('/api/health', methods=['GET'])
def health_check():
    es_status = "error"
    if es and es.ping():
        es_status = "ok"
    status = {"elasticsearch": es_status}
    http_status = 200 if es_status == "ok" else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)