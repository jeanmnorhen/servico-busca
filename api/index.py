import os
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from confluent_kafka  import Consumer, KafkaException

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    firebase_admin = None

app = Flask(__name__)
CORS(app)

# --- Variáveis globais para erros de inicialização ---
firebase_init_error = None
es_init_error = None
kafka_consumer_init_error = None

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
            firebase_init_error = "Variável FIREBASE_ADMIN_SDK_BASE64 não encontrada para reindexação."
            print(firebase_init_error)
    except Exception as e:
        firebase_init_error = str(e)
        print(f"Erro ao inicializar Firebase para reindexação: {e}")
else:
    firebase_init_error = "Biblioteca firebase_admin não encontrada."

# --- Elasticsearch Configuration ---
es = None
try:
    es_host_url = os.environ.get('ELASTIC_HOST')
    es_api_key = os.environ.get('ELASTIC_API_KEY')
    if es_host_url and es_api_key:
        es = Elasticsearch(hosts=[es_host_url], api_key=es_api_key)
        print("Elasticsearch inicializado com sucesso via ELASTIC_HOST.")
    else:
        es_init_error = "Variáveis de ambiente para conexão cloud com Elasticsearch não encontradas."
        print(es_init_error)
except Exception as e:
    es_init_error = str(e)
    print(f"Erro ao inicializar Elasticsearch: {e}")

# --- Kafka Consumer Configuration ---
kafka_consumer_instance = None
if Consumer:
    try:
        kafka_conf = {
            'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
            'group.id': 'search_service_group_cron_v2', # Novo group.id para garantir que ele leia eventos não lidos
            'auto.offset.reset': 'earliest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.environ.get('KAFKA_API_KEY'),
            'sasl.password': os.environ.get('KAFKA_API_SECRET')
        }
        if kafka_conf['bootstrap.servers']:
            kafka_consumer_instance = Consumer(kafka_conf)
            kafka_consumer_instance.subscribe(['eventos_usuarios', 'eventos_produtos', 'eventos_lojas', 'eventos_ofertas'])
            print("Consumidor Kafka inicializado com sucesso.")
        else:
            kafka_consumer_init_error = "Variáveis de ambiente do Kafka não encontradas para o consumidor."
            print(kafka_consumer_init_error)
    except Exception as e:
        kafka_consumer_init_error = str(e)
        print(f"Erro ao inicializar Consumidor Kafka: {e}")
else:
    kafka_consumer_init_error = "Biblioteca confluent_kafka não encontrada."

# --- API Routes ---

@app.route('/api/search', methods=['GET', 'OPTIONS'])
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

@app.route('/api/search/consume', methods=['POST', 'GET'])
def consume_events():
    # 1. Segurança: Protege o endpoint com um token secreto
    auth_header = request.headers.get('Authorization')
    cron_secret = os.environ.get('CRON_SECRET')
    if not cron_secret or auth_header != f'Bearer {cron_secret}':
        return jsonify({"error": "Unauthorized"}), 401

    if not kafka_consumer_instance:
        return jsonify({"error": "Kafka consumer not initialized.", "details": kafka_consumer_init_error}), 503

    # 3. Lógica de Consumo
    messages_processed = 0
    try:
        msgs = kafka_consumer_instance.consume(num_messages=20, timeout=5.0) # Consome até 20 mensagens ou por 5 segundos
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
        # O consumidor não deve ser fechado aqui se for uma instância global
        # kafka_consumer_instance.close() # Removido
        pass

    return jsonify({"status": "ok", "messages_processed": messages_processed}), 200

def get_health_status():
    env_vars = {
        "FIREBASE_ADMIN_SDK_BASE64": "present" if os.environ.get('FIREBASE_ADMIN_SDK_BASE64') else "missing",
        "ELASTIC_HOST": "present" if os.environ.get('ELASTIC_HOST') else "missing",
        "ELASTIC_API_KEY": "present" if os.environ.get('ELASTIC_API_KEY') else "missing",
        "KAFKA_BOOTSTRAP_SERVER": "present" if os.environ.get('KAFKA_BOOTSTRAP_SERVER') else "missing",
        "KAFKA_API_KEY": "present" if os.environ.get('KAFKA_API_KEY') else "missing",
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing"
    }

    es_status = "error"
    if es:
        try:
            if es.ping():
                es_status = "ok"
            else:
                es_status = "error (ping failed)"
        except Exception as e:
            es_status = f"error ({e})"
    else:
        es_status = "error (not initialized)"

    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "firestore": "ok" if db else "error",
            "elasticsearch": es_status,
            "kafka_consumer": "ok" if kafka_consumer_instance else "error"
        },
        "initialization_errors": {
            "firestore": firebase_init_error,
            "elasticsearch": es_init_error,
            "kafka_consumer": kafka_consumer_init_error
        }
    }
    return status

@app.route('/api/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["firestore"] == "ok" and
        status["dependencies"]["elasticsearch"] == "ok" and
        status["dependencies"]["kafka_consumer"] == "ok"
    )
    http_status = 200 if all_ok else 503
    
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)