# NestJS + Kafka (Docker) — Orders (Event-Driven)

## Objectif
Mini projet démonstration d’un **système de gestion de commandes** en architecture **event-driven**.

### Flow
1. L’API reçoit une commande (`POST /orders`)
2. Elle "sauvegarde" la commande (actuellement **en mémoire**, pour la démo)
3. Elle publie un event Kafka `OrderCreated` (topic `orders.created`)
4. Un consumer traite l’event (facturation + notification — stubs)

👉 Découplé, scalable, robuste (au sens **at-least-once** côté Kafka, avec idempotence côté consumer).

---

## Démarrage rapide (Docker)

```bash
docker compose up --build
```

- API: http://localhost:3000
- Kafka depuis ta machine: `localhost:29092`
- Kafka depuis les containers: `kafka:9092`

---

## Tester l’API

Créer une commande:

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"cust_123\",\"amount\":42}"
```

Tu dois voir dans les logs:
- publication de l’event (producer)
- consommation + traitement (consumer)

---

## Variables d’environnement

- `KAFKA_BROKERS` (default: `kafka:9092`) — liste CSV possible
- `KAFKA_CONSUMER_GROUP_ID` (default: `orders-group`)
- `ORDERS_CREATED_TOPIC` (default: `orders.created`)
- `IDEMPOTENCE_RETENTION_MS` (default: 3600000)

Pour lancer l’API **hors Docker** (`npm run start:dev`) tout en utilisant Kafka docker:
- mets `KAFKA_BROKERS=localhost:29092`

---

## Points de revue (ce qui est bien / améliorations)

### ✅ Bien
- **Découplage** API vs traitements (consumer) via event `OrderCreated`
- **Validation DTO** côté API (`ValidationPipe` + `class-validator`)
- Consumer avec **try/catch + validation du payload**
- **Idempotence** côté consumer (démo) + nettoyage TTL

### ⚠️ Limites actuelles (normal pour une démo)
- Pas de vraie DB: stockage en mémoire (à remplacer par Postgres/Mongo)
- Idempotence en mémoire: en prod on préfère
  - contrainte unique en DB (inbox table)
  - ou Redis
- Pas de pattern **outbox**: en prod on évite le risque “DB OK mais event non publié”
- Pas de DLQ / retry policy avancée / observabilité (metrics/tracing)

---

## Dev (local)

```bash
npm install
npm run start:dev
```

Build:

```bash
npm run build
```
