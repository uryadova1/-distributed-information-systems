# 1. Запрос на взлом хэша (для "abcd" = e2fc714c4727ee9395f324cd2e7f331f)
curl -X POST http://localhost:8000/api/hash/crack \
  -H "Content-Type: application/json" \
  -d '{
    "hash": "e2fc714c4727ee9395f324cd2e7f331f",
    "maxLength": 4,
    "algorithm": "MD5",
    "alphabet": "abcdefghijklmnopqrstuvwxyz"
  }'

# Ответ:
# {"requestId":"730a04e6-4de9-41f9-9d5b-53b88b17afac","estimatedCombinations":475254}

# 2. Проверка статуса
curl "http://localhost:8000/api/hash/status?requestId=730a04e6-4de9-41f9-9d5b-53b88b17afac"

# Готовый ответ:
# {"status":"READY","data":["abc"],"error":null}

# 3. Метрики
curl http://localhost:8000/api/metrics

# 4. Отмена запроса
curl -X DELETE "http://localhost:8000/api/hash/crack?requestId=730a04e6-4de9-41f9-9d5b-53b88b17afac"