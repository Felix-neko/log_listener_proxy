# Log Listener Proxy

Микросервис для проксирования stdout/stderr от PySpark-приложений к клиентам через WebSocket.

## Назначение

Этот микросервис решает проблему получения логов от PySpark-приложений, запущенных на удалённом Spark-кластере. Когда вы запускаете `spark-submit`, вывод stdout/stderr остаётся на удалённом драйвере. **Log Listener Proxy** позволяет получать эти логи в реальном времени на клиентской машине.

## Сценарий использования

1. **Клиентское приложение** создаёт сессию логирования через `POST /logging_session/{session_id}`
2. **Клиент** подключается к WebSocket-эндпоинтам `/logs/{session_id}/read/stdout` и `/logs/{session_id}/read/stderr`
3. **PySpark-приложение** на удалённом Spark-драйвере подключается к `/logs/{session_id}/write/stdout` и `/logs/{session_id}/write/stderr`
4. Данные от PySpark проксируются в реальном времени к клиенту
5. При закрытии write-соединения read-соединение закрывается через 5 секунд
6. Сессии автоматически удаляются по истечении времени жизни (по умолчанию 1 час)

## Установка

```bash
# Установка с помощью uv
uv sync

# Установка с dev-зависимостями (для тестов)
uv sync --all-extras
```

## Запуск сервера

```bash
# Запуск с настройками по умолчанию (0.0.0.0:8160, TTL=3600 секунд)
uv run python -m log_listener_proxy.server

# Или через uvicorn напрямую (без ServerSettings и порт-менеджера)
uv run uvicorn log_listener_proxy.server:app --host 0.0.0.0 --port 8160
```

### Конфигурация через ServerSettings

Все настройки сервера задаются через Pydantic-класс `ServerSettings`, который поддерживает
переменные среды с префиксом `LOG_LISTENER_`.

| Параметр           | Тип   | По умолчанию                                     | Переменная среды                 | Описание                                                         |
|--------------------|-------|--------------------------------------------------|----------------------------------|------------------------------------------------------------------|
| `host`             | str   | `"0.0.0.0"`                                      | `LOG_LISTENER_HOST`              | Хост для прослушивания                                           |
| `port`             | int   | `8160`                                            | `LOG_LISTENER_PORT`              | Порт для прослушивания (игнорируется при `use_port_manager=True`) |
| `ttl`              | int   | `3600`                                            | `LOG_LISTENER_TTL`               | Время жизни сессии в секундах (1 час по умолчанию)               |
| `use_port_manager` | bool  | `False`                                           | `LOG_LISTENER_USE_PORT_MANAGER`  | Запрашивать порт у порт-менеджера вместо фиксированного          |
| `port_manager_url` | str   | `"http://portmanager.airflow-fs.svc/manage"`      | `LOG_LISTENER_PORT_MANAGER_URL`  | URL порт-менеджера                                               |

**Примеры запуска:**

```bash
# Через переменные среды
LOG_LISTENER_PORT=9000 LOG_LISTENER_TTL=7200 uv run python -m log_listener_proxy.server

# С порт-менеджером в Kubernetes
LOG_LISTENER_USE_PORT_MANAGER=true uv run python -m log_listener_proxy.server
```

При запуске через `if __name__ == "__main__"` настройки читаются автоматически:

```python
settings = ServerSettings()
main(**settings.model_dump())
```

### Интеграция с порт-менеджером

Если `use_port_manager=True`, сервер:
1. Запрашивает свободный порт через `GET {port_manager_url}/v2?portcount=1&name={pod_name}`
2. Запускает uvicorn на полученном порту
3. При завершении освобождает порт через `DELETE {port_manager_url}/v2?portcount=1&name={pod_name}`

Имя пода берётся из переменной среды `POD_NAME` (Kubernetes downward API) или из `socket.gethostname()`.

## API-эндпоинты

### HTTP

| Метод  | Эндпоинт                        | Описание                    | Request Body | Response          |
|--------|----------------------------------|-----------------------------|--------------|-------------------|
| GET    | `/health`                        | Проверка работоспособности  | —            | `{"name": "...", "status": "ok"}` |
| POST   | `/logging_session/{session_id}`  | Создать сессию логирования  | —            | `SessionResponse` |
| DELETE | `/logging_session/{session_id}`  | Удалить сессию логирования  | —            | `SessionResponse` |

**SessionResponse** (Pydantic DTO):
```json
{
  "status": "ok",
  "session_id": "my-session-123",
  "message": "Сессия создана"
}
```

### WebSocket

| Эндпоинт | Описание | Направление данных |
|----------|----------|--------------------|
| `/logs/{session_id}/read/{output_type}` | Читать логи (для клиента) | Сервер → Клиент |
| `/logs/{session_id}/write/{output_type}` | Писать логи (для PySpark) | PySpark → Сервер |

**Параметры:**
- `session_id`: уникальный идентификатор сессии логирования
- `output_type`: тип вывода (`stdout` или `stderr`)

## Примеры использования

### Клиент (чтение логов)

```bash
# Создаёт сессию, подключается и выводит логи в консоль
python examples/client_reader.py my-session-123 http://localhost:8160

# Если сессия уже создана
python examples/client_reader.py my-session-123 http://localhost:8160 --no-create-session
```

### PySpark (запись логов)

```bash
# Подключается к сессии и отправляет stdout/stderr
python examples/pyspark_writer.py my-session-123 http://localhost:8160
```

## Тесты

```bash
# Запустить все тесты
uv run pytest tests/ -v -s

# Запустить конкретный тест
uv run pytest tests/test_integration.py::test_log_proxy_integration -v -s
```

**Доступные тесты:**
- `test_log_proxy_integration` — интеграционный тест полного цикла работы (клиент + писатель)
- `test_session_lifecycle` — тест жизненного цикла сессии (создание, дублирование, удаление)
- `test_session_auto_cleanup` — тест автоматической очистки истёкших сессий (~30 секунд)

## Структура проекта

```
log_listener_proxy/
├── log_listener_proxy/          # Основной пакет микросервиса
│   ├── __init__.py              # Экспорт FastAPI app
│   └── server.py                # Основной код сервера
├── examples/                    # Примеры использования
│   ├── client_reader.py         # Клиент для чтения логов (Python 3.7+)
│   └── pyspark_writer.py        # PySpark-писатель логов (Python 3.7+)
├── tests/                       # Интеграционные тесты
│   ├── __init__.py
│   └── test_integration.py      # Тесты с multiprocessing
├── pyproject.toml               # Зависимости и метаданные проекта
└── README.md                    # Документация
```

## Описание файлов

### `log_listener_proxy/server.py`

Основной файл микросервиса. Содержит:

**Pydantic модели:**
- `ServerSettings(BaseSettings)` — конфигурация сервера с поддержкой env-переменных (префикс `LOG_LISTENER_`)
- `SessionResponse` — ответ при создании/удалении сессии
- `LoggingSession` — класс сессии логирования с очередями для stdout/stderr

**Глобальные переменные:**
- `sessions: dict[str, LoggingSession]` — хранилище активных сессий
- `session_ttl: int` — время жизни сессии в секундах (настраивается через `ServerSettings.ttl`)

**Вспомогательные функции:**
- `_schedule_session_deletion()` — отложенное удаление сессии через `asyncio.sleep`
- `_port_manager_context()` — контекст-менеджер для получения/освобождения порта через порт-менеджер

**HTTP-эндпоинты:**
- `GET /health` — проверка работоспособности сервиса
- `POST /logging_session/{session_id}` — создание новой сессии логирования
- `DELETE /logging_session/{session_id}` — удаление сессии

**WebSocket-эндпоинты:**
- `/logs/{session_id}/read/{output_type}` — чтение логов клиентом (может быть несколько reader-ов)
- `/logs/{session_id}/write/{output_type}` — запись логов от PySpark (только один writer на output_type)

**Логика работы:**
1. При создании сессии запускается отложенная задача `_schedule_session_deletion()` через `asyncio.create_task()`
2. Задача ждёт `session_ttl` секунд и удаляет сессию
3. При закрытии write-соединения в очередь отправляется `None`, что сигнализирует reader-ам о необходимости закрыться через 5 секунд
4. Если `use_port_manager=True`, порт запрашивается у порт-менеджера при старте и освобождается при завершении

### `examples/client_reader.py`

Клиентский скрипт для чтения логов. Совместим с Python 3.7+.

**Основные функции:**
- `create_session(proxy_url, session_id)` — создаёт сессию через POST-запрос
- `delete_session(proxy_url, session_id)` — удаляет сессию через DELETE-запрос
- `LogReader` — класс для чтения логов из WebSocket в отдельном потоке

**Класс LogReader:**
- Использует `websocket.WebSocketApp` для подключения
- Запускается в отдельном потоке (`threading.Thread`)
- Перенаправляет полученные данные в `sys.stdout` или `sys.stderr`
- Отслеживает состояние подключения (`connected`, `closed`)

### `examples/pyspark_writer.py`

Скрипт для PySpark-приложений, отправляющий логи на прокси. Совместим с Python 3.7+.

**Класс StreamToWebSocket:**
- Обёртка для перенаправления stdout/stderr в WebSocket
- Подключается к WebSocket при инициализации
- Метод `write()` отправляет данные в оригинальный поток и в WebSocket
- Использует блокировку (`threading.Lock`) для потокобезопасности

### `tests/test_integration.py`

Интеграционные тесты с использованием `multiprocessing`.

**Фикстура `server_process`:**
- Запускает сервер в отдельном процессе через `multiprocessing.Process`
- Вызывает `server.main(host, port)` напрямую (без subprocess)
- Ждёт запуска сервера через `wait_for_server()`
- Останавливает сервер после выполнения тестов

**Тесты:**
1. `test_log_proxy_integration` — полный цикл: создание сессии, запуск клиента и писателя, проверка передачи данных
2. `test_session_lifecycle` — проверка создания, дублирования и удаления сессий
3. `test_session_auto_cleanup` — проверка автоматической очистки истёкших сессий (запускает сервер с TTL=10 секунд)

## Совместимость

- **Сервер**: Python 3.12+
- **Клиентские скрипты** (`examples/`): Python 3.7+
- **Зависимости сервера**: FastAPI, uvicorn, websockets, pydantic, pydantic-settings, requests
- **Зависимости клиентов**: requests, websocket-client

## Особенности реализации

### Автоочистка сессий

Сессии автоматически удаляются по истечении TTL. Это реализовано через:
- Отложенную асинхронную задачу `_schedule_session_deletion()` для каждой сессии
- Запуск через `asyncio.create_task()` при создании сессии
- Удаление сессии через `session_ttl` секунд после создания

### Порт-менеджер

При развёртывании в Kubernetes можно использовать порт-менеджер для динамического выделения портов:
- Включается через `LOG_LISTENER_USE_PORT_MANAGER=true`
- Порт запрашивается через `GET /v2?portcount=1&name={pod_name}`
- Порт освобождается через `DELETE /v2?portcount=1&name={pod_name}` при завершении
- Имя пода определяется из `POD_NAME` (Kubernetes downward API) или `socket.gethostname()`

### Многопоточность и безопасность

- WebSocket-соединения обрабатываются асинхронно через FastAPI/Starlette
- Клиентские скрипты используют `threading` для работы с WebSocket в фоне
- `StreamToWebSocket` использует `threading.Lock` для потокобезопасной записи
- Глобальный словарь `sessions` защищён GIL в CPython

### Тестирование через multiprocessing

Тесты используют `multiprocessing.Process` вместо `subprocess.Popen`:
- Прямой вызов функций `main()` из модулей
- Передача аргументов напрямую (без парсинга `sys.argv`)
- Более быстрый запуск и остановка процессов
- Упрощённая отладка
