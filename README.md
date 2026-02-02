# Log Listener Proxy

Микросервис для проксирования stdout/stderr от PySpark-приложений к клиентам через WebSocket.

## Назначение

Этот микросервис решает проблему получения логов от PySpark-приложений, запущенных на удалённом Spark-кластере. Когда вы запускаете `spark-submit`, вывод stdout/stderr остаётся на удалённом драйвере. **Log Listener Proxy** позволяет получать эти логи в реальном времени на клиентской машине.

## Сценарий использования

1. **Клиентское приложение** создаёт сессию логирования через `POST /logging_session`
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
# Запуск на 0.0.0.0:8160 с TTL=3600 секунд (по умолчанию)
uv run python -m log_listener_proxy.server

# Или через uvicorn с кастомными параметрами
uv run uvicorn log_listener_proxy.server:app --host 0.0.0.0 --port 8160
```

### Параметры запуска

Функция `main(host, port, ttl)` в `server.py` принимает следующие параметры:
- `host` (str): хост для прослушивания (по умолчанию `"0.0.0.0"`)
- `port` (int): порт для прослушивания (по умолчанию `8160`)
- `ttl` (int): время жизни сессии в секундах (по умолчанию `3600` = 1 час)

## API-эндпоинты

### HTTP

| Метод | Эндпоинт | Описание | Request Body | Response |
|-------|----------|----------|--------------|----------|
| POST | `/logging_session` | Создать сессию логирования | `{"session_id": "string"}` | `SessionResponse` |
| DELETE | `/logging_session/{session_id}` | Удалить сессию логирования | - | `SessionResponse` |

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
- `CreateSessionRequest` — запрос на создание сессии
- `SessionResponse` — ответ при создании/удалении сессии
- `LoggingSession` — класс сессии логирования с очередями для stdout/stderr

**Глобальные переменные:**
- `sessions: dict[str, LoggingSession]` — хранилище активных сессий
- `session_ttl: int` — время жизни сессии в секундах (настраивается через `main(ttl=...)`)
- `cleanup_task_running: bool` — флаг для остановки фоновой задачи

**Фоновые задачи:**
- `cleanup_expired_sessions_loop()` — асинхронная задача, которая каждые 10 секунд проверяет и удаляет истёкшие сессии

**HTTP эндпоинты:**
- `POST /logging_session` — создание новой сессии логирования
- `DELETE /logging_session/{session_id}` — удаление сессии

**WebSocket эндпоинты:**
- `/logs/{session_id}/read/{output_type}` — чтение логов клиентом (может быть несколько reader-ов)
- `/logs/{session_id}/write/{output_type}` — запись логов от PySpark (только один writer на output_type)

**Логика работы:**
1. При старте приложения запускается фоновая задача `cleanup_expired_sessions_loop()` через `asyncio.create_task()`
2. Каждые 10 секунд задача проверяет все сессии и удаляет те, у которых `current_time - created_at > session_ttl`
3. При подключении к WebSocket обновляется `last_activity` сессии
4. При получении/отправке данных через WebSocket также обновляется `last_activity`
5. При закрытии write-соединения в очередь отправляется `None`, что сигнализирует reader-ам о необходимости закрыться через 5 секунд

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

**Функция main():**
- Принимает параметры: `session_id`, `proxy_url`, `skip_create_session`
- Создаёт сессию (если `skip_create_session=False`)
- Подключается к stdout и stderr WebSocket-ам
- Ждёт закрытия обоих соединений
- Удаляет сессию при завершении

### `examples/pyspark_writer.py`

Скрипт для PySpark-приложений, отправляющий логи на прокси. Совместим с Python 3.7+.

**Класс StreamToWebSocket:**
- Обёртка для перенаправления stdout/stderr в WebSocket
- Подключается к WebSocket при инициализации
- Метод `write()` отправляет данные в оригинальный поток и в WebSocket
- Использует блокировку (`threading.Lock`) для потокобезопасности

**Функция main():**
- Принимает параметры: `session_id`, `proxy_url`
- Создаёт обёртки для `sys.stdout` и `sys.stderr`
- Подменяет стандартные потоки на обёртки
- Имитирует работу PySpark-приложения (в реальном использовании здесь будет ваш код)
- Восстанавливает оригинальные потоки и закрывает WebSocket-соединения

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
- **Зависимости сервера**: FastAPI, uvicorn, websockets, pydantic
- **Зависимости клиентов**: requests, websocket-client

## Особенности реализации

### Автоочистка сессий

Сессии автоматически удаляются по истечении времени жизни. Это реализовано через:
- Фоновую асинхронную задачу `cleanup_expired_sessions_loop()`
- Запуск через `asyncio.create_task()` в `lifespan` контекстном менеджере
- Проверка каждые 10 секунд
- Сравнение `current_time - session.created_at > session_ttl`

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
