"""
Микросервис log_listener_proxy для проксирования stdout/stderr от PySpark-приложений к клиентам.

Сценарий использования:
1. Клиент создаёт сессию логирования через POST /logging_session
2. Клиент подключается к WS /logs/{session_id}/read/stdout и /logs/{session_id}/read/stderr
3. PySpark-приложение подключается к WS /logs/{session_id}/write/stdout и /logs/{session_id}/write/stderr
4. Данные из write-эндпоинтов проксируются в read-эндпоинты в реальном времени
5. Сессии автоматически удаляются по истечении времени жизни
"""

import asyncio
import os
import socket
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Literal
from urllib.parse import urljoin

import requests as http_requests
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerSettings(BaseSettings):
    """Настройки сервера log_listener_proxy.

    Значения можно задавать через переменные среды с префиксом LOG_LISTENER_.
    Например: LOG_LISTENER_PORT=9000, LOG_LISTENER_USE_PORT_MANAGER=true.
    """

    model_config = SettingsConfigDict(
        # Префикс для переменных среды: LOG_LISTENER_HOST, LOG_LISTENER_PORT, ...
        env_prefix="LOG_LISTENER_",
    )

    # Хост для прослушивания
    host: str = "0.0.0.0"
    # Порт для прослушивания (игнорируется, если use_port_manager=True)
    port: int = 8160
    # Время жизни сессии логирования в секундах (по умолчанию 1 час)
    ttl: int = 3600
    # Если True, порт запрашивается у порт-менеджера вместо использования фиксированного значения
    use_port_manager: bool = False
    # URL порт-менеджера для получения/освобождения портов
    port_manager_url: str = "http://portmanager.airflow-fs.svc/manage"


class SessionResponse(BaseModel):
    """Ответ при создании/удалении сессии."""

    status: str = Field(..., description="Статус операции (ok/error)")
    session_id: str = Field(..., description="Идентификатор сессии")
    message: str = Field(..., description="Сообщение о результате операции")


class LoggingSession:
    """Сессия логирования с очередями для stdout и stderr."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        # Очереди для передачи данных от writer к reader
        self.stdout_queue: asyncio.Queue[str | None] = asyncio.Queue()
        self.stderr_queue: asyncio.Queue[str | None] = asyncio.Queue()
        # Флаги для отслеживания состояния writer-соединений
        self.stdout_writer_connected = False
        self.stderr_writer_connected = False
        # Количество подключённых reader-ов
        self.stdout_readers: list[WebSocket] = []
        self.stderr_readers: list[WebSocket] = []
        # Время создания сессии для автоочистки
        self.created_at: float = time.time()


# Глобальное хранилище сессий
sessions: dict[str, LoggingSession] = {}

# Глобальная переменная для времени жизни сессии (в секундах)
session_ttl: int = 3600  # по умолчанию 1 час


async def _schedule_session_deletion(session_id: str, delay_seconds: int):
    """Вспомогательная функция для отложенного удаления сессии."""
    await asyncio.sleep(delay_seconds)
    if session_id in sessions:
        del sessions[session_id]
        print(f"[CLEANUP] Удалена истёкшая сессия: {session_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Контекстный менеджер для управления жизненным циклом приложения."""
    yield
    # Очищаем сессии при завершении
    sessions.clear()


app = FastAPI(
    title="Log Listener Proxy",
    description="Микросервис для проксирования логов от PySpark к клиентам",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Проверка работоспособности сервиса."""
    return {"name": "log_listener_proxy", "status": "ok"}


@app.post("/logging_session/{session_id}", response_model=SessionResponse)
async def create_logging_session(session_id: str) -> SessionResponse:
    """Создать новую сессию логирования."""
    if session_id in sessions:
        raise HTTPException(status_code=400, detail=f"Сессия '{session_id}' уже существует")
    sessions[session_id] = LoggingSession(session_id)
    
    # Запускаем отложенную задачу на удаление сессии через TTL секунд
    # Используем asyncio.sleep в отдельной задаче для отложенного выполнения
    asyncio.create_task(_schedule_session_deletion(session_id, session_ttl))
    
    return SessionResponse(status="ok", session_id=session_id, message="Сессия создана")


@app.delete("/logging_session/{session_id}", response_model=SessionResponse)
async def delete_logging_session(session_id: str) -> SessionResponse:
    """Удалить сессию логирования."""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail=f"Сессия '{session_id}' не найдена")
    del sessions[session_id]
    return SessionResponse(status="ok", session_id=session_id, message="Сессия удалена")


@app.websocket("/logs/{session_id}/read/{output_type}")
async def read_logs(websocket: WebSocket, session_id: str, output_type: Literal["stdout", "stderr"]):
    """WebSocket-эндпоинт для чтения логов (для клиента)."""
    if session_id not in sessions:
        await websocket.close(code=4004, reason=f"Сессия '{session_id}' не найдена")
        return

    session = sessions[session_id]
    await websocket.accept()

    # Выбираем нужную очередь и список reader-ов
    if output_type == "stdout":
        queue = session.stdout_queue
        session.stdout_readers.append(websocket)
    else:
        queue = session.stderr_queue
        session.stderr_readers.append(websocket)

    try:
        while True:
            # Ждём данные из очереди
            data = await queue.get()
            if data is None:
                # None — сигнал о закрытии writer-а, ждём 5 секунд и закрываем
                await asyncio.sleep(5)
                break
            # Отправляем данные клиенту
            await websocket.send_text(data)
    except WebSocketDisconnect:
        pass
    finally:
        # Убираем из списка reader-ов
        if output_type == "stdout":
            if websocket in session.stdout_readers:
                session.stdout_readers.remove(websocket)
        else:
            if websocket in session.stderr_readers:
                session.stderr_readers.remove(websocket)


@app.websocket("/logs/{session_id}/write/{output_type}")
async def write_logs(websocket: WebSocket, session_id: str, output_type: Literal["stdout", "stderr"]):
    """WebSocket-эндпоинт для записи логов (для PySpark-приложения)."""
    if session_id not in sessions:
        await websocket.close(code=4004, reason=f"Сессия '{session_id}' не найдена")
        return

    session = sessions[session_id]

    # Проверяем, что writer ещё не подключён
    if output_type == "stdout":
        if session.stdout_writer_connected:
            await websocket.close(code=4001, reason="Writer для stdout уже подключён")
            return
        session.stdout_writer_connected = True
        queue = session.stdout_queue
        readers = session.stdout_readers
    else:
        if session.stderr_writer_connected:
            await websocket.close(code=4001, reason="Writer для stderr уже подключён")
            return
        session.stderr_writer_connected = True
        queue = session.stderr_queue
        readers = session.stderr_readers

    await websocket.accept()

    try:
        while True:
            # Получаем данные от PySpark
            data = await websocket.receive_text()
            # Кладём в очередь для всех reader-ов
            await queue.put(data)
    except WebSocketDisconnect:
        pass
    finally:
        # Помечаем, что writer отключился
        if output_type == "stdout":
            session.stdout_writer_connected = False
        else:
            session.stderr_writer_connected = False
        # Отправляем None в очередь, чтобы reader-ы узнали о закрытии
        await queue.put(None)


@contextmanager
def _port_manager_context(port_manager_url: str):
    """Контекст-менеджер для получения и освобождения порта через порт-менеджер.

    При входе в контекст запрашивает свободный порт через GET-запрос к порт-менеджеру.
    При выходе из контекста освобождает порт через DELETE-запрос.

    Args:
        port_manager_url: базовый URL порт-менеджера (например, "http://portmanager.airflow-fs.svc/manage")

    Yields:
        int: номер порта, выделенного порт-менеджером
    """
    # Имя пода берём из переменной среды POD_NAME (Kubernetes downward API)
    # или из hostname, если переменная не задана
    pod_name = os.environ.get("POD_NAME", socket.gethostname())
    params = {"portcount": 1, "name": pod_name}

    print(f"[PORT-MANAGER] Запрашиваем порт: url={port_manager_url}, pod={pod_name}")
    resp = http_requests.get(url=urljoin(port_manager_url, "v2"), params=params)
    resp.raise_for_status()
    port = resp.json()["ports"][0]
    print(f"[PORT-MANAGER] Получен порт: {port}")

    try:
        yield port
    finally:
        print(f"[PORT-MANAGER] Освобождаем порт: pod={pod_name}")
        try:
            http_requests.delete(url=urljoin(port_manager_url, "v2"), params=params)
            print(f"[PORT-MANAGER] Порт {port} освобождён")
        except Exception as e:
            print(f"[PORT-MANAGER] Ошибка при освобождении порта: {e}")


def main(
    host: str = "0.0.0.0",
    port: int = 8160,
    ttl: int = 3600,
    use_port_manager: bool = False,
    port_manager_url: str = "http://portmanager.airflow-fs.svc/manage",
):
    """Точка входа для запуска сервера.

    Args:
        host: хост для прослушивания
        port: порт для прослушивания (игнорируется, если use_port_manager=True)
        ttl: время жизни сессии в секундах (по умолчанию 3600 = 1 час)
        use_port_manager: если True, порт запрашивается у порт-менеджера
        port_manager_url: URL порт-менеджера для получения/освобождения портов
    """
    import uvicorn

    global session_ttl
    session_ttl = ttl

    if use_port_manager:
        # Порт запрашивается у порт-менеджера и освобождается при завершении
        with _port_manager_context(port_manager_url) as managed_port:
            uvicorn.run(app, host=host, port=managed_port)
    else:
        uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    settings = ServerSettings()
    main(**settings.model_dump())
