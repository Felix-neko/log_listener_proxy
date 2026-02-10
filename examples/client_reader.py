"""
Скрипт-пример клиента, читающего логи из log_listener_proxy.

Совместим с Python 3.7+.

Использование:
    python client_reader.py <session_id> <proxy_url>

Пример:
    python client_reader.py my-session-123 http://localhost:8160
"""

from __future__ import print_function

import sys
import threading
import time

import requests
import websocket


def create_session(proxy_url, session_id):
    # type: (str, str) -> bool
    """Создаёт сессию логирования на прокси."""
    url = "{}/logging_session/{}".format(proxy_url, session_id)
    try:
        resp = requests.post(url, timeout=10)
        if resp.status_code == 200:
            print("[CLIENT-READER] Сессия '{}' создана".format(session_id), file=sys.stderr)
            return True
        else:
            print("[CLIENT-READER] Ошибка создания сессии: {}".format(resp.text), file=sys.stderr)
            return False
    except Exception as e:
        print("[CLIENT-READER] Ошибка подключения: {}".format(e), file=sys.stderr)
        return False


def delete_session(proxy_url, session_id):
    # type: (str, str) -> None
    """Удаляет сессию логирования на прокси."""
    url = "{}/logging_session/{}".format(proxy_url, session_id)
    try:
        resp = requests.delete(url, timeout=10)
        if resp.status_code == 200:
            print("[CLIENT-READER] Сессия '{}' удалена".format(session_id), file=sys.stderr)
    except Exception as e:
        print("[CLIENT-READER] Ошибка удаления сессии: {}".format(e), file=sys.stderr)


class LogReader:
    """Читатель логов из WebSocket."""

    def __init__(self, ws_url, output_type, output_stream):
        # type: (str, str, object) -> None
        self.ws_url = ws_url
        self.output_type = output_type
        self.output_stream = output_stream
        self.ws = None
        self.connected = False
        self.closed = False
        self.thread = None

    def on_message(self, ws, message):
        # type: (websocket.WebSocketApp, str) -> None
        """Обработчик входящих сообщений."""
        # Пишем в соответствующий поток (stdout или stderr)
        print(message, file=self.output_stream, end="")
        self.output_stream.flush()

    def on_error(self, ws, error):
        # type: (websocket.WebSocketApp, Exception) -> None
        """Обработчик ошибок."""
        print("[CLIENT-READER] WebSocket {} ошибка: {}".format(self.output_type, error), file=sys.stderr)

    def on_close(self, ws, close_status_code, close_msg):
        # type: (websocket.WebSocketApp, int, str) -> None
        """Обработчик закрытия соединения."""
        self.closed = True
        print("[CLIENT-READER] WebSocket {} закрыт".format(self.output_type), file=sys.stderr)

    def on_open(self, ws):
        # type: (websocket.WebSocketApp) -> None
        """Обработчик открытия соединения."""
        self.connected = True
        print("[CLIENT-READER] WebSocket {} подключён".format(self.output_type), file=sys.stderr)

    def start(self):
        # type: () -> None
        """Запускает чтение в отдельном потоке."""
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.thread = threading.Thread(target=self.ws.run_forever)
        self.thread.daemon = True
        self.thread.start()

    def wait_connected(self, timeout=10):
        # type: (int) -> bool
        """Ждёт подключения к WebSocket."""
        start = time.time()
        while not self.connected and not self.closed:
            if time.time() - start > timeout:
                return False
            time.sleep(0.1)
        return self.connected

    def wait_closed(self, timeout=60):
        # type: (int) -> bool
        """Ждёт закрытия WebSocket."""
        start = time.time()
        while not self.closed:
            if time.time() - start > timeout:
                return False
            time.sleep(0.1)
        return True

    def close(self):
        # type: () -> None
        """Закрывает соединение."""
        if self.ws:
            self.ws.close()


def main(session_id="test-session", proxy_url="http://localhost:8160", skip_create_session=False):
    # type: (str, str, bool) -> int
    """Основная функция."""

    # Преобразуем HTTP URL в WS URL
    ws_url = proxy_url.replace("http://", "ws://").replace("https://", "wss://")

    # Создаём сессию логирования (если не указан флаг --no-create-session)
    if not skip_create_session:
        if not create_session(proxy_url, session_id):
            return 1

    try:
        # Подключаемся к stdout и stderr
        stdout_reader = LogReader("{}/logs/{}/read/stdout".format(ws_url, session_id), "stdout", sys.stdout)
        stderr_reader = LogReader("{}/logs/{}/read/stderr".format(ws_url, session_id), "stderr", sys.stderr)

        stdout_reader.start()
        stderr_reader.start()

        # Ждём подключения
        if not stdout_reader.wait_connected():
            print("[CLIENT-READER] Не удалось подключиться к stdout", file=sys.stderr)
            return 1
        if not stderr_reader.wait_connected():
            print("[CLIENT-READER] Не удалось подключиться к stderr", file=sys.stderr)
            return 1

        print("[CLIENT-READER] Ожидание данных...", file=sys.stderr)

        # Ждём закрытия обоих соединений
        stdout_reader.wait_closed(timeout=300)
        stderr_reader.wait_closed(timeout=300)

        print("[CLIENT-READER] Все соединения закрыты, завершаем работу", file=sys.stderr)
        return 0

    finally:
        # Удаляем сессию (если мы её создавали)
        if not skip_create_session:
            delete_session(proxy_url, session_id)


if __name__ == "__main__":
    # Парсим аргументы командной строки, если запускаем как скрипт
    if len(sys.argv) >= 3:
        session_id = sys.argv[1]
        proxy_url = sys.argv[2]
        skip_create_session = "--no-create-session" in sys.argv
        sys.exit(main(session_id, proxy_url, skip_create_session))
    else:
        # Используем дефолтные значения
        print("Использование: {} <session_id> <proxy_url> [--no-create-session]".format(sys.argv[0]), file=sys.stderr)
        print(
            "Запуск с дефолтными параметрами: session_id='test-session', proxy_url='http://localhost:8160'",
            file=sys.stderr,
        )
        sys.exit(main())
