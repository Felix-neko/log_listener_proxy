"""
Скрипт-пример PySpark-приложения, отправляющего stdout/stderr в log_listener_proxy.

Совместим с Python 3.7+.

Использование:
    python pyspark_writer.py <session_id> <proxy_url>

Пример:
    python pyspark_writer.py my-session-123 http://localhost:8160

Этот скрипт перехватывает stdout и stderr и отправляет их копию на прокси.
"""

from __future__ import print_function

import sys
import threading
import time

import websocket


class StreamToWebSocket:
    """Класс-обёртка для перенаправления потока в WebSocket."""

    def __init__(self, original_stream, ws_url, output_type):
        # type: (object, str, str) -> None
        self.original_stream = original_stream
        self.ws_url = ws_url
        self.output_type = output_type
        self.ws = None
        self.connected = False
        self._closed = False
        self.lock = threading.Lock()
        self._connect()

    def _connect(self):
        # type: () -> None
        """Подключается к WebSocket."""
        try:
            self.ws = websocket.WebSocket()
            self.ws.connect(self.ws_url, timeout=10)
            self.connected = True
        except Exception as e:
            # Выводим ошибку в оригинальный поток, чтобы не рекурсивно зациклиться
            self.original_stream.write("[PYSPARK-WRITER] Ошибка подключения к {}: {}\n".format(self.output_type, e))
            self.original_stream.flush()

    def write(self, data):
        # type: (str) -> int
        """Записывает данные в оригинальный поток и в WebSocket."""
        if not data:
            return 0

        # Пишем в оригинальный поток
        self.original_stream.write(data)

        # Отправляем в WebSocket, если подключены
        if self.connected and not self._closed:
            with self.lock:
                try:
                    self.ws.send(data)
                except Exception:
                    pass  # Игнорируем ошибки отправки

        return len(data)

    def flush(self):
        # type: () -> None
        """Сбрасывает буфер."""
        self.original_stream.flush()

    def close(self):
        # type: () -> None
        """Закрывает WebSocket."""
        self._closed = True
        if self.ws and self.connected:
            try:
                self.ws.close()
            except Exception:
                pass

    @property
    def encoding(self):
        # type: () -> str
        """Возвращает кодировку оригинального потока."""
        return getattr(self.original_stream, "encoding", "utf-8")


def main(session_id="test-session", proxy_url="http://localhost:8160"):
    # type: (str, str) -> int
    """Основная функция, имитирующая работу PySpark-приложения."""

    # Преобразуем HTTP URL в WS URL
    ws_url = proxy_url.replace("http://", "ws://").replace("https://", "wss://")

    # Сохраняем оригинальные потоки
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    # Создаём обёртки для перенаправления
    stdout_wrapper = StreamToWebSocket(original_stdout, "{}/logs/{}/write/stdout".format(ws_url, session_id), "stdout")
    stderr_wrapper = StreamToWebSocket(original_stderr, "{}/logs/{}/write/stderr".format(ws_url, session_id), "stderr")

    # Подменяем потоки
    sys.stdout = stdout_wrapper
    sys.stderr = stderr_wrapper

    try:
        # Проверяем подключение
        if not stdout_wrapper.connected:
            original_stderr.write("[PYSPARK-WRITER] Не удалось подключиться к stdout WebSocket\n")
            return 1
        if not stderr_wrapper.connected:
            original_stderr.write("[PYSPARK-WRITER] Не удалось подключиться к stderr WebSocket\n")
            return 1

        original_stderr.write("[PYSPARK-WRITER] Подключено к прокси, начинаем вывод\n")

        # Имитируем работу PySpark-приложения
        print("=== Начало работы PySpark-приложения ===")
        for i in range(5):
            print("Обработка партиции {}/5".format(i + 1))
            time.sleep(0.2)
            if i == 2:
                print("WARN: Предупреждение на партиции {}".format(i + 1), file=sys.stderr)

        print("=== Завершение работы PySpark-приложения ===")
        print("Итого: обработано 5 партиций", file=sys.stderr)

        # Даём время на отправку последних сообщений
        time.sleep(0.5)
        return 0

    finally:
        # Восстанавливаем оригинальные потоки
        sys.stdout = original_stdout
        sys.stderr = original_stderr

        # Закрываем WebSocket-соединения
        stdout_wrapper.close()
        stderr_wrapper.close()

        original_stderr.write("[PYSPARK-WRITER] WebSocket-соединения закрыты\n")


if __name__ == "__main__":
    # Парсим аргументы командной строки, если запускаем как скрипт
    if len(sys.argv) >= 3:
        session_id = sys.argv[1]
        proxy_url = sys.argv[2]
        sys.exit(main(session_id, proxy_url))
    else:
        # Используем дефолтные значения
        print("Использование: {} <session_id> <proxy_url>".format(sys.argv[0]), file=sys.stderr)
        print(
            "Запуск с дефолтными параметрами: session_id='test-session', proxy_url='http://localhost:8160'",
            file=sys.stderr,
        )
        sys.exit(main())
