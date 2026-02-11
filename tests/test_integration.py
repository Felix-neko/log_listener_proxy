"""
Интеграционный тест для log_listener_proxy.

Запускает микросервис, клиент-читатель и pyspark-писатель в отдельных подпроцессах
и проверяет, что данные корректно передаются от писателя к читателю.
"""

import multiprocessing
import os
import sys
import time
from pathlib import Path

import pytest

# Путь к директории проекта
PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLES_DIR = PROJECT_ROOT / "examples"

# Параметры теста
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 8161  # Используем нестандартный порт для тестов
PROXY_URL = f"http://{PROXY_HOST}:{PROXY_PORT}"
SESSION_ID = "test-session-integration"


def wait_for_server(url, timeout=30):
    """Ждёт, пока сервер начнёт отвечать."""
    import requests

    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{url}/docs", timeout=1)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


@pytest.fixture(scope="module")
def server_process():
    """Запускает сервер в отдельном процессе через multiprocessing."""
    from log_listener_proxy.server import main as server_main

    # Запускаем сервер в отдельном процессе
    proc = multiprocessing.Process(
        target=server_main,
        args=(PROXY_HOST, PROXY_PORT),
    )
    proc.start()

    # Ждём запуска сервера
    if not wait_for_server(PROXY_URL):
        proc.terminate()
        proc.join(timeout=5)
        pytest.fail(f"Сервер не запустился")

    yield proc

    # Останавливаем сервер
    proc.terminate()
    proc.join(timeout=5)
    if proc.is_alive():
        proc.kill()
        proc.join()


def test_log_proxy_integration(server_process):
    """
    Интеграционный тест: клиент создаёт сессию, читает логи,
    а pyspark-писатель отправляет данные.
    """
    import requests

    # 1. Создаём сессию логирования
    resp = requests.post(f"{PROXY_URL}/logging_session/{SESSION_ID}")
    assert resp.status_code == 200, f"Не удалось создать сессию: {resp.text}"

    try:
        # 2. Запускаем клиент-читатель в фоне через multiprocessing
        import sys
        from io import StringIO
        from examples.client_reader import main as client_main
        from examples.pyspark_writer import main as writer_main

        # Создаём очереди для перехвата stdout/stderr
        client_stdout_queue = multiprocessing.Queue()
        client_stderr_queue = multiprocessing.Queue()
        writer_stdout_queue = multiprocessing.Queue()
        writer_stderr_queue = multiprocessing.Queue()

        def run_client():
            """Запускает клиент и возвращает exit code."""
            exit_code = client_main(
                session_id=SESSION_ID,
                proxy_url=PROXY_URL,
                skip_create_session=True,
            )
            sys.exit(exit_code)

        def run_writer():
            """Запускает писатель и возвращает exit code."""
            exit_code = writer_main(
                session_id=SESSION_ID,
                proxy_url=PROXY_URL,
            )
            sys.exit(exit_code)

        # Запускаем клиент
        client_proc = multiprocessing.Process(target=run_client)
        client_proc.start()

        # Даём клиенту время подключиться
        time.sleep(2)

        # 3. Запускаем pyspark-писатель
        writer_proc = multiprocessing.Process(target=run_writer)
        writer_proc.start()

        # Ждём завершения писателя
        writer_proc.join(timeout=30)
        writer_exit_code = writer_proc.exitcode
        if writer_proc.is_alive():
            writer_proc.terminate()
            writer_proc.join(timeout=5)
            writer_exit_code = -1

        # Ждём завершения читателя (должен закрыться через 5 секунд после писателя)
        client_proc.join(timeout=30)
        client_exit_code = client_proc.exitcode
        if client_proc.is_alive():
            client_proc.terminate()
            client_proc.join(timeout=5)
            client_exit_code = -1

        # 4. Проверяем результаты
        # Писатель должен завершиться успешно
        assert writer_exit_code == 0, f"Писатель завершился с ошибкой: {writer_exit_code}"
        # Клиент должен завершиться успешно
        assert client_exit_code == 0, f"Клиент завершился с ошибкой: {client_exit_code}"

    finally:
        # Удаляем сессию
        requests.delete(f"{PROXY_URL}/logging_session/{SESSION_ID}")


def test_session_lifecycle(server_process):
    """Тест жизненного цикла сессии: создание, проверка, удаление."""
    import requests

    test_session = "lifecycle-test-session"

    # Создаём сессию
    resp = requests.post(f"{PROXY_URL}/logging_session/{test_session}")
    assert resp.status_code == 200

    # Повторное создание должно вернуть ошибку
    resp = requests.post(f"{PROXY_URL}/logging_session/{test_session}")
    assert resp.status_code == 400

    # Удаляем сессию
    resp = requests.delete(f"{PROXY_URL}/logging_session/{test_session}")
    assert resp.status_code == 200

    # Повторное удаление должно вернуть ошибку
    resp = requests.delete(f"{PROXY_URL}/logging_session/{test_session}")
    assert resp.status_code == 404


def test_session_auto_cleanup():
    """Тест автоматической очистки истёкших сессий."""
    import requests
    from log_listener_proxy.server import main as server_main

    # Запускаем сервер с коротким TTL (10 секунд для теста)
    test_port = 8162
    test_url = f"http://127.0.0.1:{test_port}"

    proc = multiprocessing.Process(
        target=server_main,
        args=("127.0.0.1", test_port, 10),  # TTL = 10 секунд
    )
    proc.start()

    # Ждём запуска сервера
    if not wait_for_server(test_url):
        proc.terminate()
        proc.join(timeout=5)
        pytest.fail("Сервер не запустился")

    try:
        test_session = "auto-cleanup-test-session"

        # Создаём сессию
        resp = requests.post(f"{test_url}/logging_session/{test_session}")
        assert resp.status_code == 200, f"Не удалось создать сессию: {resp.text}"

        # Проверяем, что сессия существует (пытаемся создать дубликат)
        resp = requests.post(f"{test_url}/logging_session/{test_session}")
        assert resp.status_code == 400, "Сессия должна существовать"

        # Ждём 25 секунд (10 секунд TTL + 10 секунд на срабатывание задачи очистки + 5 секунд запас)
        print("\n[TEST] Ожидание автоочистки сессии (25 секунд)...")
        time.sleep(25)

        # Проверяем, что сессия была удалена (пытаемся удалить её)
        resp = requests.delete(f"{test_url}/logging_session/{test_session}")
        assert resp.status_code == 404, "Сессия должна была быть автоматически удалена"

        print("[TEST] Сессия успешно автоматически удалена")

    finally:
        # Останавливаем сервер
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
            proc.join()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
