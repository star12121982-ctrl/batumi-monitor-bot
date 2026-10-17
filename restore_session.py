# -*- coding: utf-8 -*-
"""
Восстанавливает session.session из зашифрованного session.enc.

Используется в облаке (GitHub Actions): ключ расшифровки лежит в секрете
SESSION_KEY, сам файл сессии — в репозитории в зашифрованном виде.
"""
import base64
import os
import pathlib
import sys

BASE = pathlib.Path(__file__).parent
OUT = BASE / "session.session"
ENC = BASE / "session.enc"


def main() -> int:
    if OUT.exists() and OUT.stat().st_size > 0:
        print("Сессия уже на месте:", OUT.stat().st_size, "байт")
        return 0

    key_b64 = os.environ.get("SESSION_KEY", "").strip()
    if not key_b64 or not ENC.exists():
        print("Нет SESSION_KEY или session.enc — потребуется вход по QR-коду.")
        return 0

    import nacl.secret  # pynacl

    key = base64.b64decode(key_b64)
    blob = base64.b64decode(ENC.read_text().strip())
    OUT.write_bytes(nacl.secret.SecretBox(key).decrypt(blob))
    print("Сессия восстановлена:", OUT.stat().st_size, "байт")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("ОШИБКА восстановления сессии:", type(e).__name__, e)
        sys.exit(1)
