from cryptography.fernet import Fernet

# Генерация ключа
key = Fernet.generate_key()
print(f"Сохраните ключ: {key.decode()}")

# Сохраните ключ в защищённом месте, например, в файле:
with open("secret.key", "wb") as key_file:
    key_file.write(key)