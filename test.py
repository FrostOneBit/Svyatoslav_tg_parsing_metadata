import os
import subprocess

# Укажи свой путь к директории с проектами
BASE_DIR = r"D:\Program\Python\Project"

# Старое и новое имя пользователя на GitHub
old_username = "FrostOneBit"
new_username = "0x-als"

def is_git_repo(path):
    return os.path.isdir(os.path.join(path, ".git"))

def get_git_remote_url(path):
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=path,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def set_git_remote_url(path, new_url):
    try:
        subprocess.run(
            ["git", "remote", "set-url", "origin", new_url],
            cwd=path,
            check=True
        )
        print(f"[✔] Updated remote in: {path}")
    except subprocess.CalledProcessError as e:
        print(f"[!] Failed to update remote in {path}: {e}")

def update_all_git_remotes(base_dir, old_user, new_user):
    for root, dirs, files in os.walk(base_dir):
        if ".git" in dirs:
            remote_url = get_git_remote_url(root)
            if remote_url and old_user in remote_url:
                new_url = remote_url.replace(old_user, new_user)
                set_git_remote_url(root, new_url)
            dirs[:] = []  # чтобы не углубляться внутрь .git-папок

if __name__ == "__main__":
    update_all_git_remotes(BASE_DIR, old_username, new_username)
