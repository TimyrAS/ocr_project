#!/usr/bin/env python3
"""
Графический лаунчер пайплайна OCR.

Сотрудник видит окно с кнопками:
  1. «Добавить фото» — выбор файлов, копируются в Photo/
  2. «Запустить» — запускает run_pipeline.py, лог в окне
  3. По окончании — автоматически открывает Excel-результат

Запуск:
    python3 launcher.py
"""

import os
import platform
import shutil
import subprocess
import sys
import threading
import tkinter as tk
from tkinter import filedialog, scrolledtext
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "Photo"
OUTPUT_FILE = ROOT / "clients_database.xlsx"
PIPELINE_SCRIPT = ROOT / "run_pipeline.py"

PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tiff", ".tif", ".bmp"}


def open_file(path):
    """Открыть файл стандартной программой ОС."""
    path = str(path)
    if platform.system() == "Darwin":
        subprocess.Popen(["open", path])
    elif platform.system() == "Windows":
        os.startfile(path)
    else:
        subprocess.Popen(["xdg-open", path])


class LauncherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("OCR — Оцифровка карточек")
        self.root.geometry("700x500")
        self.root.resizable(True, True)

        self._running = False

        # --- Верхняя панель кнопок ---
        btn_frame = tk.Frame(root)
        btn_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        self.btn_add = tk.Button(
            btn_frame, text="📂  Добавить фото",
            command=self._add_photos, width=20, height=2,
        )
        self.btn_add.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_run = tk.Button(
            btn_frame, text="▶  Запустить",
            command=self._start_pipeline, width=20, height=2,
        )
        self.btn_run.pack(side=tk.LEFT, padx=(0, 10))

        self.btn_open = tk.Button(
            btn_frame, text="📄  Открыть результат",
            command=lambda: open_file(OUTPUT_FILE), width=20, height=2,
        )
        self.btn_open.pack(side=tk.LEFT)

        # --- Счётчик фото ---
        self.lbl_status = tk.Label(root, text="", anchor=tk.W)
        self.lbl_status.pack(fill=tk.X, padx=10)

        # --- Лог ---
        self.log = scrolledtext.ScrolledText(
            root, state=tk.DISABLED, wrap=tk.WORD, font=("Menlo", 11),
        )
        self.log.pack(fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))

        self._update_photo_count()

    # ── Добавление фото ──────────────────────────────────

    def _add_photos(self):
        files = filedialog.askopenfilenames(
            title="Выберите фото карточек",
            filetypes=[
                ("Изображения", "*.jpg *.jpeg *.png *.heic *.heif *.tiff *.tif *.bmp"),
                ("Все файлы", "*.*"),
            ],
        )
        if not files:
            return

        INPUT_DIR.mkdir(parents=True, exist_ok=True)
        copied = 0
        for f in files:
            src = Path(f)
            if src.suffix.lower() not in PHOTO_EXTENSIONS:
                continue
            dest = INPUT_DIR / src.name
            if dest.exists():
                self._log_msg(f"  Пропущен (уже есть): {src.name}")
                continue
            shutil.copy2(src, dest)
            copied += 1

        self._log_msg(f"Добавлено фото: {copied} из {len(files)}")
        self._update_photo_count()

    def _update_photo_count(self):
        if INPUT_DIR.exists():
            count = sum(
                1 for f in INPUT_DIR.iterdir()
                if f.suffix.lower() in PHOTO_EXTENSIONS
            )
        else:
            count = 0
        self.lbl_status.config(text=f"Фото в папке Photo/: {count}")

    # ── Запуск пайплайна ─────────────────────────────────

    def _start_pipeline(self):
        if self._running:
            return
        self._running = True
        self.btn_run.config(state=tk.DISABLED, text="⏳  Работает...")
        self.btn_add.config(state=tk.DISABLED)
        self._clear_log()
        self._log_msg("Запуск пайплайна...\n")

        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self):
        try:
            proc = subprocess.Popen(
                [sys.executable, str(PIPELINE_SCRIPT)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(ROOT),
            )
            for line in proc.stdout:
                self.root.after(0, self._log_msg, line.rstrip("\n"))
            proc.wait()

            if proc.returncode == 0:
                self.root.after(0, self._log_msg, "\n✅ Готово!")
                if OUTPUT_FILE.exists():
                    self.root.after(0, open_file, OUTPUT_FILE)
            else:
                self.root.after(0, self._log_msg,
                                f"\n❌ Ошибка (код {proc.returncode})")
        except Exception as e:
            self.root.after(0, self._log_msg, f"\n❌ Ошибка: {e}")
        finally:
            self.root.after(0, self._finish_pipeline)

    def _finish_pipeline(self):
        self._running = False
        self.btn_run.config(state=tk.NORMAL, text="▶  Запустить")
        self.btn_add.config(state=tk.NORMAL)
        self._update_photo_count()

    # ── Лог ───────────────────────────────────────────────

    def _log_msg(self, msg):
        self.log.config(state=tk.NORMAL)
        self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.log.config(state=tk.DISABLED)

    def _clear_log(self):
        self.log.config(state=tk.NORMAL)
        self.log.delete("1.0", tk.END)
        self.log.config(state=tk.DISABLED)


if __name__ == "__main__":
    app = tk.Tk()
    LauncherApp(app)
    app.mainloop()
