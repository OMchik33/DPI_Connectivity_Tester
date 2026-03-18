#!/usr/bin/env python3
import concurrent.futures
import json
import os
import queue
import socket
import ssl
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import dns.resolver
import requests
import tkinter as tk
from tkinter import messagebox, ttk

APP_TITLE = "Connectivity Tester"
APP_VERSION = "v5.5"
TIMEOUT = 10
MAX_WORKERS = 4
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
SITES_FILENAME = "user_sites.txt"

REMOTE_SUITE_URL = "https://raw.githubusercontent.com/hyperion-cs/dpi-checkers/refs/heads/main/ru/tcp-16-20/suite.json"
REMOTE_HOSTS_URL = "https://raw.githubusercontent.com/hyperion-cs/dpi-checkers/refs/heads/main/ru/tcp-16-20/suite.v2.json"

PRIORITY_SITES = [
    {"id": "TG-WEB", "provider": "Telegram", "country": "", "host": "web.telegram.org", "url": "https://web.telegram.org/"},
    {"id": "YT-WEB", "provider": "YouTube", "country": "", "host": "youtube.com", "url": "https://www.youtube.com/"},
    {"id": "WA-WEB", "provider": "WhatsApp Web", "country": "", "host": "web.whatsapp.com", "url": "https://web.whatsapp.com/"},
    {"id": "GH-WEB", "provider": "GitHub", "country": "", "host": "github.com", "url": "https://github.com/"},
    {"id": "MS-WEB", "provider": "Microsoft", "country": "", "host": "microsoft.com", "url": "https://www.microsoft.com/"},
]

BUNDLED_URL_SUITE = [
    { "id": "SE.AKM-01", "provider": "Akamai", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://media.miele.com/images/2000015/200001503/20000150334.png" },
    { "id": "US.AKM-01", "provider": "Akamai", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://www.roxio.com/static/roxio/videos/products/nxt9/lamp-magic.mp4" },
    { "id": "DE.AWS-01", "provider": "AWS", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://www.getscope.com/assets/fonts/fa-solid-900.woff2" },
    { "id": "US.AWS-01", "provider": "AWS", "country": "", "thresholdBytes": 596179, "times": 1, "url": "https://corp.kaltura.com/wp-content/cache/min/1/wp-content/themes/airfleet/dist/styles/theme.css" },
    { "id": "US.CDN77-01", "provider": "CDN77", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://cdn.eso.org/images/banner1920/eso2520a.jpg" },
    { "id": "CA.CF-01", "provider": "Cloudflare", "country": "", "thresholdBytes": 210116, "times": 1, "url": "https://www.bigcartel.com/_next/static/chunks/453-03e77cda85f8a09a.js" },
    { "id": "CA.CF-02", "provider": "Cloudflare", "country": "", "thresholdBytes": 218884, "times": 1, "url": "https://aegis.audioeye.com/assets/index.js" },
    { "id": "US.CF-01", "provider": "Cloudflare", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://img.wzstats.gg/cleaver/gunFullDisplay" },
    { "id": "US.CF-02", "provider": "Cloudflare", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://esm.sh/gh/esm-dev/esm.sh@e7447dea04/server/embed/assets/sceenshot-deno-types.png" },
    { "id": "FR.CNTB-01", "provider": "Contabo", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://www.cateringexner.cz/font/ebrima/ebrima.woff2" },
    { "id": "FR.CNTB-02", "provider": "Contabo", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://findair.net/wp-content/uploads/2025/07/online-booking-2.jpeg" },
    { "id": "US.DO-01", "provider": "DigitalOcean", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://carishealthcare.com/content/uploads/2025/04/Rectangle-105.jpg" },
    { "id": "US.DO-02", "provider": "DigitalOcean", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://bohnlawllc.com/wp-content/uploads/sites/27/2024/01/Trusts.jpg" },
    { "id": "US.DO-03", "provider": "DigitalOcean", "country": "", "thresholdBytes": 443944, "times": 1, "url": "https://ecomstal.com/_next/static/css/73cc557714b4846b.css" },
    { "id": "CA.FST-01", "provider": "Fastly", "country": "", "thresholdBytes": 250078, "times": 1, "url": "https://ssl.p.jwpcdn.com/player/v/8.40.5/bidding.js" },
    { "id": "US.FST-01", "provider": "Fastly", "country": "", "thresholdBytes": 215899, "times": 1, "url": "https://www.jetblue.com/footer/footer-element-es2015.js" },
    { "id": "LU.GCORE-01", "provider": "Gcore", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://gcore.com/assets/fonts/Montserrat-Variable.woff2" },
    { "id": "US.GC-01", "provider": "Google Cloud", "country": "", "thresholdBytes": 521495, "times": 1, "url": "https://api.usercentrics.eu/gvl/v3/en.json" },
    { "id": "US.GC-02", "provider": "Google Cloud", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://widgets.reputation.com/fonts/Inter-Light.ttf" },
    { "id": "DE.HE-01", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://apiwhatsapp-1000.zapipro.com/libs/bootstrap/dist/css/bootstrap.min.css" },
    { "id": "DE.HE-02", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://www.industrialport.net/wp-content/uploads/custom-fonts/2022/10/Lato-Bold.ttf" },
    { "id": "FI.HE-01", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://251b5cd9.nip.io/1MB.bin" },
    { "id": "FI.HE-02", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://nioges.com/libs/fontawesome/webfonts/fa-solid-900.woff2" },
    { "id": "FI.HE-03", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://5fd8bdae.nip.io/1MB.bin" },
    { "id": "FI.HE-04", "provider": "Hetzner", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://5fd8bca5.nip.io/1MB.bin" },
    { "id": "US.MBCOM-01", "provider": "Melbicom", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://twin.mentat.su/assets/fonts/Inter-SemiBold.woff2" },
    { "id": "CO.OR-01", "provider": "Oracle", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://plataforma.trackerintl.com/images/background.jpg" },
    { "id": "SG.OR-01", "provider": "Oracle", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://global-seres.com.sg/wp-content/uploads/2024/02/SVG00732-scaled.jpg" },
    { "id": "FR.OVH-01", "provider": "OVH", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://testing.symarobot.com/content/images/logo.png" },
    { "id": "FR.OVH-02", "provider": "OVH", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://filmoteka.net.pl/css/bootstrap.min.css" },
    { "id": "NL.SW-01", "provider": "Scaleway", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://www.velivole.fr/img/header.jpg" },
    { "id": "DE.VLTR-01", "provider": "Vultr", "country": "", "thresholdBytes": 226114, "times": 1, "url": "https://static-cdn.play.date/static/js/model-viewer.min.js" },
    { "id": "US.VLTR-01", "provider": "Vultr", "country": "", "thresholdBytes": 65536, "times": 1, "url": "https://us.rudder.qntmnet.com/QN-CDN/images/qn_bg_.jpg" },
]

BUNDLED_HOST_SUITE = [
    { "id": "US.GH-HPRN", "provider": "Self check", "country": "", "host": "hyperion-cs.github.io" },
    { "id": "PL.AKM-01", "provider": "Akamai", "country": "", "host": "www.mobil.com.se" },
    { "id": "SE.AKM-01", "provider": "Akamai", "country": "", "host": "cdn.apple-mapkit.com" },
    { "id": "DE.AWS-01", "provider": "AWS", "country": "", "host": "vibersporocila.akton.si" },
    { "id": "US.AWS-01", "provider": "AWS", "country": "", "host": "corp.kaltura.com" },
    { "id": "US.CDN77-01", "provider": "CDN77", "country": "", "host": "cdn.eso.org" },
    { "id": "CA.CF-01", "provider": "Cloudflare", "country": "", "host": "hertzen.com" },
    { "id": "CA.CF-02", "provider": "Cloudflare", "country": "", "host": "justice.gov" },
    { "id": "US.CF-01", "provider": "Cloudflare", "country": "", "host": "img.wzstats.gg" },
    { "id": "US.CF-02", "provider": "Cloudflare", "country": "", "host": "esm.sh" },
    { "id": "FR.CNTB-01", "provider": "Contabo", "country": "", "host": "ctbnew.netmania.hu" },
    { "id": "FR.CNTB-02", "provider": "Contabo", "country": "", "host": "oddremedies.com" },
    { "id": "DE.DO-01", "provider": "DigitalOcean", "country": "", "host": "ui-arts.com" },
    { "id": "UK.DO-01", "provider": "DigitalOcean", "country": "", "host": "africa-s.org" },
    { "id": "UK.DO-02", "provider": "DigitalOcean", "country": "", "host": "admin.survey54.com" },
    { "id": "CA.FST-01", "provider": "Fastly", "country": "", "host": "ssl.p.jwpcdn.com" },
    { "id": "US.FST-01", "provider": "Fastly", "country": "", "host": "www.jetblue.com" },
    { "id": "US.FTBVM-01", "provider": "FT/BuyVM", "country": "", "host": "buyvm.net" },
    { "id": "US.FTBVM-02", "provider": "FT/BuyVM", "country": "", "host": "dmvideo.download" },
    { "id": "LU.GCORE-01", "provider": "Gcore", "country": "", "host": "gcore.com" },
    { "id": "US.GC-01", "provider": "Google Cloud", "country": "", "host": "api.usercentrics.eu" },
    { "id": "US.GC-02", "provider": "Google Cloud", "country": "", "host": "widgets.reputation.com" },
    { "id": "DE.HE-01", "provider": "Hetzner", "country": "", "host": "king.hr" },
    { "id": "DE.HE-02", "provider": "Hetzner", "country": "", "host": "mail.server.apaone.com" },
    { "id": "FI.HE-01", "provider": "Hetzner", "country": "", "host": "251b5cd9.nip.io" },
    { "id": "FI.HE-02", "provider": "Hetzner", "country": "", "host": "nioges.com" },
    { "id": "FI.HE-03", "provider": "Hetzner", "country": "", "host": "5fd8bdae.nip.io" },
    { "id": "FI.HE-04", "provider": "Hetzner", "country": "", "host": "5fd8bca5.nip.io" },
    { "id": "US.MBCOM-01", "provider": "Melbicom", "country": "", "host": "elecane.com" },
    { "id": "ES.OR-01", "provider": "Oracle", "country": "", "host": "sh00065.hostgator.com" },
    { "id": "SG.OR-01", "provider": "Oracle", "country": "", "host": "vps.inprodec.com" },
    { "id": "FR.OVH-01", "provider": "OVH", "country": "", "host": "www.adwin.fr" },
    { "id": "FR.OVH-02", "provider": "OVH", "country": "", "host": "www.emca.be" },
    { "id": "NL.SW-01", "provider": "Scaleway", "country": "", "host": "www.velivole.fr" },
    { "id": "DE.VLTR-01", "provider": "Vultr", "country": "", "host": "gertrud.tv" },
    { "id": "US.VLTR-01", "provider": "Vultr", "country": "", "host": "us.rudder.qntmnet.com" },
]

HELP_TEXT = """
Этот скрипт выполняет тесты с вашего компьютера для определения различных видов блокировок.

--- ОПИСАНИЕ ТЕСТОВ В ВЫВОДЕ ---

DNS:
  - Тест отправляет запросы к независимым публичным DNS-серверам (Cloudflare, Google).
  - "OK" означает, что IP-адрес успешно получен.
  - "Ошибка" говорит о невозможности получить IP. Это может быть как проблемой сети,
    так и признаком DNS-блокировки.

Локация:
  - Определяет страну и город, где предположительно находится сервер,
    на основе его IP-адреса через внешний гео-сервис.
  - В текущей GUI-версии в подробностях показывается именно текстовая локация,
    а не флаг или эмодзи.

TLS 1.3 / 1.2:
  - Проверяет возможность установить зашифрованное соединение с сервером, используя
    конкретную версию протокола TLS. Блокировка TLS 1.3 может указывать на то,
    что провайдер пытается понизить соединение до старой версии для анализа.

SSL:
  - Проверяет полное TLS-рукопожатие, включая проверку подлинности SSL-сертификата.
  - "OK" означает, что сертификат сайта подлинный и соединение установлено.
  - "Подмена сертификата" — явный признак атаки 'человек посередине' (MITM),
    часто используемой DPI для расшифровки и анализа HTTPS-трафика.
  - "Ошибка" (напр. ConnectionResetError) на этом этапе указывает на блокировку по IP
    или по имени сервера (SNI) на ранней стадии соединения.

HTTP:
  - После успешного TLS-соединения отправляется стандартный веб-запрос (HTTP GET).
  - "OK" c кодом 200-299 означает, что сервер успешно ответил.
  - "Ошибка" на этом этапе — классический признак DPI, который анализирует и
    блокирует трафик по его содержимому уже внутри "защищенного" канала.

DPI (16KB):
  - Специальный тест, который пытается скачать большой файл. Если загрузка обрывается
    на объеме около 16-24 КБ, это указывает на вид DPI-блокировки,
    разрывающей соединение после передачи небольшого объема данных.

--- ИНТЕРПРЕТАЦИЯ ИТОГОВЫХ ВЕРДИКТОВ ---

DNS-блокировка:
  - Не удалось получить IP-адрес домена. Возможно, домен не существует
    или его DNS-записи блокируются.

Блокировка по IP / SNI:
  - DNS-запрос успешен, но SSL-соединение было сброшено на самом раннем этапе.

Блокировка 'black-hole':
  - Запрос к серверу не завершился за отведенное время (таймаут). Трафик
    к заблокированному ресурсу просто отбрасывается без ответа.

Подмена SSL (DPI/MITM):
  - Соединение установлено, но SSL-сертификат не является доверенным.
    Явный признак атаки 'человек посередине' (MITM).

Блокировка по DPI (HTTP):
  - DNS и SSL-соединение прошли успешно, но последующий HTTP-запрос
    внутри защищенного канала был заблокирован.

DPI (разрыв при скачивании):
  - Выявлен специфический тип DPI, рвущий соединение при попытке скачать файл.

Доступен:
  - Все основные тесты (DNS, SSL, HTTP) прошли успешно.

--- ДОПОЛНЕНИЯ ДЛЯ ТЕКУЩЕЙ GUI-ВЕРСИИ ---

Мой список:
  - В пользовательский список можно добавлять не только домен, но и полный адрес.
  - Поддерживаются варианты:
      site.com
      site.com:771
      https://site.com
      http://site.com
      https://site.com/path/file.ext
  - Если протокол не указан, программа автоматически подставляет https://
  - Если указан путь до конкретного файла или страницы, проверка будет выполняться именно по этому адресу.

Что лучше добавлять в список:
  - Для обычной проверки доступности чаще всего лучше использовать сам домен или главную страницу сайта.
    Они обычно живут дольше и реже пропадают.
  - Адрес до конкретного файла тоже можно использовать, но такие ссылки со временем могут перестать работать,
    даже если сам сайт и домен остаются доступными.
  - Для стандартного списка приложение сначала пытается скачать актуальные test-suite списки из GitHub,
    а если это не удалось — использует встроенный резервный набор адресов из самого приложения.

Не проверено (HTTP ...):
  - Это не обязательно ошибка и не обязательно блокировка.
  - Такой статус означает, что HTTP-ответ получен, но код ответа оказался служебным
    или неподходящим для теста скачивания большого объема данных.
  - Например, сайт может вернуть редирект, страницу защиты, запрет доступа или другой нестандартный ответ.

Не проверено (<16 KB ...):
  - Сайт ответил успешно, но объем данных оказался слишком маленьким для теста DPI по разрыву скачивания.
  - Это нормальная ситуация для небольших страниц, коротких ответов, редиректов и легких сайтов.
  - Такой статус не означает, что сайт заблокирован.

Локация в текущей версии:
  - Приложение пытается определить страну и город сервера по IP-адресу.
  - В части случаев локация может не определиться, определиться не полностью
    или показываться приблизительно.
  - Это нормально для некоторых CDN, балансировщиков, Anycast-узлов и защитных сетей.

Стандартная проверка:
  - В начале списка добавлены пользовательские индикаторы доступности:
      Telegram Web
      YouTube
      WhatsApp Web
      GitHub
      Microsoft
  - Далее используется инфраструктурный список из GitHub test-suite.
  - Для DNS/TLS/SSL приложение берёт host,
    а для HTTP и DPI — url с реальным ресурсом.
  - Если удалённый список GitHub недоступен, используется встроенный fallback.

Подсказки в интерфейсе:
  - При наведении мыши на ячейки в таблице и на строки в подробностях приложение может показывать
    дополнительные пояснения по ошибкам, статусам и итоговому вердикту.
  - В таблице "Результаты" показываются только:
      Сайт
      Хост
      IP
      Вердикт
  - Все остальные этапы проверки вынесены в окно "Подробности".
""".strip()


@dataclass
class SiteResult:
    label: str
    site_id: str
    provider: str
    country: str
    url: str
    host: str
    dns_status: str
    dns_time: str
    ip: str
    location: str
    tls13_status: str
    tls12_status: str
    ssl_status: str
    ssl_time: str
    http_status: str
    http_time: str
    dpi_download_status: str
    verdict: str
    source_hint: str = ""
    order_index: int = 0


class ToolTip:
    def __init__(self, widget):
        self.widget = widget
        self.tipwindow = None
        self.text = ""

    def show(self, text, x, y):
        if not text:
            self.hide()
            return
        if self.tipwindow and self.text == text:
            try:
                self.tipwindow.geometry(f"+{x}+{y}")
            except tk.TclError:
                pass
            return
        self.hide()
        self.text = text
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = ttk.Label(
            tw,
            text=text,
            justify="left",
            background="#fff8dc",
            relief="solid",
            borderwidth=1,
            padding=(8, 6),
            wraplength=430,
        )
        label.pack()

    def hide(self):
        if self.tipwindow is not None:
            self.tipwindow.destroy()
            self.tipwindow = None
        self.text = ""


def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def get_user_sites_path() -> Path:
    return get_app_base_dir() / SITES_FILENAME


def normalize_url(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.hostname:
        raise ValueError("Не удалось определить домен. Введите адрес в формате site.com или https://site.com")
    return value


def load_user_sites() -> list[str]:
    path = get_user_sites_path()
    try:
        if not path.exists():
            path.write_text("", encoding="utf-8")
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        return []


def save_user_sites(sites: list[str]) -> None:
    path = get_user_sites_path()
    path.write_text("\n".join(sites) + ("\n" if sites else ""), encoding="utf-8")


def open_user_sites_file() -> None:
    path = get_user_sites_path()
    if not path.exists():
        path.write_text("", encoding="utf-8")
    if sys.platform.startswith("win"):
        os.startfile(str(path))
    elif sys.platform == "darwin":
        subprocess.Popen(["open", str(path)])
    else:
        subprocess.Popen(["xdg-open", str(path)])


def _http_get_json(url: str, timeout: int = 6):
    response = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.json()


def fetch_remote_standard_suite() -> tuple[list[dict], str]:
    try:
        url_suite = _http_get_json(REMOTE_SUITE_URL)
        host_suite = _http_get_json(REMOTE_HOSTS_URL)

        if not isinstance(url_suite, list) or not isinstance(host_suite, list):
            raise ValueError("Некорректный формат списка")

        url_by_id = {item.get("id"): dict(item) for item in url_suite if isinstance(item, dict) and item.get("id")}
        host_by_id = {item.get("id"): dict(item) for item in host_suite if isinstance(item, dict) and item.get("id")}

        merged: list[dict] = []
        for site_id, item in url_by_id.items():
            combined = dict(item)
            host_info = host_by_id.get(site_id, {})
            if host_info.get("host"):
                combined["host"] = host_info["host"]
            if host_info.get("country") and not combined.get("country"):
                combined["country"] = host_info["country"]
            merged.append(combined)

        # Добавим только host-only записи, если они не self-check.
        for site_id, item in host_by_id.items():
            if site_id in url_by_id:
                continue
            if item.get("provider", "").lower() == "self check":
                continue
            merged.append(
                {
                    "id": item.get("id", ""),
                    "provider": item.get("provider", "Сайт"),
                    "country": item.get("country", ""),
                    "host": item.get("host", ""),
                    "url": f"https://{item.get('host', '')}/" if item.get("host") else "",
                }
            )

        merged = [item for item in merged if item.get("host") or item.get("url")]
        merged.sort(key=lambda x: (x.get("provider", ""), x.get("id", "")))
        return PRIORITY_SITES + merged, "Стандартный список: GitHub test-suite"
    except Exception:
        return build_bundled_standard_suite(), "Стандартный список: встроенный fallback"


def build_bundled_standard_suite() -> list[dict]:
    url_by_id = {item.get("id"): dict(item) for item in BUNDLED_URL_SUITE}
    host_by_id = {item.get("id"): dict(item) for item in BUNDLED_HOST_SUITE}
    merged: list[dict] = []

    for site_id, item in url_by_id.items():
        combined = dict(item)
        host_info = host_by_id.get(site_id, {})
        if host_info.get("host"):
            combined["host"] = host_info["host"]
        if host_info.get("country") and not combined.get("country"):
            combined["country"] = host_info["country"]
        merged.append(combined)

    for site_id, item in host_by_id.items():
        if site_id in url_by_id or item.get("provider", "").lower() == "self check":
            continue
        merged.append(
            {
                "id": item.get("id", ""),
                "provider": item.get("provider", "Сайт"),
                "country": item.get("country", ""),
                "host": item.get("host", ""),
                "url": f"https://{item.get('host', '')}/" if item.get("host") else "",
            }
        )
    merged.sort(key=lambda x: (x.get("provider", ""), x.get("id", "")))
    return PRIORITY_SITES + merged


def test_dns(hostname: str):
    resolver = dns.resolver.Resolver()
    resolver.nameservers = ["1.1.1.1", "8.8.8.8"]
    resolver.lifetime = TIMEOUT
    start_time = time.monotonic()
    try:
        answers = resolver.resolve(hostname)
        ip_address = answers[0].to_text()
        duration = time.monotonic() - start_time
        return f"OK ({ip_address})", f"{duration:.3f} с", ip_address
    except Exception as exc:
        duration = time.monotonic() - start_time
        return f"Ошибка ({exc.__class__.__name__})", f"{duration:.3f} с", None


def get_ip_location(ip_address: str | None, country_hint: str = "") -> str:
    if not ip_address:
        return country_hint or "Не удалось определить"

    services = [
        (f"http://ip-api.com/json/{ip_address}?fields=status,country,regionName,city", ("country", "regionName", "city"), "status", "success"),
        (f"https://ipwho.is/{ip_address}", ("country", "region", "city"), "success", True),
    ]

    for url, fields, ok_key, ok_value in services:
        try:
            response = requests.get(url, timeout=5, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            data = response.json()
            if data.get(ok_key) != ok_value:
                continue
            parts = []
            for field in fields:
                value = data.get(field, "")
                if value and value not in parts:
                    parts.append(value)
            location = ", ".join(parts)
            if location:
                return location
        except Exception:
            continue

    return country_hint or "Не удалось определить"


def test_tls_version(host: str, ip: str | None, port: int, version_enum: ssl.TLSVersion) -> str:
    if not ip:
        return "Пропуск (нет IP)"
    context = ssl.create_default_context()
    context.minimum_version = version_enum
    context.maximum_version = version_enum
    try:
        with socket.create_connection((ip, port), timeout=TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=host):
                return "OK ✅"
    except Exception as exc:
        return f"Blocked ❌ ({exc.__class__.__name__})"


def test_ssl_handshake(host: str, ip: str | None, port: int):
    if not ip:
        return "Пропуск (нет IP)", "N/A"
    context = ssl.create_default_context()
    start_time = time.monotonic()
    try:
        with socket.create_connection((ip, port), timeout=TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=host):
                duration = time.monotonic() - start_time
                return "OK ✅", f"{duration:.3f} с"
    except ssl.SSLCertVerificationError:
        duration = time.monotonic() - start_time
        return "Подмена сертификата ❌", f"{duration:.3f} с"
    except Exception as exc:
        duration = time.monotonic() - start_time
        return f"Ошибка ({exc.__class__.__name__}) ❌", f"{duration:.3f} с"


def test_http_get(url: str):
    try:
        response = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}, allow_redirects=True)
        duration = response.elapsed.total_seconds()
        if 200 <= response.status_code <= 299:
            return f"OK ({response.status_code}) ✅", f"{duration:.3f} с"
        return f"OK ({response.status_code}) ⚠️", f"{duration:.3f} с"
    except requests.exceptions.RequestException as exc:
        return f"Ошибка ({exc.__class__.__name__}) ❌", "N/A"


def test_dpi_download(url: str, threshold_bytes: int = 65536):
    safe_threshold = max(threshold_bytes, 16 * 1024)
    try:
        with requests.get(url, stream=True, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT}, allow_redirects=True) as response:
            status_code = response.status_code
            if status_code >= 400:
                return f"Не проверено (HTTP {status_code})"

            total = 0
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                total += len(chunk)
                if total >= safe_threshold:
                    return "Not detected ✅"

            if total < 16 * 1024:
                return f"Не проверено (<16 KB, {total // 1024} KB)"
            if 16 * 1024 <= total <= 24 * 1024:
                return f"Detected❗️ ({total // 1024} KB)"
            return "Not detected ✅"
    except requests.exceptions.RequestException as exc:
        return f"Detected❗️ ({exc.__class__.__name__})"


def determine_verdict(results: dict[str, str]) -> str:
    ssl_text = results["ssl_status"]
    http_text = results["http_status"]
    dpi_text = results["dpi_download_status"]

    if "Ошибка" in results["dns_status"]:
        return "DNS-блокировка ❗️"
    if "Подмена сертификата" in ssl_text:
        return "Подмена SSL (DPI/MITM) ❗️"
    if "Timeout" in ssl_text or "ReadTimeout" in ssl_text:
        return "Блокировка 'black-hole' ❗️"
    if "Ошибка" in ssl_text:
        return "Блокировка по IP/SNI ❗️"
    if "Ошибка" in http_text:
        return "Блокировка по DPI (HTTP) ❗️"
    if dpi_text.startswith("Detected"):
        return "DPI (разрыв при скачивании) ❗️"
    if "Не проверено" in dpi_text:
        return "Доступен ✅ (DPI-тест ограничен)"
    return "Доступен ✅"


def build_label(item: dict, index: int, total: int) -> str:
    if isinstance(item, dict):
        provider = item.get("provider", "Сайт")
        site_id = item.get("id", f"SITE-{index + 1}")
        return f"[{site_id}] {provider}"
    return f"Сайт {index + 1}/{total}"


def run_full_test_on_url(item, index: int = 0, total: int = 1) -> SiteResult:
    if isinstance(item, dict):
        source_item = dict(item)
        url = source_item.get("url", "")
        site_id = source_item.get("id", "")
        provider = source_item.get("provider", "")
        country = source_item.get("country", "")
        host_override = source_item.get("host", "")
        threshold_bytes = int(source_item.get("thresholdBytes", 65536) or 65536)
        source_hint = source_item.get("source_hint", "")
    else:
        source_item = {}
        url = str(item)
        site_id = f"USER-{index + 1:02d}"
        provider = "Пользовательский сайт"
        country = ""
        host_override = ""
        threshold_bytes = 65536
        source_hint = ""

    parsed_url = urlparse(url)
    hostname = host_override or parsed_url.hostname or ""
    if not url and hostname:
        url = f"https://{hostname}/"
        parsed_url = urlparse(url)

    port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)

    results: dict[str, str] = {}
    results["dns_status"], results["dns_time"], ip = test_dns(hostname)
    results["location"] = get_ip_location(ip, country_hint=country)
    results["tls13_status"] = test_tls_version(hostname, ip, port, ssl.TLSVersion.TLSv1_3)
    results["tls12_status"] = test_tls_version(hostname, ip, port, ssl.TLSVersion.TLSv1_2)
    results["ssl_status"], results["ssl_time"] = test_ssl_handshake(hostname, ip, port)
    results["http_status"], results["http_time"] = test_http_get(url)
    results["dpi_download_status"] = test_dpi_download(url, threshold_bytes=threshold_bytes)
    results["verdict"] = determine_verdict(results)

    return SiteResult(
        label=build_label(source_item if isinstance(item, dict) else {"id": site_id, "provider": provider}, index, total),
        site_id=site_id,
        provider=provider,
        country=country,
        url=url,
        host=hostname,
        dns_status=results["dns_status"],
        dns_time=results["dns_time"],
        ip=ip or "N/A",
        location=results["location"],
        tls13_status=results["tls13_status"],
        tls12_status=results["tls12_status"],
        ssl_status=results["ssl_status"],
        ssl_time=results["ssl_time"],
        http_status=results["http_status"],
        http_time=results["http_time"],
        dpi_download_status=results["dpi_download_status"],
        verdict=results["verdict"],
        source_hint=source_hint,
        order_index=index,
    )


class DPIConnectivityApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"{APP_TITLE} ({APP_VERSION})")
        self.root.geometry("1220x820")
        self.root.minsize(980, 680)

        self.user_sites = load_user_sites()
        self.worker_thread = None
        self.stop_event = threading.Event()
        self.ui_queue: queue.Queue = queue.Queue()
        self.result_by_row: dict[str, SiteResult] = {}
        self.all_results: list[SiteResult] = []
        self.current_run_id = 0
        self.running_run_id: int | None = None

        self.stats_var = tk.StringVar(value="Готово.")
        self.path_var = tk.StringVar(value=f"Файл списка: {get_user_sites_path()}")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.show_only_issues = tk.BooleanVar(value=False)

        self.tree_tooltip = ToolTip(self.root)
        self.details_tooltip = ToolTip(self.root)

        self._build_ui()
        self._refresh_user_sites_box()
        self._poll_ui_queue()

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        self.paned = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        self.paned.grid(row=0, column=0, sticky="nsew")

        self.left_frame = ttk.Frame(self.paned, padding=12)
        self.right_frame = ttk.Frame(self.paned, padding=12)

        self.paned.add(self.left_frame, weight=0)
        self.paned.add(self.right_frame, weight=1)

        self._build_left_panel()
        self._build_right_panel()

        self.root.after(120, self._fix_initial_pane_width)

    def _fix_initial_pane_width(self):
        try:
            self.root.update_idletasks()
            left_width = max(self.left_frame.winfo_reqwidth() + 18, 300)
            self.paned.sashpos(0, left_width)
        except Exception:
            pass

    def _build_left_panel(self):
        lf = self.left_frame
        lf.columnconfigure(0, weight=1)
        lf.rowconfigure(2, weight=1)

        header = ttk.Label(lf, text=f"{APP_TITLE} {APP_VERSION}", font=("Segoe UI", 14, "bold"))
        header.grid(row=0, column=0, sticky="w", pady=(0, 10))

        section_main = ttk.LabelFrame(lf, text="Основные действия", padding=10)
        section_main.grid(row=1, column=0, sticky="ew")
        section_main.columnconfigure(0, weight=1)

        self.btn_standard = ttk.Button(section_main, text="Стандартная проверка", command=self.run_standard_suite)
        self.btn_standard.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        self.btn_my_list = ttk.Button(section_main, text="Мой список", command=self.run_user_suite)
        self.btn_my_list.grid(row=1, column=0, sticky="ew", pady=(0, 8))

        self.btn_stop = ttk.Button(section_main, text="Остановить тест", command=self.stop_tests)
        self.btn_stop.grid(row=2, column=0, sticky="ew")

        section_add = ttk.LabelFrame(lf, text="Работа с моим списком", padding=10)
        section_add.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        section_add.columnconfigure(0, weight=1)
        section_add.rowconfigure(7, weight=1)

        ttk.Label(section_add, text="Адрес сайта").grid(row=0, column=0, sticky="w")
        self.site_entry = ttk.Entry(section_add)
        self.site_entry.grid(row=1, column=0, sticky="ew", pady=(4, 6))
        self.site_entry.insert(0, "site.com")
        self.site_entry.bind("<FocusIn>", self._clear_placeholder)
        self.site_entry.bind("<FocusOut>", self._restore_placeholder)
        self.site_entry.bind("<Return>", lambda _event: self.add_and_check_site())

        ttk.Label(
            section_add,
            text="Можно вводить site.com, site.com:771, https://site.com или полный URL до файла/страницы",
            foreground="#666666",
            wraplength=280,
        ).grid(row=2, column=0, sticky="w", pady=(0, 8))

        self.btn_add_check = ttk.Button(section_add, text="Добавить и проверить", command=self.add_and_check_site)
        self.btn_add_check.grid(row=3, column=0, sticky="ew", pady=(0, 6))

        self.btn_open_file = ttk.Button(section_add, text="Открыть файл списка", command=self.open_sites_file)
        self.btn_open_file.grid(row=4, column=0, sticky="ew")

        ttk.Label(section_add, textvariable=self.path_var, wraplength=280, foreground="#666666").grid(
            row=5, column=0, sticky="w", pady=(8, 6)
        )

        ttk.Label(section_add, text="Сайты в списке:").grid(row=6, column=0, sticky="w")
        list_frame = ttk.Frame(section_add)
        list_frame.grid(row=7, column=0, sticky="nsew", pady=(2, 8))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self.user_sites_box = tk.Listbox(list_frame, height=8)
        self.user_sites_box.grid(row=0, column=0, sticky="nsew")
        user_scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.user_sites_box.yview)
        user_scroll.grid(row=0, column=1, sticky="ns")
        self.user_sites_box.configure(yscrollcommand=user_scroll.set)

        section_help = ttk.LabelFrame(lf, text="Справка", padding=10)
        section_help.grid(row=3, column=0, sticky="ew", pady=(12, 0))
        section_help.columnconfigure(0, weight=1)
        self.btn_help = ttk.Button(section_help, text="Открыть подробную справку", command=self.show_help_window)
        self.btn_help.grid(row=0, column=0, sticky="ew")

    def _build_right_panel(self):
        rf = self.right_frame
        rf.columnconfigure(0, weight=1)
        rf.rowconfigure(1, weight=1)
        rf.rowconfigure(2, weight=1)

        topbar = ttk.Frame(rf)
        topbar.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        topbar.columnconfigure(0, weight=1)
        topbar.columnconfigure(1, weight=0)

        ttk.Label(topbar, textvariable=self.stats_var, font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(
            topbar,
            text="Показать только проблемы",
            variable=self.show_only_issues,
            command=self._on_filter_toggle,
        ).grid(row=0, column=1, sticky="e", padx=(12, 0))
        ttk.Progressbar(topbar, variable=self.progress_var, maximum=100).grid(
            row=1, column=0, columnspan=2, sticky="ew", pady=(6, 0)
        )

        result_frame = ttk.LabelFrame(rf, text="Результаты", padding=8)
        result_frame.grid(row=1, column=0, sticky="nsew")
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        columns = ("label", "host", "ip", "verdict")
        self.tree = ttk.Treeview(result_frame, columns=columns, show="headings", height=14)
        self.tree.grid(row=0, column=0, sticky="nsew")

        headings = {
            "label": "Сайт",
            "host": "Хост",
            "ip": "IP",
            "verdict": "Вердикт",
        }
        widths = {
            "label": 250,
            "host": 220,
            "ip": 130,
            "verdict": 260,
        }

        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], minwidth=90, stretch=True, anchor="w")

        yscroll = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.tree.yview)
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.tag_configure("ok", foreground="#1f7a1f")
        self.tree.tag_configure("limited", foreground="#9a6b00")
        self.tree.tag_configure("issue", foreground="#b22222")

        self.tree.bind("<<TreeviewSelect>>", self._on_select_result)
        self.tree.bind("<Motion>", self._on_tree_motion)
        self.tree.bind("<Leave>", lambda _e: self.tree_tooltip.hide())

        details_frame = ttk.LabelFrame(rf, text="Подробности", padding=8)
        details_frame.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        details_frame.columnconfigure(0, weight=1)
        details_frame.rowconfigure(0, weight=1)

        self.details_text = tk.Text(details_frame, wrap="word", height=12)
        self.details_text.grid(row=0, column=0, sticky="nsew")
        self.details_text.configure(state="disabled")
        details_scroll = ttk.Scrollbar(details_frame, orient=tk.VERTICAL, command=self.details_text.yview)
        details_scroll.grid(row=0, column=1, sticky="ns")
        self.details_text.configure(yscrollcommand=details_scroll.set)
        self.details_text.bind("<Motion>", self._on_details_motion)
        self.details_text.bind("<Leave>", lambda _e: self.details_tooltip.hide())

        self._set_details_text("Выберите строку в таблице, чтобы увидеть подробности по сайту.")

    def _clear_placeholder(self, _event=None):
        if self.site_entry.get().strip() == "site.com":
            self.site_entry.delete(0, tk.END)

    def _restore_placeholder(self, _event=None):
        if not self.site_entry.get().strip():
            self.site_entry.insert(0, "site.com")

    def _refresh_user_sites_box(self):
        self.user_sites_box.delete(0, tk.END)
        for site in self.user_sites:
            self.user_sites_box.insert(tk.END, site)
        self.path_var.set(f"Файл списка: {get_user_sites_path()}")

    def _reset_results(self):
        self.all_results.clear()
        self.result_by_row.clear()
        for row_id in self.tree.get_children():
            self.tree.delete(row_id)
        self._set_details_text("Выберите строку в таблице, чтобы увидеть подробности по сайту.")
        self.progress_var.set(0)
        self.tree_tooltip.hide()
        self.details_tooltip.hide()

    def _set_details_text(self, text: str):
        self.details_text.configure(state="normal")
        self.details_text.delete("1.0", tk.END)
        self.details_text.insert("1.0", text)
        self.details_text.configure(state="disabled")

    def _row_tag_for_result(self, result: SiteResult) -> str:
        verdict = result.verdict.lower()
        if verdict.startswith("доступен ✅"):
            if "ограничен" in verdict:
                return "limited"
            return "ok"
        return "issue"

    def _matches_filter(self, result: SiteResult) -> bool:
        if not self.show_only_issues.get():
            return True
        return not result.verdict.lower().startswith("доступен ✅")

    def _render_result_row(self, result: SiteResult):
        values = (
            result.label,
            result.host,
            result.ip,
            result.verdict,
        )
        row_id = self.tree.insert("", tk.END, values=values, tags=(self._row_tag_for_result(result),))
        self.result_by_row[row_id] = result
        return row_id

    def _append_result(self, result: SiteResult):
        self.all_results.append(result)
        self.all_results.sort(key=lambda item: item.order_index)
        self._refresh_tree_from_results()

    def _refresh_tree_from_results(self):
        selected_result = None
        selected = self.tree.selection()
        if selected:
            selected_result = self.result_by_row.get(selected[0])

        for row_id in self.tree.get_children():
            self.tree.delete(row_id)
        self.result_by_row.clear()

        selected_row = None
        for result in self.all_results:
            if self._matches_filter(result):
                row_id = self._render_result_row(result)
                if selected_result is result:
                    selected_row = row_id

        if selected_row:
            self.tree.selection_set(selected_row)
            self.tree.see(selected_row)
        elif self.tree.get_children():
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self._on_select_result()
        else:
            self._set_details_text("Нет строк, подходящих под текущий фильтр.")

    def _format_result_details(self, result: SiteResult) -> str:
        source_line = f"Источник списка: {result.source_hint}\n" if result.source_hint else ""
        return (
            f"Метка:      {result.label}\n"
            f"ID:         {result.site_id}\n"
            f"Провайдер:  {result.provider or '—'}\n"
            f"URL:        {result.url}\n"
            f"Хост:       {result.host}\n"
            f"IP:         {result.ip}\n"
            f"Локация:    {result.location}\n"
            f"{source_line}\n"
            f"DNS:        {result.dns_status}, {result.dns_time}\n"
            f"TLS 1.3:    {result.tls13_status}\n"
            f"TLS 1.2:    {result.tls12_status}\n"
            f"SSL:        {result.ssl_status}, {result.ssl_time}\n"
            f"HTTP:       {result.http_status}, {result.http_time}\n"
            f"DPI (16KB): {result.dpi_download_status}\n\n"
            f"Вердикт:    {result.verdict}"
        )

    def _on_select_result(self, _event=None):
        selected = self.tree.selection()
        if not selected:
            return
        result = self.result_by_row.get(selected[0])
        if result:
            self._set_details_text(self._format_result_details(result))

    def _tooltip_verdict_text(self, result: SiteResult) -> str:
        verdict = result.verdict.lower()
        if "dns-блокировка" in verdict:
            return "Не удалось получить IP. Это похоже на проблему DNS или DNS-фильтрацию."
        if "подмена ssl" in verdict:
            return "Сертификат не доверенный. Возможна подмена сертификата или MITM."
        if "ip/sni" in verdict:
            return "DNS ответил, но SSL/TLS не установился. Похоже на блокировку по IP или SNI."
        if "dpi (разрыв" in verdict:
            return "Во время загрузки большого ответа соединение оборвалось. Это похоже на DPI-разрыв."
        if "dpi (http)" in verdict:
            return "TLS поднялся, но HTTP-запрос не прошёл. Это похоже на DPI по содержимому."
        if "black-hole" in verdict:
            return "Трафик к ресурсу, вероятно, молча отбрасывается без ответа."
        if "ограничен" in verdict:
            return "Базовая доступность подтверждена, но DPI-тест был неполным или неподходящим."
        if "доступен" in verdict:
            return "Сайт доступен: DNS, SSL и HTTP прошли успешно."
        return result.verdict

    def _explain_text(self, text: str) -> str:
        lowered = text.lower().strip()

        if not lowered:
            return ""

        # Явные успешные состояния сначала, чтобы не было ложных срабатываний на TLS/SSL.
        if lowered.startswith("tls 1.3:") or lowered.startswith("tls 1.2:"):
            if "ok" in lowered:
                return "Проверка конкретной версии TLS прошла успешно."
            if "blocked" in lowered:
                return self._explain_issue_line(lowered)
        if lowered.startswith("ssl:") and "ok" in lowered:
            return "TLS-рукопожатие и проверка сертификата прошли успешно."
        if lowered.startswith("dns:") and "ok" in lowered:
            return "Домен успешно разрешился в IP через публичные DNS-серверы."
        if lowered.startswith("http:") and "ok" in lowered:
            return "HTTP-ответ получен. На этом этапе сайт отвечает."
        if lowered.startswith("dpi (16kb):") and "not detected" in lowered:
            return "При скачивании не обнаружен характерный DPI-разрыв."

        return self._explain_issue_line(lowered)

    def _explain_issue_line(self, lowered: str) -> str:
        patterns = [
            (["nxdomain"], "Такой домен не найден через DNS."),
            (["lifetimeout", "dns timeout"], "DNS-сервер не ответил вовремя. Возможна фильтрация DNS-запросов."),
            (["timeout", "readtimeout", "connecttimeout"], "Сервер не ответил вовремя. Возможен black-hole или сильная фильтрация."),
            (["sslcertverificationerror", "подмена сертификата"], "Ошибка проверки сертификата. Возможна подмена сертификата или MITM."),
            (["ssl error", "sslerror", "ssl_error"], "Ошибка SSL/TLS. Соединение не удалось согласовать или защитить."),
            (["wrong version number"], "Сервер отверг согласование версии TLS."),
            (["eof occurred in violation of protocol"], "Соединение оборвалось во время TLS-рукопожатия."),
            (["certificate verify failed"], "Сертификат не прошёл проверку доверия."),
            (["connectionreseterror"], "Соединение было сброшено. Часто бывает при блокировке по IP/SNI или DPI."),
            (["connectionabortederror"], "Соединение было прервано до нормального завершения."),
            (["connectionrefusederror"], "Удалённый узел отверг подключение. Порт может быть закрыт или соединение фильтруется."),
            (["gaierror", "name or service not known"], "Имя узла не удалось преобразовать в IP. Проверь домен и DNS."),
            (["remote end closed connection", "remotedisconnected"], "Удалённая сторона закрыла соединение без полного ответа."),
            (["не проверено (http"], "Сайт ответил HTTP-кодом, который не подходит для DPI-проверки по скачиванию."),
            (["не проверено (<16 kb"], "Ответ получен, но он слишком маленький для DPI-проверки по объёму."),
            (["detected❗️", "chunkedencodingerror"], "Поток оборвался во время скачивания. Это похоже на DPI-разрыв или нестабильный маршрут."),
            (["blocked ❌"], "Соединение с этой версией TLS не удалось установить."),
            (["ok ⚠️"], "HTTP ответил, но код ответа не из 2xx. Базовая доступность есть, но ответ нестандартный."),
        ]
        for keys, explanation in patterns:
            if any(key in lowered for key in keys):
                return explanation
        return ""

    def _text_has_issue(self, text: str) -> bool:
        lowered = text.lower()
        success_markers = ["ok (", "ok ✅", "доступен ✅", "не проверено", "not detected"]
        issue_markers = [
            "ошибка", "подмена", "blocked", "detected", "timeout", "nxdomain",
            "connection", "black-hole", "mitm", "refused", "aborted", "closed",
            "ssl error", "sslerror",
        ]
        if any(marker in lowered for marker in issue_markers):
            return True
        if any(marker in lowered for marker in success_markers):
            return False
        return False

    def _tooltip_text_for_result_column(self, result: SiteResult, column_id: str) -> str:
        if column_id in {"#1", "#2", "#3", "#4"}:
            return self._tooltip_verdict_text(result)
        return ""

    def _on_tree_motion(self, event):
        row_id = self.tree.identify_row(event.y)
        column_id = self.tree.identify_column(event.x)
        if not row_id or not column_id:
            self.tree_tooltip.hide()
            return

        result = self.result_by_row.get(row_id)
        if not result:
            self.tree_tooltip.hide()
            return

        explanation = self._tooltip_text_for_result_column(result, column_id)
        if explanation:
            self.tree_tooltip.show(explanation, event.x_root + 14, event.y_root + 12)
        else:
            self.tree_tooltip.hide()

    def _on_details_motion(self, event):
        try:
            index = self.details_text.index(f"@{event.x},{event.y}")
            line_start = f"{index} linestart"
            line_end = f"{index} lineend"
            line_text = self.details_text.get(line_start, line_end).strip()
        except tk.TclError:
            self.details_tooltip.hide()
            return

        explanation = self._explain_text(line_text)
        if not explanation and line_text.startswith(("Метка:", "ID:", "Провайдер:", "URL:", "Хост:", "IP:", "Локация:", "Источник списка:")):
            selected = self.tree.selection()
            if selected:
                result = self.result_by_row.get(selected[0])
                if result:
                    explanation = self._tooltip_verdict_text(result)

        if not explanation and self._text_has_issue(line_text):
            explanation = line_text

        if explanation:
            self.details_tooltip.show(explanation, event.x_root + 14, event.y_root + 12)
        else:
            self.details_tooltip.hide()

    def _on_filter_toggle(self):
        self._refresh_tree_from_results()

    def show_help_window(self):
        win = tk.Toplevel(self.root)
        win.title("Подробная справка")
        win.geometry("900x760")
        win.minsize(700, 520)
        win.columnconfigure(0, weight=1)
        win.rowconfigure(0, weight=1)

        text = tk.Text(win, wrap="word")
        text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(win, orient=tk.VERTICAL, command=text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        text.configure(yscrollcommand=scroll.set)
        text.insert("1.0", HELP_TEXT)
        text.configure(state="disabled")

    def open_sites_file(self):
        try:
            open_user_sites_file()
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось открыть файл списка.\n\n{exc}")

    def add_and_check_site(self):
        raw = self.site_entry.get().strip()
        if not raw or raw == "site.com":
            messagebox.showinfo("Пустой ввод", "Введите адрес сайта, например: site.com")
            return
        try:
            url = normalize_url(raw)
        except ValueError as exc:
            messagebox.showerror("Некорректный адрес", str(exc))
            return

        if url not in self.user_sites:
            self.user_sites.append(url)
            save_user_sites(self.user_sites)
            self._refresh_user_sites_box()

        self.site_entry.delete(0, tk.END)
        self.site_entry.insert(0, "site.com")
        self._start_suite([url], "Проверка нового сайта", source_hint="Пользовательский список")

    def run_standard_suite(self):
        suite, source_hint = fetch_remote_standard_suite()
        for item in suite:
            if isinstance(item, dict):
                item["source_hint"] = source_hint
        self._start_suite(suite, "Стандартная проверка", source_hint=source_hint)

    def run_user_suite(self):
        self.user_sites = load_user_sites()
        self._refresh_user_sites_box()
        if not self.user_sites:
            messagebox.showinfo("Мой список", "Список пуст. Добавьте хотя бы один сайт.")
            return
        self._start_suite(self.user_sites, "Проверка сайтов из моего списка", source_hint="Пользовательский список")

    def stop_tests(self):
        if self.running_run_id is not None and self.worker_thread and self.worker_thread.is_alive():
            self.stop_event.set()
            stopped_run_id = self.running_run_id
            self.running_run_id = None
            self._set_running_state(False)
            self.stats_var.set("Остановка теста... Новые проверки отменены, результаты текущего прогона больше не будут добавляться.")
            self.ui_queue.put(("suite_cancelled", {"run_id": stopped_run_id}))
        else:
            self.stats_var.set("Нет активного теста.")

    def _set_running_state(self, is_running: bool):
        state_main = "disabled" if is_running else "normal"
        self.btn_standard.configure(state=state_main)
        self.btn_my_list.configure(state=state_main)
        self.btn_add_check.configure(state=state_main)
        self.site_entry.configure(state=state_main)
        self.btn_open_file.configure(state="normal")
        self.btn_help.configure(state="normal")
        self.btn_stop.configure(state="normal")

    def _start_suite(self, suite, title: str, source_hint: str = ""):
        if self.running_run_id is not None:
            messagebox.showwarning("Тест уже идет", "Сейчас уже выполняется тест. Сначала остановите его или дождитесь завершения.")
            return
        self._reset_results()
        self.current_run_id += 1
        run_id = self.current_run_id
        self.running_run_id = run_id
        self.stop_event = threading.Event()
        self._set_running_state(True)
        self.stats_var.set(f"{title}: подготовка...")
        self.worker_thread = threading.Thread(target=self._worker_run_suite, args=(suite, title, source_hint, run_id, self.stop_event), daemon=True)
        self.worker_thread.start()

    def _worker_run_suite(self, suite, title: str, source_hint: str, run_id: int, stop_event: threading.Event):
        total = len(suite)
        self.ui_queue.put(("suite_started", {"title": title, "total": total, "run_id": run_id}))
        completed = 0

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, total)))
        future_map = {
            executor.submit(run_full_test_on_url, item, index, total): (index, item)
            for index, item in enumerate(suite)
        }
        try:
            for future in concurrent.futures.as_completed(future_map):
                if stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    result = future.result()
                    if not result.source_hint:
                        result.source_hint = source_hint
                    completed += 1
                    self.ui_queue.put(("result", {"result": result, "completed": completed, "total": total, "title": title, "run_id": run_id}))
                except Exception as exc:
                    completed += 1
                    index, item = future_map[future]
                    label = build_label(item if isinstance(item, dict) else {"id": f"USER-{index+1:02d}", "provider": "Пользовательский сайт"}, index, total)
                    item_url = item["url"] if isinstance(item, dict) else str(item)
                    fallback = SiteResult(
                        label=label,
                        site_id=f"ERR-{index+1:02d}",
                        provider=item.get("provider", "") if isinstance(item, dict) else "Пользовательский сайт",
                        country=item.get("country", "") if isinstance(item, dict) else "",
                        url=item_url,
                        host=(item.get("host") if isinstance(item, dict) else None) or urlparse(item_url).hostname or "",
                        dns_status=f"Ошибка ({exc.__class__.__name__})",
                        dns_time="N/A",
                        ip="N/A",
                        location="Не удалось определить",
                        tls13_status="N/A",
                        tls12_status="N/A",
                        ssl_status=f"Ошибка ({exc.__class__.__name__}) ❌",
                        ssl_time="N/A",
                        http_status=f"Ошибка ({exc.__class__.__name__}) ❌",
                        http_time="N/A",
                        dpi_download_status=f"Detected❗️ ({exc.__class__.__name__})",
                        verdict="Ошибка выполнения ❗️",
                        source_hint=source_hint,
                        order_index=index,
                    )
                    self.ui_queue.put(("result", {"result": fallback, "completed": completed, "total": total, "title": title, "run_id": run_id}))
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            status = "stopped" if stop_event.is_set() else "done"
            self.ui_queue.put(("suite_finished", {"title": title, "completed": completed, "total": total, "status": status, "run_id": run_id}))

    def _poll_ui_queue(self):
        try:
            while True:
                kind, payload = self.ui_queue.get_nowait()
                if kind == "suite_started":
                    if payload.get("run_id") != self.running_run_id:
                        continue
                    total = payload["total"]
                    self.stats_var.set(f"{payload['title']}: 0 из {total}")
                    self.progress_var.set(0)
                elif kind == "result":
                    if payload.get("run_id") != self.running_run_id:
                        continue
                    result = payload["result"]
                    self._append_result(result)
                    completed = payload["completed"]
                    total = payload["total"]
                    self.stats_var.set(f"{payload['title']}: {completed} из {total}")
                    self.progress_var.set((completed / total) * 100 if total else 0)
                elif kind == "suite_cancelled":
                    continue
                elif kind == "suite_finished":
                    run_id = payload.get("run_id")
                    total = payload["total"]
                    completed = payload["completed"]
                    if run_id == self.running_run_id:
                        if payload["status"] == "stopped":
                            self.stats_var.set(f"{payload['title']}: остановлено ({completed} из {total})")
                        else:
                            self.stats_var.set(f"{payload['title']}: завершено ({completed} из {total})")
                        self.progress_var.set(100 if total and completed >= total else self.progress_var.get())
                        self._set_running_state(False)
                        self.running_run_id = None
                        if self.tree.get_children() and not self.tree.selection():
                            first = self.tree.get_children()[0]
                            self.tree.selection_set(first)
                            self.tree.see(first)
                            self._on_select_result()
                    else:
                        continue
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._poll_ui_queue)


def main():
    root = tk.Tk()
    try:
        root.iconname(APP_TITLE)
    except tk.TclError:
        pass
    DPIConnectivityApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
