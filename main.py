#!/usr/bin/env python3
import concurrent.futures
import ipaddress
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

APP_TITLE = "DPI & Connectivity Tester"
APP_VERSION = "v5.7"
TIMEOUT = 10
MAX_WORKERS = 4
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
SITES_FILENAME = "user_sites.txt"

COUNTRY_CODE_TO_NAME = {
    "CA": "Канада",
    "CO": "Колумбия",
    "DE": "Германия",
    "ES": "Испания",
    "FI": "Финляндия",
    "FR": "Франция",
    "LU": "Люксембург",
    "NL": "Нидерланды",
    "PL": "Польша",
    "SE": "Швеция",
    "SG": "Сингапур",
    "UK": "Великобритания",
    "US": "США",
}


def normalize_country_name(country_value: str) -> str:
    value = (country_value or "").strip()
    if not value:
        return ""
    upper_value = value.upper()
    if len(upper_value) == 2:
        return COUNTRY_CODE_TO_NAME.get(upper_value, upper_value)
    return value


def infer_country_from_site_id(site_id: str) -> str:
    prefix = (site_id or "").split(".", 1)[0].strip().upper()
    if len(prefix) == 2:
        return COUNTRY_CODE_TO_NAME.get(prefix, prefix)
    return ""


def normalize_location_text(location_value: str, country_hint: str = "") -> str:
    value = (location_value or "").strip()
    if not value:
        normalized_hint = normalize_country_name(country_hint)
        return normalized_hint or ""

    parts = []
    for part in value.split(","):
        cleaned = part.strip()
        if not cleaned:
            continue
        if len(cleaned) == 2 and cleaned.upper() in COUNTRY_CODE_TO_NAME:
            cleaned = COUNTRY_CODE_TO_NAME[cleaned.upper()]
        parts.append(cleaned)

    normalized = ", ".join(parts).strip()
    if len(normalized) == 2 and normalized.upper() in COUNTRY_CODE_TO_NAME:
        normalized = COUNTRY_CODE_TO_NAME[normalized.upper()]

    if normalized:
        return normalized

    normalized_hint = normalize_country_name(country_hint)
    return normalized_hint or ""

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

FakeIP / DNS-прокси:
  - Если DNS возвращает IP из специальных диапазонов (например 198.18.x.x),
    это обычно означает работу FakeIP, DNS-прокси или VPN-клиента.
  - В таком режиме могут быть неточными IP-адрес, локация и часть прямых проверок по IP.
  - Если при этом SSL и HTTP прошли успешно, сайт считается доступным.

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
    http_host: str = ""
    http_ip: str = ""
    notes: str = ""


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
            combined["country"] = normalize_country_name(combined.get("country", "") or infer_country_from_site_id(site_id))
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
                    "country": normalize_country_name(item.get("country", "") or infer_country_from_site_id(site_id)),
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
        combined["country"] = normalize_country_name(combined.get("country", "") or infer_country_from_site_id(site_id))
        merged.append(combined)

    for site_id, item in host_by_id.items():
        if site_id in url_by_id or item.get("provider", "").lower() == "self check":
            continue
        merged.append(
            {
                "id": item.get("id", ""),
                "provider": item.get("provider", "Сайт"),
                "country": normalize_country_name(item.get("country", "") or infer_country_from_site_id(site_id)),
                "host": item.get("host", ""),
                "url": f"https://{item.get('host', '')}/" if item.get("host") else "",
            }
        )
    merged.sort(key=lambda x: (x.get("provider", ""), x.get("id", "")))
    return PRIORITY_SITES + merged




LOCAL_DOMAIN_SUFFIXES = (".lan", ".local", ".home", ".internal", ".arpa")


def is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address((value or "").strip())
        return True
    except ValueError:
        return False


def host_looks_public(hostname: str) -> bool:
    host = (hostname or "").strip().rstrip(".").lower()
    if not host or is_ip_literal(host):
        return False
    if "." not in host:
        return False
    if host.endswith(LOCAL_DOMAIN_SUFFIXES):
        return False
    return True


def is_special_ip_for_public_host(ip_value: str | None, hostname: str) -> bool:
    if not ip_value or not host_looks_public(hostname):
        return False
    try:
        return not ipaddress.ip_address(ip_value).is_global
    except ValueError:
        return False


def choose_preferred_ip(ip_values: list[str]) -> str | None:
    if not ip_values:
        return None

    unique_ips: list[str] = []
    for ip_value in ip_values:
        if ip_value and ip_value not in unique_ips:
            unique_ips.append(ip_value)

    global_ipv4: list[str] = []
    global_ipv6: list[str] = []
    other_ips: list[str] = []

    for ip_value in unique_ips:
        try:
            parsed = ipaddress.ip_address(ip_value)
        except ValueError:
            continue
        if parsed.is_global:
            if parsed.version == 4:
                global_ipv4.append(ip_value)
            else:
                global_ipv6.append(ip_value)
        else:
            other_ips.append(ip_value)

    return (global_ipv4 or global_ipv6 or other_ips or [None])[0]


def resolve_with_resolver(hostname: str, nameservers: list[str] | None = None) -> tuple[list[str], list[str]]:
    resolver = dns.resolver.Resolver()
    resolver.lifetime = TIMEOUT
    resolver.timeout = min(TIMEOUT, 5)
    if nameservers:
        resolver.nameservers = nameservers

    ips: list[str] = []
    errors: list[str] = []

    for record_type in ("A", "AAAA"):
        try:
            answers = resolver.resolve(hostname, record_type)
            for answer in answers:
                value = answer.to_text().strip()
                if value and value not in ips:
                    ips.append(value)
        except Exception as exc:
            errors.append(exc.__class__.__name__)

    return ips, errors


def resolve_with_system_dns(hostname: str) -> tuple[list[str], list[str]]:
    ips: list[str] = []
    errors: list[str] = []
    try:
        info = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
        for item in info:
            sockaddr = item[4]
            if not sockaddr:
                continue
            ip_value = sockaddr[0]
            if ip_value and ip_value not in ips:
                ips.append(ip_value)
    except Exception as exc:
        errors.append(exc.__class__.__name__)
    return ips, errors


def build_http_target(parsed_url) -> str:
    target = parsed_url.path or "/"
    if parsed_url.params:
        target = f"{target};{parsed_url.params}"
    if parsed_url.query:
        target = f"{target}?{parsed_url.query}"
    return target or "/"


def build_host_header(hostname: str, scheme: str, port: int) -> str:
    default_port = 443 if scheme == "https" else 80
    if port == default_port:
        return hostname
    return f"{hostname}:{port}"


def read_http_response_headers(sock: socket.socket) -> tuple[int, dict[str, str], bytes]:
    buffer = b""
    max_header_size = 64 * 1024

    while b"\r\n\r\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buffer += chunk
        if len(buffer) > max_header_size:
            raise ValueError("HeaderTooLarge")

    if b"\r\n\r\n" not in buffer:
        raise ValueError("IncompleteHeaders")

    header_bytes, remainder = buffer.split(b"\r\n\r\n", 1)
    header_text = header_bytes.decode("iso-8859-1", errors="replace")
    lines = header_text.split("\r\n")
    if not lines:
        raise ValueError("EmptyStatusLine")

    status_line = lines[0]
    parts = status_line.split(" ", 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise ValueError("InvalidStatusLine")

    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    status_code = int(parts[1])
    return status_code, headers, remainder


def open_http_socket(ip_value: str, port: int, hostname: str, use_ssl: bool):
    raw_sock = socket.create_connection((ip_value, port), timeout=TIMEOUT)
    raw_sock.settimeout(TIMEOUT)
    if not use_ssl:
        return raw_sock, raw_sock

    context = ssl.create_default_context()
    wrapped_sock = context.wrap_socket(raw_sock, server_hostname=hostname)
    wrapped_sock.settimeout(TIMEOUT)
    return raw_sock, wrapped_sock


def classify_http_exception(exc: Exception) -> tuple[str, str]:
    name = exc.__class__.__name__
    dpi_like_errors = {
        "ConnectionResetError",
        "ConnectionAbortedError",
        "RemoteDisconnected",
        "SSLEOFError",
        "SSLError",
        "BrokenPipeError",
    }
    timeout_like_errors = {
        "TimeoutError",
        "socket.timeout",
        "Timeout",
        "ReadTimeout",
    }

    if name in dpi_like_errors:
        return name, "dpi_like"
    if name in timeout_like_errors or isinstance(exc, (TimeoutError, socket.timeout)):
        return name, "timeout"
    if isinstance(exc, ssl.SSLCertVerificationError):
        return name, "cert"
    return name, "other"


def test_dns(hostname: str) -> dict:
    start_time = time.monotonic()

    public_ips, public_errors = resolve_with_resolver(hostname, nameservers=["1.1.1.1", "8.8.8.8"])
    ip_address = choose_preferred_ip(public_ips)
    resolver_source = "public"
    system_fallback_used = False

    if not ip_address:
        system_ips, system_errors = resolve_with_system_dns(hostname)
        ip_address = choose_preferred_ip(system_ips)
        if ip_address:
            resolver_source = "system"
            system_fallback_used = True
            public_errors.extend(system_errors)
        else:
            errors = public_errors + system_errors
            duration = time.monotonic() - start_time
            error_name = errors[0] if errors else "DNSFailure"
            return {
                "text": f"Ошибка ({error_name})",
                "time": f"{duration:.3f} с",
                "ip": None,
                "ok": False,
                "resolver_source": "none",
                "system_fallback_used": False,
                "fake_ip": False,
                "error": error_name,
            }

    duration = time.monotonic() - start_time
    fake_ip = is_special_ip_for_public_host(ip_address, hostname)
    if system_fallback_used:
        text = f"OK ({ip_address}) ⚠️"
    else:
        text = f"OK ({ip_address})"

    if fake_ip:
        text = f"{text} ⚠️ FakeIP/спецдиапазон"

    return {
        "text": text,
        "time": f"{duration:.3f} с",
        "ip": ip_address,
        "ok": True,
        "resolver_source": resolver_source,
        "system_fallback_used": system_fallback_used,
        "fake_ip": fake_ip,
        "error": None,
    }


def test_tls_version(host: str, ip: str | None, port: int, version_enum: ssl.TLSVersion, enabled: bool = True) -> dict:
    if not enabled:
        return {"text": "Не применяется (HTTP URL)", "ok": False, "skipped": True, "error": None}
    if not ip:
        return {"text": "Пропуск (нет IP)", "ok": False, "skipped": True, "error": "NoIP"}

    context = ssl.create_default_context()
    context.minimum_version = version_enum
    context.maximum_version = version_enum

    try:
        with socket.create_connection((ip, port), timeout=TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=host):
                return {"text": "OK ✅", "ok": True, "skipped": False, "error": None}
    except Exception as exc:
        error_name, _ = classify_http_exception(exc)
        return {"text": f"Blocked ❌ ({error_name})", "ok": False, "skipped": False, "error": error_name}


def test_ssl_handshake(host: str, ip: str | None, port: int, enabled: bool = True) -> dict:
    if not enabled:
        return {"text": "Не применяется (HTTP URL)", "time": "N/A", "ok": False, "skipped": True, "error": None, "cert_mitm": False, "timeout": False}
    if not ip:
        return {"text": "Пропуск (нет IP)", "time": "N/A", "ok": False, "skipped": True, "error": "NoIP", "cert_mitm": False, "timeout": False}

    context = ssl.create_default_context()
    start_time = time.monotonic()
    try:
        with socket.create_connection((ip, port), timeout=TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=host):
                duration = time.monotonic() - start_time
                return {"text": "OK ✅", "time": f"{duration:.3f} с", "ok": True, "skipped": False, "error": None, "cert_mitm": False, "timeout": False}
    except ssl.SSLCertVerificationError as exc:
        duration = time.monotonic() - start_time
        return {"text": "Подмена сертификата ❌", "time": f"{duration:.3f} с", "ok": False, "skipped": False, "error": exc.__class__.__name__, "cert_mitm": True, "timeout": False}
    except Exception as exc:
        duration = time.monotonic() - start_time
        error_name, category = classify_http_exception(exc)
        return {
            "text": f"Ошибка ({error_name}) ❌",
            "time": f"{duration:.3f} с",
            "ok": False,
            "skipped": False,
            "error": error_name,
            "cert_mitm": False,
            "timeout": category == "timeout",
        }


def test_http_get(url: str, host: str, ip: str | None, port: int, use_ssl: bool) -> dict:
    if not ip:
        return {"text": "Ошибка (NoIP) ❌", "time": "N/A", "ok": False, "accessible": False, "status_code": None, "error": "NoIP", "category": "other"}

    parsed_url = urlparse(url)
    request_target = build_http_target(parsed_url)
    host_header = build_host_header(host, parsed_url.scheme or ("https" if use_ssl else "http"), port)
    request_bytes = (
        f"GET {request_target} HTTP/1.1\r\n"
        f"Host: {host_header}\r\n"
        f"User-Agent: {USER_AGENT}\r\n"
        "Accept: */*\r\n"
        "Accept-Encoding: identity\r\n"
        "Connection: close\r\n\r\n"
    ).encode("utf-8")

    start_time = time.monotonic()
    raw_sock = None
    io_sock = None
    try:
        raw_sock, io_sock = open_http_socket(ip, port, host, use_ssl)
        io_sock.sendall(request_bytes)
        status_code, _headers, _remainder = read_http_response_headers(io_sock)
        duration = time.monotonic() - start_time
        if 200 <= status_code <= 299:
            text = f"OK ({status_code}) ✅"
        else:
            text = f"OK ({status_code}) ⚠️"
        return {
            "text": text,
            "time": f"{duration:.3f} с",
            "ok": True,
            "accessible": True,
            "status_code": status_code,
            "error": None,
            "category": "ok",
        }
    except Exception as exc:
        duration = time.monotonic() - start_time
        error_name, category = classify_http_exception(exc)
        return {
            "text": f"Ошибка ({error_name}) ❌",
            "time": f"{duration:.3f} с" if duration > 0 else "N/A",
            "ok": False,
            "accessible": False,
            "status_code": None,
            "error": error_name,
            "category": category,
        }
    finally:
        try:
            if io_sock and io_sock is not raw_sock:
                io_sock.close()
        except Exception:
            pass
        try:
            if raw_sock:
                raw_sock.close()
        except Exception:
            pass


def test_dpi_download(url: str, host: str, ip: str | None, port: int, use_ssl: bool, threshold_bytes: int = 65536) -> dict:
    if not ip:
        return {"text": "Не проверено (нет IP)", "detected": False, "limited": True, "category": "no_ip"}

    safe_threshold = max(int(threshold_bytes or 65536), 16 * 1024)
    parsed_url = urlparse(url)
    request_target = build_http_target(parsed_url)
    host_header = build_host_header(host, parsed_url.scheme or ("https" if use_ssl else "http"), port)
    request_bytes = (
        f"GET {request_target} HTTP/1.1\r\n"
        f"Host: {host_header}\r\n"
        f"User-Agent: {USER_AGENT}\r\n"
        "Accept: */*\r\n"
        "Accept-Encoding: identity\r\n"
        "Connection: close\r\n\r\n"
    ).encode("utf-8")

    raw_sock = None
    io_sock = None
    total = 0
    try:
        raw_sock, io_sock = open_http_socket(ip, port, host, use_ssl)
        io_sock.sendall(request_bytes)
        status_code, _headers, remainder = read_http_response_headers(io_sock)

        if status_code >= 400:
            return {"text": f"Не проверено (HTTP {status_code})", "detected": False, "limited": True, "category": "http_status"}

        total = len(remainder)
        while total < safe_threshold:
            chunk = io_sock.recv(8192)
            if not chunk:
                break
            total += len(chunk)

        if total >= safe_threshold:
            return {"text": "Not detected ✅", "detected": False, "limited": False, "category": "ok"}
        if total < 16 * 1024:
            return {"text": f"Не проверено (<16 KB, {total // 1024} KB)", "detected": False, "limited": True, "category": "too_small"}
        if 16 * 1024 <= total <= 24 * 1024:
            return {"text": f"Detected❗️ ({total // 1024} KB)", "detected": True, "limited": False, "category": "size_window"}
        return {"text": "Not detected ✅", "detected": False, "limited": False, "category": "ok"}
    except Exception as exc:
        error_name, category = classify_http_exception(exc)
        if total >= 16 * 1024:
            return {"text": f"Detected❗️ ({error_name})", "detected": True, "limited": False, "category": category}
        return {"text": f"Не проверено ({error_name})", "detected": False, "limited": True, "category": category}
    finally:
        try:
            if io_sock and io_sock is not raw_sock:
                io_sock.close()
        except Exception:
            pass
        try:
            if raw_sock:
                raw_sock.close()
        except Exception:
            pass


def determine_verdict(results: dict) -> str:
    dns_result = results["dns"]
    ssl_result = results["ssl"]
    http_result = results["http"]
    dpi_result = results["dpi"]
    mixed_hosts = results.get("mixed_hosts", False)

    if not dns_result["ok"]:
        return "DNS-блокировка ❗️"
    if ssl_result["cert_mitm"]:
        return "Подмена SSL (DPI/MITM) ❗️"
    if ssl_result["timeout"]:
        return "Блокировка 'black-hole' ❗️"
    if not ssl_result["skipped"] and not ssl_result["ok"]:
        if mixed_hosts and http_result["ok"]:
            return "Частичная проблема (DNS/TLS probe) ❗️"
        return "Блокировка по IP/SNI ❗️"
    if not http_result["ok"]:
        if http_result["category"] == "dpi_like":
            return "Возможная блокировка по DPI (HTTP) ❗️"
        if http_result["category"] == "timeout":
            return "Ошибка HTTP / таймаут ❗️"
        return "Ошибка HTTP ❗️"
    if dpi_result["detected"]:
        return "DPI (разрыв при скачивании) ❗️"
    if dpi_result["limited"]:
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
        country = normalize_country_name(source_item.get("country", "") or infer_country_from_site_id(site_id))
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
    url_host = parsed_url.hostname or ""
    probe_host = host_override or url_host
    http_host = url_host or probe_host

    if not url and probe_host:
        url = f"https://{probe_host}/"
        parsed_url = urlparse(url)
        http_host = parsed_url.hostname or probe_host

    scheme = (parsed_url.scheme or "https").lower()
    is_https = scheme == "https"
    port = parsed_url.port or (443 if is_https else 80)

    dns_result = test_dns(probe_host)
    probe_ip = dns_result["ip"]
    mixed_hosts = bool(http_host and probe_host and http_host != probe_host)

    if mixed_hosts:
        http_dns_result = test_dns(http_host)
        http_ip = http_dns_result["ip"]
    else:
        http_dns_result = dns_result
        http_ip = probe_ip

    location_text = normalize_location_text(get_ip_location(probe_ip, country_hint=country), country_hint=country)
    tls13_result = test_tls_version(probe_host, probe_ip, port, ssl.TLSVersion.TLSv1_3, enabled=is_https)
    tls12_result = test_tls_version(probe_host, probe_ip, port, ssl.TLSVersion.TLSv1_2, enabled=is_https)
    ssl_result = test_ssl_handshake(probe_host, probe_ip, port, enabled=is_https)
    http_result = test_http_get(url, http_host, http_ip, port, use_ssl=is_https)
    dpi_result = test_dpi_download(url, http_host, http_ip, port, use_ssl=is_https, threshold_bytes=threshold_bytes)

    notes: list[str] = []
    if dns_result["system_fallback_used"]:
        notes.append("Публичный DNS не ответил, использован системный DNS.")
    if dns_result["fake_ip"]:
        notes.append("DNS вернул IP из спецдиапазона. Это может делать неточными IP, локацию и часть прямых проверок по IP, но не отменяет успешный HTTP/SSL.")
    if mixed_hosts:
        notes.append(f"DNS/TLS/SSL проверялись по хосту {probe_host}, HTTP/DPI — по хосту {http_host}.")
        if not http_dns_result["ok"]:
            notes.append(f"HTTP-хост не удалось разрешить: {http_dns_result['text']}.")

    result_meta = {
        "dns": dns_result,
        "ssl": ssl_result,
        "http": http_result,
        "dpi": dpi_result,
        "mixed_hosts": mixed_hosts,
    }

    return SiteResult(
        label=build_label(source_item if isinstance(item, dict) else {"id": site_id, "provider": provider}, index, total),
        site_id=site_id,
        provider=provider,
        country=country,
        url=url,
        host=probe_host,
        dns_status=dns_result["text"],
        dns_time=dns_result["time"],
        ip=probe_ip or "N/A",
        location=location_text,
        tls13_status=tls13_result["text"],
        tls12_status=tls12_result["text"],
        ssl_status=ssl_result["text"],
        ssl_time=ssl_result["time"],
        http_status=http_result["text"],
        http_time=http_result["time"],
        dpi_download_status=dpi_result["text"],
        verdict=determine_verdict(result_meta),
        source_hint=source_hint,
        order_index=index,
        http_host=http_host,
        http_ip=http_ip or "N/A",
        notes=" ".join(notes).strip(),
    )


def get_ip_location(ip_address: str | None, country_hint: str = "") -> str:

    normalized_hint = normalize_country_name(country_hint)

    if not ip_address:
        return normalized_hint or "Не удалось определить"

    services = [
        (f"http://ip-api.com/json/{ip_address}?fields=status,country,regionName,city,countryCode", ("country", "countryCode", "regionName", "city"), "status", "success"),
        (f"https://ipwho.is/{ip_address}", ("country", "country_code", "region", "city"), "success", True),
    ]

    for url, fields, ok_key, ok_value in services:
        try:
            response = requests.get(url, timeout=5, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            data = response.json()
            if data.get(ok_key) != ok_value:
                continue

            parts = []
            country_value = ""
            for field in fields:
                value = str(data.get(field, "") or "").strip()
                if not value:
                    continue

                if field.lower() in {"country", "countrycode", "country_code"}:
                    normalized_country = normalize_country_name(value)
                    if normalized_country:
                        country_value = normalized_country
                    continue

                if value not in parts:
                    parts.append(value)

            if country_value:
                location = ", ".join([country_value] + parts)
            else:
                location = ", ".join(parts)

            location = normalize_location_text(location, country_hint=normalized_hint)
            if location:
                return location
        except Exception:
            continue

    return normalized_hint or "Не удалось определить"


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
        if "⚠️" in result.verdict or "частичная проблема" in verdict:
            return "limited"
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
        http_host_line = ""
        http_ip_line = ""
        notes_line = f"Примечание: {result.notes}\n" if result.notes else ""

        if result.http_host and result.http_host != result.host:
            http_host_line = f"HTTP-хост:  {result.http_host}\n"
        if result.http_ip and result.http_ip not in {"", "N/A"} and result.http_ip != result.ip:
            http_ip_line = f"HTTP-IP:    {result.http_ip}\n"

        return (
            f"Метка:      {result.label}\n"
            f"ID:         {result.site_id}\n"
            f"Провайдер:  {result.provider or '—'}\n"
            f"URL:        {result.url}\n"
            f"Хост:       {result.host}\n"
            f"{http_host_line}"
            f"IP:         {result.ip}\n"
            f"{http_ip_line}"
            f"Локация:    {normalize_location_text(result.location, country_hint=result.country)}\n"
            f"{source_line}"
            f"{notes_line}\n"
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
        if "fakeip" in verdict or "dns-прокси" in verdict:
            return "Домен разрешился в спецдиапазон или через DNS-прокси. Результат проверки может быть искажен VPN, FakeIP или локальным DNS."
        if "dns-блокировка" in verdict:
            return "Не удалось получить IP. Это похоже на проблему DNS или DNS-фильтрацию."
        if "подмена ssl" in verdict:
            return "Сертификат не доверенный. Возможна подмена сертификата или MITM."
        if "частичная проблема" in verdict:
            return "Один probe-хост не прошёл DNS/TLS-проверку, но HTTP-ресурс при этом отвечает. Проверьте, что в наборе не используются разные хосты."
        if "ip/sni" in verdict:
            return "DNS ответил, но SSL/TLS не установился. Похоже на блокировку по IP или SNI."
        if "dpi (разрыв" in verdict:
            return "Во время загрузки большого ответа соединение оборвалось. Это похоже на DPI-разрыв."
        if "возможная блокировка по dpi" in verdict:
            return "HTTP-соединение оборвалось уже после установки канала. Это похоже на DPI или принудительный сброс."
        if "ошибка http / таймаут" in verdict:
            return "HTTP-запрос не завершился вовремя. Это может быть таймаут маршрута, black-hole или нестабильная сеть."
        if "ошибка http" in verdict:
            return "HTTP-запрос завершился ошибкой, но по одному этому признаку нельзя уверенно назвать это DPI."
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
            (["fakeip", "спецдиапазон"], "Домен разрешился в специальный IP-диапазон. Часто это означает FakeIP, DNS-прокси или влияние VPN."),
            (["nxdomain"], "Такой домен не найден через DNS."),
            (["lifetimeout", "dns timeout"], "DNS-сервер не ответил вовремя. Возможна фильтрация DNS-запросов."),
            (["публичный dns не ответил"], "Публичный DNS не ответил, поэтому приложение перешло на системный DNS."),
            (["timeout", "readtimeout", "connecttimeout"], "Сервер не ответил вовремя. Возможен black-hole или сильная фильтрация."),
            (["sslcertverificationerror", "подмена сертификата"], "Ошибка проверки сертификата. Возможна подмена сертификата или MITM."),
            (["ssl error", "sslerror", "ssl_error"], "Ошибка SSL/TLS. Соединение не удалось согласовать или защитить."),
            (["wrong version number"], "Сервер отверг согласование версии TLS."),
            (["eof occurred in violation of protocol", "ssleoferror"], "Соединение оборвалось во время TLS-рукопожатия или уже после него."),
            (["certificate verify failed"], "Сертификат не прошёл проверку доверия."),
            (["connectionreseterror"], "Соединение было сброшено. Часто бывает при блокировке по IP/SNI или DPI."),
            (["connectionabortederror"], "Соединение было прервано до нормального завершения."),
            (["connectionrefusederror"], "Удалённый узел отверг подключение. Порт может быть закрыт или соединение фильтруется."),
            (["gaierror", "name or service not known"], "Имя узла не удалось преобразовать в IP. Проверь домен и DNS."),
            (["remote end closed connection", "remotedisconnected"], "Удалённая сторона закрыла соединение без полного ответа."),
            (["не проверено (http"], "Сайт ответил HTTP-кодом, который не подходит для DPI-проверки по скачиванию."),
            (["не проверено (<16 kb"], "Ответ получен, но он слишком маленький для DPI-проверки по объёму."),
            (["не проверено (нет ip)"], "DPI-проверка пропущена, потому что IP для HTTP-хоста не был получен."),
            (["detected❗️"], "Поток оборвался во время скачивания или размер ответа попал в характерное окно 16–24 КБ."),
            (["blocked ❌"], "Соединение с этой версией TLS не удалось установить."),
            (["ok ⚠️"], "HTTP ответил, но код ответа не из 2xx. Базовая доступность есть, но ответ нестандартный."),
        ]
        for keys, explanation in patterns:
            if any(key in lowered for key in keys):
                return explanation
        return ""

    def _text_has_issue(self, text: str) -> bool:
        lowered = text.lower()
        success_markers = ["ok (", "ok ✅", "доступен ✅", "не проверено", "not detected", "не применяется"]
        issue_markers = [
            "ошибка", "подмена", "blocked", "detected", "timeout", "nxdomain",
            "connection", "black-hole", "mitm", "refused", "aborted", "closed",
            "ssl error", "sslerror", "fakeip", "спецдиапазон",
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
        if not explanation and line_text.startswith(("Метка:", "ID:", "Провайдер:", "URL:", "Хост:", "HTTP-хост:", "IP:", "HTTP-IP:", "Локация:", "Источник списка:", "Примечание:")):
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
