# Connectivity Tester (GUI)

[![Release](https://img.shields.io/github/v/release/OMchik33/DPI_Connectivity_Tester)](https://github.com/OMchik33/DPI_Connectivity_Tester/releases)
[![License](https://img.shields.io/badge/license-MIT-green)](#-license)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)

👉 **Русская версия README:**
https://github.com/OMchik33/Connectivity_Tester/blob/main/README-RU.md

------------------------------------------------------------------------

## ⚠️ Disclaimer

This utility is intended **only for diagnosing network connectivity,
traffic analysis, and infrastructure availability testing**.

The author is not responsible for misuse of this software in violation
of laws.
Use it **only for legal purposes**.

------------------------------------------------------------------------

## 🧩 About

A powerful tool for diagnosing:

-   Network restrictions
-   Traffic filtering (DPI)
-   Website accessibility issues

Now with a full **GUI**, parallel testing, and clear result
visualization.

------------------------------------------------------------------------

## 🖼️ Interface Preview

<img width="1219" height="881" alt="image" src="https://github.com/user-attachments/assets/3050f5c8-dec7-461b-bd05-2e17c4c2b70d" />

------------------------------------------------------------------------

## 🚀 Features

-   🌐 Website & infrastructure availability checks
-   🔎 DNS restriction detection
-   🔐 TLS (1.2 / 1.3) testing
-   🛡️ SSL validation
-   📡 HTTP availability check
-   🧠 DPI filtering analysis
-   ⚡ Parallel execution
-   🎨 Color-coded results
-   💬 Tooltips with explanations
-   🔍 "Show only issues" filter

------------------------------------------------------------------------

## 🖥️ GUI

### Controls

-   **Standard Check** --- run built-in list
-   **My List** --- run custom list
-   **Stop Test** --- stop execution

### Results Table

  Status   Meaning
  -------- ----------------------
  🟢       Available
  🟡       Restricted / Partial
  🔴       Connection Problem

Hover over cells to see detailed explanations.

------------------------------------------------------------------------

## 📄 Custom Site List

File: `user_sites.txt`
Created automatically next to the EXE.

### Supported formats

    site.com
    site.com:771
    https://site.com
    https://site.com/path
    https://site.com/file.ext

### Tips

-   Use **domain** → general testing
-   Use **full URL** → precise diagnostics

------------------------------------------------------------------------

## 🌐 Built-in Test List

Includes stable global services:

-   YouTube
-   GitHub
-   Microsoft
-   Cloudflare
-   AWS
-   Hetzner
-   OVH
-   DigitalOcean
-   Vultr
-   Fastly
-   Google Cloud

------------------------------------------------------------------------

## 🔍 Test Types

### DNS

Checks if domain resolves to IP.

### TLS

Tests TLS 1.2 / 1.3.

### SSL

Validates certificate and connection.

### HTTP

Checks HTTP response.

### DPI Analysis

Analyzes first \~16KB of response.

------------------------------------------------------------------------

## ⚠️ Statuses

  Status       Description
  ------------ -------------------------------
  Available    Everything works
  Restricted   Partial issues
  Problem      Connection or filtering error

Additional:

-   `Not checked (HTTP XXX)`
-   `Not checked (<16 KB ...)`

------------------------------------------------------------------------

## 📍 Geolocation

Uses external services.

Limitations:

-   Some IPs cannot be detected
-   CDN may return inaccurate location

------------------------------------------------------------------------

## 🧠 Tooltips

Shows:

-   Error causes
-   Status explanation
-   Final verdict

------------------------------------------------------------------------

## ⚙️ Build EXE

    python -m PyInstaller --noconfirm --clean --onefile --windowed --name DPIConnectivityTester main.py

------------------------------------------------------------------------

## 📦 Dependencies

    requests
    dnspython

------------------------------------------------------------------------

## 💡 Notes

-   Parallel tests with thread limits
-   DPI analysis may fail on small pages
-   Results depend on ISP / region

------------------------------------------------------------------------

## 📜 License

MIT License

Use responsibly and in accordance with your local laws.
