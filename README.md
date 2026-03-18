# DPI Connectivity Tester (GUI)

[🇷🇺 Русская версия](https://github.com/OMchik33/DPI_Connectivity_Tester/blob/main/README-RU.md)

---

## 🚀 DPI / Blocking / Network Diagnostics Tool

A modern utility for detecting:

- DPI filtering  
- Network restrictions  
- DNS blocking  
- TLS/SSL issues  
- Website accessibility problems  

Now with a **full-featured GUI**, **parallel testing**, and **clear visual results**.

---

## 📸 Screenshot

<img width="1118" height="749" alt="GUI Screenshot" src="https://github.com/user-attachments/assets/058c79d6-94df-4980-9b0f-61b80a8d20bb" />

---

## ✨ Features

- 🌐 Website & infrastructure availability check  
- 🔎 DNS resolution test (detect blocking)  
- 🔐 TLS 1.2 / 1.3 support check  
- 🛡 SSL certificate validation  
- 📡 HTTP response verification  
- 🧠 DPI detection (first ~16KB analysis)  
- ⚡ Parallel execution (fast testing)  
- 🎨 Color-coded results  
- 💬 Smart tooltips with explanations  
- 🔍 "Show only issues" filter  

---

## 🖥️ GUI Interface

### Controls

| Button | Description |
|--------|------------|
| **Standard Check** | Run built-in list |
| **My List** | Run your custom list |
| **Stop Test** | Stop execution |

### Result Status Colors

| Color | Meaning |
|------|--------|
| 🟢 | Available |
| 🟡 | Restricted / Partial |
| 🔴 | Problem |

Hover over cells to see detailed explanations.

---

## 📄 Custom Site List

File: `user_sites.txt`  
Created automatically

### Supported formats:

```
site.com
site.com:771
https://site.com
https://site.com/path
https://site.com/file.ext
```

### Recommendations

- Use **domain** → for general checks  
- Use **full URL** → for precise testing  

---

## 🌐 Default Test List

Includes major providers and infrastructure:

- YouTube  
- GitHub  
- Microsoft  
- Cloudflare  
- AWS  
- Hetzner  
- OVH  
- DigitalOcean  
- Vultr  
- Fastly  
- Google Cloud  

Focus: **stable domains**, not temporary files.

---

## 🔍 Test Types

| Test | Description |
|------|------------|
| DNS | Domain → IP resolution |
| TLS | TLS 1.2 / 1.3 support |
| SSL | Certificate validation |
| HTTP | HTTP response |
| DPI | Deep packet inspection detection |

---

## ⚠️ Statuses

| Status | Meaning |
|--------|--------|
| Available | Everything works |
| Restricted | Partial issues |
| Problem | Block or error |

### Additional states

- `Not tested (HTTP XXX)` — server returned error  
- `Not tested (<16 KB ...)` — not enough data for DPI  

---

## 📍 Location Detection

Uses external geo-IP services.

Limitations:

- Some IPs cannot be resolved  
- CDN may return inaccurate locations  

---

## 🧠 Tooltips

Hover over results to see:

- error reasons  
- decoded statuses  
- final verdict  

---

## ⚙️ Build EXE

```
python -m PyInstaller --noconfirm --clean --onefile --windowed --name DPIConnectivityTester main.py
```

---

## 📦 Dependencies

```
requests
dnspython
```

---

## 💡 Notes

- Tests run in parallel (thread-limited)  
- DPI test may fail on small responses  
- Results may vary by ISP/region  

---

## 📜 License

MIT License
