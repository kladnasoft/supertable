# SuperTable

![Python](https://img.shields.io/badge/python-3.10+-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**  
A high‑performance, lightweight transaction catalog that integrates multiple
basic tables into a single, cohesive framework designed for ultimate
efficiency.
It automatically creates and manages tables so you can start running SQL queries
immediately—no complicated schemas or manual joins required.


---

## Installation

```bash
pip install supertable
```

## Setup
To set the SUPERTABLE_HOME variable in Linux (default: ~/supertable):
```bash
export SUPERTABLE_HOME="$HOME/supertable"
```

SuperTable is published on PyPI. The only requirement is Python ≥ 3.10.

---

## Key Features

- **Automatic table creation**  
  Load your data and SuperTable instantly builds the required tables and
  columns—no predefined schema or extra setup.

- **Self‑referencing architecture**  
  Combine and analyze data across tables without writing manual joins.

- **Staging module with history**  
  Upload files to a staging area and reload any version at any time, keeping a
  complete audit trail for tracking and compliance.

- **Columnar storage for speed**  
  With fully denormalized columnar storage, queries remain lightning-fast, 
  even when dealing with thousands of columns

- **Built‑in RBAC security**  
  Define users and roles to control row‑ and column‑level access—no external
  security tools required.

- **Platform independent**  
  Deploy on any major cloud provider or on‑premise. SuperTable is a pure Python
  library with no hidden costs.

---

## Examples

The project ships with an **`examples/`** folder that walks you through common
workflows:

| Script prefix | What it shows |
|---------------|---------------|
| **1.\*** | Create a SuperTable, roles, and users |
| **2.\*** | Write dummy or single‑file data into a simple table |
| **3.\*** | Read data, query statistics, and inspect metadata |
| **4.1**   | Clean obsolete files |
| **5.\*** | Delete tables and supertables |

Additional utility scripts demonstrate locking, parallel writes, and
performance measurement. Browse the folder to get started quickly.

---

## Benefits

- **Quick start**  
  Go from raw data to query‑ready in minutes—faster than spreadsheets or
  traditional databases.

- **Higher efficiency**  
  Eliminate manual steps and rework so you spend more time analyzing and less
  time wrangling.

- **Holistic insights**  
  Analyze datasets individually or together to uncover trends, outliers, and
  cross‑dependencies.

- **Cost savings**  
  Consolidate licenses, simplify support, and reinvest the savings in deeper
  analytics.



## 🚀 SuperTable Benchmark (Serialized Run)

Our latest benchmark shows how **SuperTable** performs in a serialized run on real data:

<table>
<tr>
<td valign="top">

<h3>💡 Performance Highlights</h3>

<ul>
  <li>👉 <b>29,788 files processed</b></li>
  <li>⚡ ~<b>18,865 rows/sec</b> throughput</li>
  <li>📂 ~<b>4.9 files/sec</b> processed</li>
  <li>📈 <b>1.1 MB/sec</b> sustained throughput</li>
  <li>✅ <b>74 million new rows inserted</b> with zero deletions</li>
</ul>

</td>
<td valign="top">

<img width="226" height="340" alt="SuperTable Benchmark" src="https://github.com/user-attachments/assets/b53ace69-098c-4953-b18a-460571e15da5" />

</td>
</tr>
</table>


---

SuperTable provides a flexible, high‑performance solution that grows with your
business. Cut complexity, save time, and gain deeper insights—all in a single,
streamlined platform.

