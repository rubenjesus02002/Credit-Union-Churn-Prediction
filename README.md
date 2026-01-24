# Data Directory

## Files

### `credit_union_data.db` (Not in Git)
- **Full dataset:** 10,000 members, 7.2M transactions
- **Size:** ~150MB
- **Location:** This file is `.gitignore`d and must be generated locally
- **How to generate:**
```bash
  cd ../01_data_generation
  python random_data_generation.py
  mv credit_union_data.db ../data/
```

### `csv_data_preview/`
- Sample data (200 rows per table)
- For quick preview without running generation script
- Safe for Git repository

## Quick Start

After generating the database:
```bash
# Connect with Python
import sqlite3
conn = sqlite3.connect('data/credit_union_data.db')

# Or open with DB Browser for SQLite
# File → Open Database → data/credit_union_data.db
```
