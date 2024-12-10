# onyx
onyx_challenge

## Setup Instructions

### 1. Poetry Installation & Dependencies
First, make sure you have Poetry installed:
```bash
brew install poetry
```

Navigate to the project directory:
```bash
cd onyx_challenge
```

Install dependencies:
```bash
poetry install
```
Install dbt Dependencies
```bash
cd dbt_project
dbt deps
```

### 2. Run pipeline
```bash
poetry run python dbt_project/pipeline.py
```
### 3. Generate dbt docs

```bash
dbt generate docs --target gold
```

serve docs

```bash
dbt docs serve --target gold
```

