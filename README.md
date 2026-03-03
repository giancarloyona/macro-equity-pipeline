# macro-equity-pipeline

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Tests](https://img.shields.io/badge/tests-pytest-brightgreen.svg)](https://docs.pytest.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end **Data Engineering and Analytics pipeline** designed to analyze the correlation between macroeconomic indicators (e.g., Fed Funds Rate, CPI) and Equity Market performance (e.g., S&P 500, Sector ETFs).

## Project Overview

This project addresses the challenge of aligning and analyzing heterogeneous financial data sources. It automates the ingestion, transformation, and visualization of high-frequency market data combined with low-frequency economic indicators, providing a "Real Return" perspective for portfolio analysis.

### Key Features
* **Automated Ingestion:** Robust, modular fetchers for **FRED (Federal Reserve)** and **Yahoo Finance** APIs.
* **Software Engineer approach:** Developed using **TDD (Test-Driven Development)** and **Clean Architecture**.
* **Modern Tooling:** Managed by **`uv`** for dependency resolution and deterministic builds.
* **Production Ready:** Fully modularized for easy extension to new data sources.

---

## Tech Stack

* **Language:** Python 3.11+
* **Dependency Management:** `uv`
* **Data Processing:** `Pandas`, `NumPy`
* **Data Sources:** `yfinance`, `fredapi`
* **Testing:** `Pytest` (Unit and Integration)

---

## Architecture & Design

The project follows **Object-Oriented Programming (OOP)** principles to ensure maintainability and scalability:

* **`BaseFetcher`**: An Abstract Base Class (ABC) defining the contract for all data ingestion modules.
* **`Processors`**: Dedicated logic for time-series alignment (resampling), inflation adjustment, and volatility calculation.
* **`TDD Workflow`**: Core business logic is developed following the Red-Green-Refactor cycle.

---

## Getting Started

### Prerequisites
* Python 3.11 or higher
* [uv](https://github.com/astral-sh/uv) installed
* A FRED API Key (Free - [Get it here](https://fred.stlouisfed.org/docs/api/api_key.html))

### Installation
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/giancarloyona/macro-equity-pipeline.git
    cd macro-equity-pipeline
    ```

2.  **Set up the environment and install dependencies:**
    ```bash
    uv sync
    ```

3.  **Configure environment variables:**
    Create a `.env` file in the root directory and add your key:
    ```text
    FRED_API_KEY=your_api_key_here
    ```

4.  **Run the app:**
    ```bash
    uv run streamlit run src/macro_pipeline/dashboard/app.py
    ```

### Running Tests
To ensure the pipeline integrity, run the test suite:
```bash
uv run pytest