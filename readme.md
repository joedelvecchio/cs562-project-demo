# CS562 Project Demo

CS 562 EMF Query Processing Engine
Overview
This project develops an Extended Multi-Feature (EMF) query processing engine for advanced multi-dimensional database analysis. The engine enables complex OLAP (Online Analytical Processing) queries by supporting multiple grouping variables, sophisticated conditions, and diverse aggregate functions.
Key Features

🔍 Dynamic query processing for multi-dimensional analyses
📊 Support for multiple aggregate functions (count, sum, avg, min, max)
🔬 Flexible condition and having clause parsing
📈 Scalable processing of large datasets

Technical Requirements

Python 3.8+
PostgreSQL
Dependencies listed in requirements.txt

Installation

Clone the repository
Install dependencies:
pip install -r requirements.txt

Create a .env file with database credentials:
USER=your_username
PASSWORD=your_database_password
DBNAME=your_database_name

Ensure the sales table is created in your PostgreSQL database

Usage
Generating Queries
python generator.py

Follow the interactive prompts to:

Choose input method (file or manual)
Select query parameters
Generate and execute query processing code

Running Tests
python test_emf.py

Example Queries
The input/ directory contains sample query definitions demonstrating various multi-dimensional analysis scenarios.