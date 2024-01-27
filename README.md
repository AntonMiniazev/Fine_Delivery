# Project Overview:

<p align="center">
  <img src="https://github.com/AntonMiniazev/Fine_Delivery/blob/main/other/data-pipeline-architecture-purpose.jpg" />
</p>

**Description:** This project involves the development of an automated dashboard for an online retail company, designed to monitor key sales metrics by delivery zones. 
We will simplify the pipeline preparation process for the online retail store utilizing the following tools:
- PostgreSQL database (Amazon RDS)
- Airflow (Amazon EC2)
- Power BI

[Final report](https://www.novypro.com/project/fine-delivery-dashboard-power-bi)

**Main Task:** The primary goal is to create Power BI reports that provide a simplified business overview focusing on sales metrics and basket analysis.

**Data Description:** A pre-generated set of tables in a PostgreSQL database. This data, which includes specific sales-related information such as orders, products, and sales, will be processed and organized into a designated table (**Proposed table**). This table will then serve as a consistent source for Power BI and will be regularly updated with new sales data using Airflow.

<p align="center">
  <img src="https://lucid.app/publicSegments/view/3564fc0c-9ef3-44a1-ba8b-819ac82206d3/image.png" />
</p>

# Step 1. Preparing Data

Orders and sale details (tables Orders and Products) are generated using script [Order_generator.ipynb](https://github.com/AntonMiniazev/Fine_Delivery/blob/main/project_notebooks/Order_generator.ipynb).
Other tables include delivery information, associated with the order completion expenses.
All sourcing data tables are stored in csv files in folder [initial_data](https://github.com/AntonMiniazev/Fine_Delivery/tree/main/project_notebooks)

# Step 2. Creating Database

Setup PostgreSQL database on Amazon RDS, free tier is enough for our aims. 
Tables in PostgreSQL database are created using [Database_initialization.ipynb](https://github.com/AntonMiniazev/Fine_Delivery/blob/main/project_notebooks/Database_initialization.ipynb).

# Step 3. Creating DAG

**DAG 1:** To go as a live service, new order creation is needed. For these purposes we create a DAG that generates random orders and uploads them every week to our DB.

Script for this process: [DAG 1](https://github.com/AntonMiniazev/Fine_Delivery/blob/main/DAGs/dag_load_order_data-master.py)

**DAG 2:** Initial tables in database will be reprocessed into the Proposed table by Airflow. 
Process requirements:
- Scheduled daily to maintain up-to-date the Proposed table.
- Combine data from initial tables for orders from previous two days.
- Insert combined data into the Proposed table with specifics from **Data Description**.

Script for this process: [DAG 2](https://github.com/AntonMiniazev/Fine_Delivery/blob/main/DAGs/dag_zone_economy-master.py)

# Step 4. Visualization in Power BI

Connecting Power BI to our DB using PostrgreSQL connection. After that all our tables are available with existing relationships.
Final report is published using https://www.novypro.com service.

[Final report](https://www.novypro.com/project/fine-delivery-dashboard-power-bi)

