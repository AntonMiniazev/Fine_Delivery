# Content

### Order_generator-master.ipynb
1. Takes assortment and delivery type tables
2. Generates orders with given specifics:
  - Every product has quantity limits (min and max, max depends on delivery_type: 1 - Bike, 2 - Car). Limits are set in [assortment_generator.csv](https://github.com/AntonMiniazev/Fine_Delivery/blob/main/project_notebooks/assortment_generator.csv)
  - Every day has 20-35 orders.
  - Number of positions for cars 5 to 20 and for bike 2 to 12.
  - Range of dates is an input to order_dates function

3. Generated orders are splitted into two input tables Orders and Products to be uploaded to initial_data folder 

### Database_initialization_RDS-master.ipynb
1. Uploads tables from initial_data folder to database and sets up relations.
2. Creates the Proposed table.

