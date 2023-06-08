# Project Name
Bosta DATA ENGINEER CASE STUDY

## Project Overview


first i use Prefect 
Prefect enables you to build and observe resilient data workflows so that you can understand, react to, and recover from unexpected changes. It's the easiest way to transform any Python function into a unit of work that can be observed and orchestrated. Just bring your Python code, sprinkle in a few decorators, and go!


Extract, Transform, and Load (ETL) process, take data from api or any source and load data into 
mongodb instance , transform data and load it into sql instance .

So i get a datasets called taxi_trip_data this data in api and i extract this data in csv file and 
load it into mongoo ( simulate scrapping data ) and continue normal process 

## Prerequisites

Before running the project, ensure that the following prerequisites are met:

Python 3.7 or higher is installed on your system.
The required libraries and their versions mentioned in the requirements.txt file are installed.


## Installation

To install the project and its dependencies, follow these steps:

1. Install Prefect  
    pip install -U prefect

2. Clone the repository:

   ```shell
   git clone https://github.com/ahmeddiab9/Bosta_assignment.git

3. cd your-project

4. Set up a virtual environment (optional ) ( recommended ):
    python -m venv env
    source env/bin/activate

5. Install the required libraries:
    pip install -r requirements.txt

6. start the Prefect Orion server before executing the following commands run python file
    prefect orion start

7. Usage: To run the ETL pipeline, execute the following command:
    python parameterize_flow.py (filename.py)

