# data-engineer-challenge

## Description

This proof of concept will explore concepts and tools in Data Engineering, and required knowledge in
Software development as well.

### Scope

This POC task is to build an automatic process to ingest data on an on-demand basis. The data represents trips taken by different vehicles, and include a city, a point of origin and a destination.

This [CSV file](https://drive.google.com/file/d/14JcOSJAWqKOUNyadVZDPm7FplA7XYhrU/view) gives a small sample of the data your solution will have to handle. We would like to have some visual reports of this data, but in order to do that, we need to explore the following features.

### Mandatory Features

- There must be an automated process to ingest and store the data.
- Trips with similar origin, destination, and time of day should be grouped together.
- Develop a way to obtain the weekly average number of trips for an area, defined by a bounding box (given by coordinates) or by a region.
- Develop a way to inform the user about the status of the data ingestion without using a polling solution.
- The solution should be scalable to 100 million entries. It is encouraged to simplify the data by a data model. Please add proof that the solution is scalable.
- Use a SQL database.

### Bonus features

- Containerize the solution.
- Sketch up how one would set up the application using any cloud provider (AWS, GoogleCloud, etc).

## Solution

Here is the [link to the video explaining my solution](https://drive.google.com/file/d/13N5IkBpd5Mq9SaqvOXsYvjATlQyaxysr/view?usp=sharing)

### Architecture
![alt text](https://github.com/marcelo-guimaraes/data-engineering-challenge/blob/main/files/diagram.jpeg?raw=true)

I decided to build the solution on the Google Cloud Platform. I used the Google Drive as the data source and built an external table in Google BigQuery pointing to it. Then, using the Dataform as a query engine tool and the Airflow for orchestration, I built a curated table to be ingested by the Looker Studio dashboard. I've also created a stutus table in BigQuery with the status of the pipeline execution from Airflow, that was I could inform the user about the status of the data ingestion.
As you can see in the [curated table script](https://github.com/marcelo-guimaraes/data-engineering-challenge/blob/main/definitions/curated/cur_trips.sqlx), I multiplied the raw table records to 1M, making the total number of records in the curated table a 100M records, proofing the scalability of the architecture.

Here is how the final dashboard looks:
![image](https://github.com/marcelo-guimaraes/data-engineering-challenge/assets/56089963/f250fadc-964d-4c95-b8b1-3b590decc966)


