# Dibimbing - Kampus Merdeka 1 - Final Project - Kelompok 7
- Fathur Rahman Syahputra
- Animni Fiddaroini
- alya Nabila
- Gloria W. Z.
- Imas Siti M.

Tools that you need to prepare
- WSL 2/ubuntu (Highly recommended): you can't run this on Windows, so you'll need a linux subsytem on windows like WSL 2
- Docker (Must install)
- Airflow (Must install)
- DBeaver (optional): you can use other SQL query tools
- PostgreSQL (Must install)

## How to
- In order to spin up the containers, first you have to build all the Docker images needed using 
    ```sh
    make build
    ```
- Once all the images have been build up, you can try to spin up the containers using
    ```sh
    make spinup
    ```
- Once all the containers ready, you can try to
    - Access the Airflow on port `8081`, and access `http://localhost:8081/connection/add` to set postgres connection
    - Connection Id: postgres_dw
    - Host: host.docker.internal
    - Schema: data_warehouse
    - login: user
    - password: password
    - port 5433
    - Now you can try to run dag task on Airflow
    - Since we have 2 Postgres containers, you can use `dataeng-warehouse-postgres` container as your data warehouse and ignore the `dataeng-ops-postgres` container since it is only being used for the opetrational purposes.
---
## Folder Structure

**main**

In the main folder, you can find `makefile`, so if you want to automate any script, you can try to modify it.

There is also `requirements.txt`, so if you want to add a library to the Airflow container, you can try to add it there. Once you add the library name in the file, make sure you rebuild the image before you spin up the container.

**dags**

This is were you put your `dag` files. This folder is already mounted on the container, hence any updates here will automatically take effect on the container side.

**data**

This flder contains the data needed for your project. If you want to generate or add additional data, you can place them here.

**docker**

Here is the place where you can modify or add a new docker stack if you decide to introduce a new data stack in your data platform. You are free to modify the given `docker-compose.yml` and `Dockerfile.airflow`.

**scripts**

This folder contains script needed in order to automate an initializations process on docker-container setup.

---
