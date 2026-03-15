### Requirements

- Docker
- Docker compose
- Git

### Quickstart
Step 1: Clone the Repo

```bash
git clone https://github.com/Larbol624/Environmental-Sensor-Pipeline.git
cd Environmental-Sensor-Pipeline
```

Step 2: Create a external network

```bash
docker network create env_network
```
Step 3: Create a .env file

    create a .env file in the folder configs/postgres
    Copy the contents of example_postgres_env.txt into the .env file and 
    choose your own postgres user and postgres password 


Step 4: Build the docker compose

```bash
docker compose -f docker-compose-sensor-pipeline.yml build
```

Step 5: Start the container

```bash
docker compose -f docker-compose-sensor-pipeline.yml up -d
```

Step 6: Open the dashboard

Use your browser to visit http://localhost:3000

you can log in by using the credentials 
username: admin
password: admin


Then you can open the dashboard sensor_dashboard.
