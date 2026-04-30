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

Step 2: Create a .env file and network

    create the network and a .env file in the folder configs/postgres by running
```bash
make setup
```
you can change the values to whatever you want but the default credentials are:
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

Step 4: Build the docker compose

```bash
make build
```

Step 5: Start the container

```bash
make run
```

Step 6: Open the dashboard

Use your browser to visit http://localhost:3000

you can log in by using the credentials 
username: admin
password: admin


Then you can open the dashboard sensor_dashboard.
note: It might take take some time for the data to show. You can refresh by clicking the icon in the top right
