# Air Traffic Digital Twin

This project creates **digital twins for air traffic**, enabling real-time monitoring and advanced predictive analytics using **machine learning** and **AI**.

> **Status:** Active development. Features and documentation may change frequently.


## Project Goals

- **Real-time flight tracking:** Visualize and monitor flights as they happen.
- **Air traffic congestion prediction:** LSTM/GRU models to forecast congested airspace.
- **Deviation detection:** Autoencoders to identify abnormal flight patterns.
- **Turbulence and climate event prediction:** Combining CNN and LSTM models for weather-related forecasts.
- **Air traffic simulation:** “What-if” scenarios using **reinforcement learning**.
- **Pollution prediction:** Estimating environmental impact of air traffic.
- **Delay prediction:** Forecast flight delays using data-driven approaches.



## Prerequisites

- Docker
- Python >= 3.10



## Deployment

1. **Clone the repository**

```bash
git clone https://github.com/v-mdev/airtraffic-digital-twin.git .
cd airtraffic-digital-twin
```

2. **Configure environment variables**

Fill the `.env.template` doc and move it:
```bash
cp .env.template src/airtraffic/config/.env
cd ..
```

3. **Start services with Docker Compose**

```bash
docker compose up -d
```

4. **Verify that services are running**

```bash
docker compose ps
```

5. **Access the application**

- Kafka UI: `http://localhost:9000`  
- InfluxDB: `http://localhost:8086`
- Grafana: `http://localhost:3000`

6. **Logs and debugging**

```bash
docker-compose logs -f <service_name>
docker-compose down
```

