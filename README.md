# Name of the project



# Description of the project

There are 3 main components in this project:
1. Producer: A Spring Boot application that produces messages to a Kafka topic.
2. Consumer: A Spring Boot application that consumes messages from a Kafka topic.
3. Kafka: A Kafka broker running in a Docker container.

The Producer application reads records from a CSV file and sends them to the Kafka topic.
The Consumer application aggregates the messages it receives and sends the aggregated result to another Kafka topic.
The Producer application reads the aggregated messages from the second Kafka topic and prints them to the console.

# Architecture diagram

Producer --> Kafka Topic 1 --> Consumer --> Kafka Topic 2 --> Producer

# Tech stack
- Docker
- Docker Compose
- Kafka
- Java
- Spring Boot
- Maven

# Prerequisites
To run:
- Docker
- Docker Compose

For development:
- Java 11 or higher
- Maven
- Git

# Installation instructions

1. Clone the repository: `git clone <repository_url>`
2. Navigate to the project directory: `cd <project_directory>`
3. Build and run the Docker images: `docker-compose up --build`

- To see the output from a particular component, use: `docker-compose logs -f <component_name>`

# Configuration

# Testing instructions


