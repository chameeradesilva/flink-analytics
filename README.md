# Flink Learning Lab 🚀
**PoC Goals**: Socket-stream word counting 📊 & fraud detection ⚠️ using Flink state.

## Prerequisites
1. **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
2. **Netcat (nc)**:
    - **Linux/macOS**: Pre-installed (verify with `nc -h`).
    - **Windows**: Use [ncat (from Nmap)](https://nmap.org/ncat/) or Git Bash's `nc`.

## Key Patterns & Principles
- **Design Patterns**: `KeyedProcessFunction` (stateful fraud detection), `FlatMap` (parsing).
- **Fault Tolerance**: Checkpointing ⏱️ enabled (5s intervals).
- **Watermarks**: Bounded disorder handling (5s delay) for event time.

## Run It! ▶️
1. **Build & Deploy**:
   ```bash  
   mvn clean package           # Build JAR to target/  
   docker-compose up           # Start Flink cluster  