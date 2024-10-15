# API backend for hub.stx.pub

This project provides a backend API for exploring and visualizing data from the Stacks blockchain. It offers endpoints for miner visualization, mempool statistics, and transaction decoding.

## Features

- Miner visualization data
- Miner power statistics
- Mempool size and popular contracts
- Transaction decoding
- Periodic data collection tasks

## Prerequisites

- Go 1.21 or higher
- SQLite3

## Usage

The server will start on port 8123 by default.

## API Endpoints

- `GET /miners/viz`: Get miner visualization data
- `GET /miners/power`: Get miner power statistics
- `GET /mempool/popular`: Get popular contracts in the mempool
- `GET /mempool/size`: Get mempool size over time
- `POST /tx/decode`: Decode a hex-encoded transaction

## Development

The project uses the following main Go packages:

- `github.com/go-chi/chi/v5`: For routing
- `github.com/jmoiron/sqlx`: For database operations
- `github.com/madflojo/tasks`: For scheduling periodic tasks

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [Unlicense](LICENSE).
