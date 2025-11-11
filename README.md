# connector-intake-service

Gateway for document intake (stateless refactor)

## Development Setup

- Run `./scripts/start-connector-intake.sh` to launch Quarkus dev mode with the shared Pipeline DevServices stack.
- The helper script bootstraps `dev-assets` utilities automatically (similar to the Gradle wrapper) so no manual setup is needed.
- Ensure Docker is running; the `quarkus-pipeline-devservices` extension will extract the shared compose file to `~/.pipeline/compose-devservices.yml` and Quarkus Compose Dev Services will start MySQL, Kafka, and Consul for you.

## Testing

- Quarkus DevServices provisions an ephemeral MySQL 8 container when tests run via `./gradlew test`.
- Schema migrations execute through Flyway (no manual SQL bootstrapping required).