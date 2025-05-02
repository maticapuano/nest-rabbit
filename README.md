# Nest RabbitMQ

A powerful RabbitMQ module for NestJS applications with support for delayed messages, message priorities, and more.

## Features

- 🔄 Automatic connection and reconnection to RabbitMQ
- ⏱️ Delayed message delivery support
- ⚡ Message priority support
- ⏳ TTL (Time To Live) support
- 📝 Custom message headers
- 💾 Persistent messages
- 🔍 Debug logging
- 🛡️ Robust error handling
- 🔌 Optional delayed exchange support

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Module Setup](#module-setup)
  - [Publishing Messages](#publishing-messages)
  - [Consuming Messages](#consuming-messages)
- [Options](#options)
  - [Publish Options](#publish-options)
  - [Configuration Options](#configuration-options)
- [Examples](#examples)
- [Contributing](#contributing)
- [License](#license)

## Installation

```bash
npm install nest-rabbit
```

## Configuration

If you plan to use delayed messages (`useDelayedExchange: true`), you'll need to enable the `rabbitmq_delayed_message_exchange` plugin on your RabbitMQ server.

## Usage

### Module Setup

#### Basic Configuration

```typescript
import { Module } from "@nestjs/common";
import { RabbitMQModule } from "nest-rabbit";

@Module({
  imports: [
    RabbitMQModule.forRoot({
      urls: ["amqp://localhost:5672"],
      queue: "default-queue",
      queueOptions: {
        durable: true,
        exclusive: false,
        autoDelete: false,
      },
      prefetchCount: 10,
      debug: true,
      useDelayedExchange: true, // Enable delayed messages
    }),
  ],
})
export class AppModule {}
```

#### Async Configuration

You can also configure the module asynchronously using `forRootAsync`. This is useful when you need to inject dependencies or use environment variables:

```typescript
import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { RabbitMQModule } from "nest-rabbit";

@Module({
  imports: [
    ConfigModule.forRoot(), // Make sure ConfigModule is imported
    RabbitMQModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => ({
        urls: [configService.get<string>("RABBITMQ_URL")],
        queue: configService.get<string>("RABBITMQ_QUEUE"),
        queueOptions: {
          durable: configService.get<boolean>("RABBITMQ_DURABLE", true),
          exclusive: configService.get<boolean>("RABBITMQ_EXCLUSIVE", false),
          autoDelete: configService.get<boolean>("RABBITMQ_AUTO_DELETE", false),
        },
        prefetchCount: configService.get<number>("RABBITMQ_PREFETCH_COUNT", 10),
        debug: configService.get<boolean>("RABBITMQ_DEBUG", false),
        useDelayedExchange: configService.get<boolean>(
          "RABBITMQ_USE_DELAYED_EXCHANGE",
          true,
        ),
      }),
      inject: [ConfigService],
    }),
  ],
})
export class AppModule {}
```

### Publishing Messages

```typescript
import { Injectable } from "@nestjs/common";
import { RabbitMQService } from "nest-rabbit";

@Injectable()
export class MessageService {
  constructor(private readonly rabbitMQService: RabbitMQService) {}

  async publishMessage() {
    // Simple message
    await this.rabbitMQService.publish("my-queue", { message: "Hello" });

    // Delayed message
    await this.rabbitMQService.publish(
      "my-queue",
      { message: "Hello" },
      {
        delay: 5000, // 5 seconds
      },
    );

    // Message with multiple options
    await this.rabbitMQService.publish(
      "my-queue",
      { message: "Hello" },
      {
        delay: 5000,
        priority: 1,
        persistent: true,
        ttl: 10000, // 10 seconds
        headers: {
          "custom-header": "value",
        },
      },
    );
  }
}
```

### Consuming Messages

```typescript
import { Injectable } from "@nestjs/common";
import { Queue, Process } from "nest-rabbit";

@Queue({
  name: "my-queue",
  prefetchCount: 10,
})
export class MessageProcessor {
  @Process()
  async handleMessage(message: any) {
    console.log("Received message:", message);
    // Process the message
  }
}
```

## Options

### Publish Options

The `PublishOptions` interface allows you to configure:

| Option               | Type                  | Description                                          | Default |
| -------------------- | --------------------- | ---------------------------------------------------- | ------- |
| `delay`              | `number`              | Delay in milliseconds before message delivery        | -       |
| `persistent`         | `boolean`             | Whether the message should persist on server restart | `true`  |
| `priority`           | `number`              | Message priority (0-255)                             | -       |
| `headers`            | `Record<string, any>` | Custom message headers                               | -       |
| `ttl`                | `number`              | Message time to live in milliseconds                 | -       |
| `useDelayedExchange` | `boolean`             | Whether to use the delayed exchange                  | `true`  |

### Configuration Options

The `AmqpOptions` interface allows you to configure:

| Option                    | Type       | Description                                                             | Default |
| ------------------------- | ---------- | ----------------------------------------------------------------------- | ------- |
| `urls`                    | `string[]` | RabbitMQ connection URLs                                                | -       |
| `queue`                   | `string`   | Default queue name                                                      | -       |
| `queueOptions`            | `object`   | Queue configuration options                                             | -       |
| `queueOptions.durable`    | `boolean`  | Whether the queue should survive broker restarts                        | `false` |
| `queueOptions.exclusive`  | `boolean`  | Whether the queue can only be accessed by the current connection        | `false` |
| `queueOptions.autoDelete` | `boolean`  | Whether the queue should be deleted when the last consumer unsubscribes | `false` |
| `prefetchCount`           | `number`   | Number of messages to prefetch                                          | `1`     |
| `debug`                   | `boolean`  | Enable debug logging                                                    | `false` |
| `useDelayedExchange`      | `boolean`  | Whether to use the delayed exchange                                     | `true`  |

## Examples

### Basic Setup Without Delayed Exchange

```typescript
@Module({
  imports: [
    RabbitMQModule.forRoot({
      urls: ["amqp://localhost:5672"],
      queue: "default-queue",
      queueOptions: {
        durable: true,
      },
      useDelayedExchange: false, // Disable delayed exchange
    }),
  ],
})
export class AppModule {}
```

### Priority Queue Example

```typescript
@Injectable()
export class PriorityService {
  constructor(private readonly rabbitMQService: RabbitMQService) {}

  async sendHighPriorityMessage() {
    await this.rabbitMQService.publish(
      "priority-queue",
      { message: "High priority" },
      {
        priority: 10,
        persistent: true,
      },
    );
  }
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
