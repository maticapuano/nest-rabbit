import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { AmqpConnectionManager, ChannelWrapper, connect } from "amqp-connection-manager";
import { ConfirmChannel } from "amqplib";
import { RABBITMQ_OPTIONS } from "../constants/rabbitmq.constants";
import { RabbitMQException } from "../exceptions/rabbitmq.exception";
import { AmqpOptions, PublishOptions } from "../interfaces/amqp-options.interface";

/**
 * Service for managing RabbitMQ connections and operations
 */
@Injectable()
export class RabbitMQService implements OnModuleInit, OnModuleDestroy {
  private connection: AmqpConnectionManager;
  private channelWrapper: ChannelWrapper;
  private readonly logger = new Logger(RabbitMQService.name);
  private isClosing = false;
  private readonly queues = new Set<string>();

  constructor(@Inject(RABBITMQ_OPTIONS) private readonly options: AmqpOptions) {
    this.initialize();
  }

  async onModuleInit(): Promise<void> {
    await this.channelWrapper.waitForConnect();

    this.logger.log("RabbitMQ connection established");
  }

  async onModuleDestroy(): Promise<void> {
    await this.closeConnection();

    this.logger.log("RabbitMQ connection closed successfully");
  }

  /**
   * Publishes a message to a queue
   * @param queueName - Name of the queue to publish to
   * @param message - Message to publish
   * @param options - Publishing options
   */
  async publish<T>(
    queueName: string,
    message: T,
    options?: PublishOptions,
  ): Promise<void> {
    if (this.isClosing) {
      throw new RabbitMQException("Cannot publish message while connection is closing");
    }

    try {
      await this.ensureQueue(queueName);

      const messageString = JSON.stringify(message);
      const buffer = Buffer.from(messageString, "utf8");

      const publishOptions: any = {
        persistent: options?.persistent ?? true,
      };

      if (options?.delay && options?.useDelayedExchange !== false) {
        publishOptions.headers = {
          ...publishOptions.headers,
          "x-delay": options.delay,
        };
      }

      if (options?.priority) {
        publishOptions.priority = options.priority;
      }

      if (options?.ttl) {
        publishOptions.expiration = options.ttl.toString();
      }

      if (options?.headers) {
        publishOptions.headers = {
          ...publishOptions.headers,
          ...options.headers,
        };
      }

      const exchange = options?.useDelayedExchange === false ? "" : "delayed";

      await this.channelWrapper.publish(exchange, queueName, buffer, publishOptions);

      if (this.options.debug) {
        this.logger.debug(`Message published to ${queueName}:`, message);
      }
    } catch (error) {
      if (this.options.debug) {
        this.logger.debug(`Failed to publish message to queue ${queueName}:`, error);
      }

      throw new RabbitMQException(
        `Failed to publish message to queue ${queueName}`,
        error,
      );
    }
  }

  /**
   * Subscribes to messages from a queue
   * @param queueName - Name of the queue to subscribe to
   * @param callback - Function to handle received messages
   * @param options - Subscription options
   */
  async subscribe(
    queueName: string,
    callback: (message: any) => Promise<void>,
    prefetchCount?: number,
  ): Promise<void> {
    if (this.isClosing) {
      if (this.options.debug) {
        this.logger.debug("Cannot subscribe while connection is closing");
      }

      throw new RabbitMQException("Cannot subscribe while connection is closing");
    }

    try {
      await this.ensureQueue(queueName, { prefetchCount });

      await this.channelWrapper.addSetup(async (channel: ConfirmChannel) => {
        await channel.consume(
          queueName,
          async message => {
            if (this.isClosing) {
              if (message) {
                await this.channelWrapper.nack(message, false, true);
              }
              return;
            }

            try {
              if (!message) {
                return;
              }

              let parsedMessage;

              try {
                const contentString = Buffer.from(message.content).toString("utf8");

                parsedMessage = JSON.parse(contentString);
              } catch (error) {
                if (this.options.debug) {
                  this.logger.warn(
                    `Failed to parse message content: ${error instanceof Error ? error.message : String(error)}`,
                  );
                }
              }

              const isBufferMessage =
                parsedMessage &&
                typeof parsedMessage === "object" &&
                "type" in parsedMessage &&
                parsedMessage.type === "Buffer" &&
                Array.isArray(parsedMessage.data);

              if (isBufferMessage) {
                const bufferData = Buffer.from(parsedMessage.data);
                const jsonData = JSON.parse(bufferData.toString());

                await callback(jsonData);
              } else {
                await callback(parsedMessage);
              }

              await this.channelWrapper.ack(message);
            } catch (error) {
              if (message) {
                await this.channelWrapper.nack(message, false, true);
              }

              if (this.options.debug) {
                this.logger.error(
                  `Error processing message from queue ${queueName}`,
                  error instanceof Error ? error.stack : String(error),
                );
              }
            }
          },
          { noAck: false },
        );
      });

      this.logger.log(`Subscribed to queue: ${queueName}`);
    } catch (error) {
      throw new RabbitMQException(`Failed to subscribe to queue ${queueName}`, error);
    }
  }

  private async initialize(): Promise<void> {
    try {
      this.connection = connect(this.options.urls);
      this.channelWrapper = this.connection.createChannel({
        json: true,
        setup: async (channel: ConfirmChannel) => {
          this.logger.log("Setting up RabbitMQ channel");
          await channel.prefetch(1);
        },
      });

      this.handleConnectionEvents();
    } catch (error) {
      throw new RabbitMQException("Failed to initialize RabbitMQ connection", error);
    }
  }

  private handleConnectionEvents(): void {
    this.connection.on("connect", () => {
      this.logger.log("Connected to RabbitMQ");
    });

    this.connection.on("error", err => {
      this.logger.error(
        "RabbitMQ connection error",
        err instanceof Error ? err.stack : String(err),
      );
    });
  }

  /**
   * Ensures a queue exists and is properly configured
   * @param queueName - Name of the queue to ensure
   * @param options - Queue configuration options
   */
  private async ensureQueue(queueName: string, options: { prefetchCount?: number } = {}) {
    if (this.queues.has(queueName)) {
      return;
    }

    await this.channelWrapper.addSetup(async (channel: ConfirmChannel) => {
      this.logger.log(`Creating queue: ${queueName}`);

      // Solo declarar el exchange con delay si está habilitado en las opciones
      if (this.options.useDelayedExchange !== false) {
        await channel.assertExchange("delayed", "x-delayed-message", {
          durable: true,
          arguments: {
            "x-delayed-type": "direct",
          },
        });
      }

      // Declarar la cola
      await channel.assertQueue(queueName, {
        durable: this.options.queueOptions?.durable ?? true,
        exclusive: this.options.queueOptions?.exclusive ?? false,
        autoDelete: this.options.queueOptions?.autoDelete ?? false,
      });

      // Solo vincular la cola al exchange si está habilitado
      if (this.options.useDelayedExchange !== false) {
        await channel.bindQueue(queueName, "delayed", queueName);
      }

      if (options.prefetchCount) {
        await channel.prefetch(options.prefetchCount);
      }

      this.queues.add(queueName);
      this.logger.log(`Queue ${queueName} created and configured`);
    });
  }

  async closeConnection(): Promise<void> {
    if (this.isClosing) {
      return;
    }

    this.isClosing = true;
    this.logger.log("Closing RabbitMQ connection...");

    try {
      if (this.channelWrapper) {
        await this.channelWrapper.close();
      }

      if (this.connection) {
        await this.connection.close();
      }

      this.logger.log("RabbitMQ connection closed successfully");
    } catch (error) {
      this.logger.error(
        "Error closing RabbitMQ connection",
        error instanceof Error ? error.stack : String(error),
      );
    } finally {
      this.isClosing = false;
    }
  }
}
