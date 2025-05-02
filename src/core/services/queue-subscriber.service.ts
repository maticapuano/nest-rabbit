import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { DiscoveryService, MetadataScanner, Reflector } from "@nestjs/core";
import { RabbitMQService } from "../../infrastructure/amqp/services/rabbitmq.service";
import { PROCESS_METADATA, ProcessOptions } from "../decorators/process.decorator";
import {
  QUEUE_EVENTS_METADATA,
  QueueEventType,
} from "../decorators/queue-events.decorator";
import { QUEUE_METADATA } from "../decorators/queue.decorator";

/**
 * Service responsible for discovering and subscribing to queues based on decorators.
 * It handles:
 * - Queue discovery using decorators
 * - Event handler registration
 * - Message processing with retry logic
 * - Event emission for queue lifecycle and message processing
 */
@Injectable()
export class QueueSubscriberService implements OnModuleInit {
  private readonly logger = new Logger(QueueSubscriberService.name);
  private readonly eventHandlers = new Map<QueueEventType, Set<Function>>();

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly metadataScanner: MetadataScanner,
    private readonly reflector: Reflector,
    private readonly rabbitMQService: RabbitMQService,
  ) {}

  /**
   * Initializes the service by setting up event handlers and subscribing to queues
   */
  async onModuleInit() {
    this.setupEventHandlers();
    await this.subscribeToQueues();
    await this.emitEvent(QueueEventType.CONNECTED);
  }

  /**
   * Discovers and registers event handlers from providers
   * @private
   */
  private setupEventHandlers() {
    const providers = this.discoveryService.getProviders();

    providers.forEach(provider => {
      if (!provider.instance) return;

      const prototype = Object.getPrototypeOf(provider.instance);
      const methods = this.metadataScanner.getAllMethodNames(prototype);

      methods.forEach(methodName => {
        const method = provider.instance[methodName];
        const eventOptions = this.reflector.get<{ event: QueueEventType }>(
          QUEUE_EVENTS_METADATA,
          method,
        );

        if (eventOptions) {
          if (!this.eventHandlers.has(eventOptions.event)) {
            this.eventHandlers.set(eventOptions.event, new Set());
          }
          this.eventHandlers.get(eventOptions.event)?.add(method.bind(provider.instance));
        }
      });
    });
  }

  /**
   * Emits an event to all registered handlers
   * @param event - The type of event to emit
   * @param data - The data to pass to event handlers
   * @private
   */
  private async emitEvent(event: QueueEventType, data?: any) {
    const handlers = this.eventHandlers.get(event);

    if (handlers) {
      for (const handler of handlers) {
        try {
          await handler(data);
        } catch (error) {
          this.logger.error(`Error in ${event} event handler:`, error);
        }
      }
    }
  }

  /**
   * Executes a function with retry logic based on the provided options
   * @param fn - The function to execute
   * @param options - Retry configuration options
   * @param attempt - Current attempt number (starts at 1)
   * @returns The result of the function execution
   * @private
   */
  private async retryWithBackoff(
    fn: () => Promise<any>,
    options: ProcessOptions["retry"],
    attempt = 1,
  ): Promise<any> {
    if (!options) {
      return fn();
    }

    try {
      return await fn();
    } catch (error) {
      if (attempt >= options.attempts!) {
        throw error;
      }

      const baseDelay = options.delay!;
      const delay = options.backoff
        ? Math.min(
            baseDelay * Math.pow(options.backoffFactor!, attempt - 1),
            options.maxDelay!,
          )
        : baseDelay;

      this.logger.warn(
        `Attempt ${attempt}/${options.attempts} failed. Retrying in ${delay}ms...`,
      );

      await new Promise(resolve => setTimeout(resolve, delay));

      return this.retryWithBackoff(fn, options, attempt + 1);
    }
  }

  /**
   * Discovers and subscribes to queues based on decorators
   * @private
   */
  private async subscribeToQueues() {
    const providers = this.discoveryService.getProviders();

    for (const provider of providers) {
      if (!provider.instance) continue;

      const queueMetadata = this.reflector.get(
        QUEUE_METADATA,
        provider.instance.constructor,
      );

      if (!queueMetadata) continue;

      const prototype = Object.getPrototypeOf(provider.instance);
      const methods = this.metadataScanner.getAllMethodNames(prototype);

      for (const methodName of methods) {
        const processOptions = this.reflector.get<ProcessOptions>(
          PROCESS_METADATA,
          prototype[methodName],
        );
        if (!processOptions) continue;

        const method = provider.instance[methodName];
        const queueName = processOptions.queue || queueMetadata.name;

        await this.rabbitMQService.subscribe(
          queueName,
          async message => {
            try {
              await this.emitEvent(QueueEventType.MESSAGE_RECEIVED, {
                queue: queueName,
                content: message,
              });

              const result = await this.retryWithBackoff(
                () => method.call(provider.instance, message),
                processOptions.retry,
              );

              await this.emitEvent(QueueEventType.MESSAGE_PROCESSED, {
                queue: queueName,
                content: message,
                result,
              });

              return result;
            } catch (error) {
              await this.emitEvent(QueueEventType.MESSAGE_FAILED, {
                queue: queueName,
                content: message,
                error,
              });
              throw error;
            }
          },
          queueMetadata.prefetchCount,
        );
      }
    }
  }
}
