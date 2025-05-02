import { SetMetadata } from "@nestjs/common";

/**
 * Metadata key used to store queue configuration
 */
export const QUEUE_METADATA = Symbol("QUEUE_METADATA");

/**
 * Options for configuring a queue
 */
export interface QueueOptions {
  /**
   * Name of the queue
   */
  name: string;

  /**
   * Number of messages to prefetch from the queue
   * @default 1
   */
  prefetchCount?: number;

  queueOptions?: {
    durable?: boolean;
    exclusive?: boolean;
    autoDelete?: boolean;
  };
}

/**
 * Decorator that marks a class as a queue processor
 *
 * @example
 * ```typescript
 * @Queue({
 *   name: "test-queue",
 *   prefetchCount: 10
 * })
 * @Injectable()
 * class MyQueueProcessor {
 *   @Process("test-queue")
 *   async handleMessage(message: any) {
 *     // Process message
 *   }
 * }
 * ```
 *
 * @param options - Queue configuration options
 * @returns ClassDecorator
 */
export const Queue = (options: QueueOptions) => {
  return SetMetadata(QUEUE_METADATA, {
    ...options,
    prefetchCount: options.prefetchCount || 1,
  });
};
