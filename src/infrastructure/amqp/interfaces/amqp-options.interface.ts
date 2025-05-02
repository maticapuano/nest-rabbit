/**
 * Configuration options for AMQP connection and queue setup
 */
export interface AmqpOptions {
  /**
   * Array of AMQP connection URLs to try connecting to
   */
  urls: string[];

  /**
   * Name of the default queue to use
   */
  queue: string;

  /**
   * Optional queue configuration options
   */
  queueOptions?: {
    /**
     * Whether the queue should survive broker restarts
     * @default false
     */
    durable?: boolean;

    /**
     * Whether the queue can only be accessed by the current connection
     * @default false
     */
    exclusive?: boolean;

    /**
     * Whether the queue should be deleted when the last consumer unsubscribes
     * @default false
     */
    autoDelete?: boolean;
  };

  /**
   * Number of messages to prefetch from the queue
   * @default 1
   */
  prefetchCount?: number;

  /**
   * Number of times to retry failed operations
   * @default 3
   */
  retryAttempts?: number;

  /**
   * Delay in milliseconds between retry attempts
   * @default 1000
   */
  retryDelay?: number;

  /**
   * Whether to enable debug logging
   * @default false
   */
  debug?: boolean;

  /**
   * Whether to use the delayed exchange
   * @default true
   */
  useDelayedExchange?: boolean;
}

export interface PublishOptions {
  /**
   * Delay in milliseconds before the message is delivered
   */
  delay?: number;

  /**
   * Whether the message should persist in case of server restart
   * @default true
   */
  persistent?: boolean;

  /**
   * Message priority (0-255)
   */
  priority?: number;

  /**
   * Custom headers for the message
   */
  headers?: Record<string, any>;

  /**
   * Time to live in milliseconds
   */
  ttl?: number;

  /**
   * Whether to use the delayed exchange
   * @default true
   */
  useDelayedExchange?: boolean;
}
