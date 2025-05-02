import { SetMetadata } from "@nestjs/common";

export const PROCESS_METADATA = Symbol("PROCESS_METADATA");

/**
 * Options for configuring retry behavior when processing messages fails
 */
export interface RetryOptions {
  /**
   * Number of retry attempts before giving up
   * @default 3
   */
  attempts?: number;

  /**
   * Initial delay between retries in milliseconds
   * @default 1000
   */
  delay?: number;

  /**
   * Whether to use exponential backoff for retry delays
   * @default true
   */
  backoff?: boolean;

  /**
   * Multiplier for exponential backoff
   * @default 2
   */
  backoffFactor?: number;

  /**
   * Maximum delay between retries in milliseconds
   * @default 30000
   */
  maxDelay?: number;
}

/**
 * Options for configuring message processing
 */
export interface ProcessOptions {
  /**
   * Name of the queue to process messages from
   */
  queue?: string;

  /**
   * Configuration for retry behavior when processing fails
   */
  retry?: RetryOptions;
}

/**
 * Decorator that marks a method as a message processor for a queue
 *
 * @example
 * ```typescript
 * // Simple usage with just queue name
 * @Process("test-queue")
 * async handleMessage(message: any) {
 *   // Process message
 * }
 *
 * // Full configuration with retry options
 * @Process({
 *   queue: "test-queue",
 *   retry: {
 *     attempts: 3,
 *     delay: 1000,
 *     backoff: true,
 *     backoffFactor: 2,
 *     maxDelay: 5000
 *   }
 * })
 * async handleMessage(message: any) {
 *   // Process message
 * }
 * ```
 *
 * @param options - Queue name or full process configuration
 * @returns MethodDecorator
 */
export const Process = (options: string | ProcessOptions) => {
  const processOptions: ProcessOptions =
    typeof options === "string" ? { queue: options } : options;

  const defaultRetryOptions: RetryOptions = {
    attempts: 3,
    delay: 1000,
    backoff: true,
    backoffFactor: 2,
    maxDelay: 30000,
  };

  const finalOptions: ProcessOptions = {
    ...processOptions,
    retry: processOptions.retry
      ? { ...defaultRetryOptions, ...processOptions.retry }
      : undefined,
  };

  return SetMetadata(PROCESS_METADATA, finalOptions);
};
