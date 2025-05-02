export interface QueueOptions {
  /**
   * The name of the queue
   */
  name: string;

  /**
   * Whether the queue should survive broker restarts
   * @default true
   */
  durable?: boolean;

  /**
   * The maximum number of messages that can be processed concurrently
   * @default 1
   */
  prefetchCount?: number;
}
