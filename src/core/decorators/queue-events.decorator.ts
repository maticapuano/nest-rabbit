import { SetMetadata } from "@nestjs/common";

/**
 * Metadata key used to store queue event handlers
 */
export const QUEUE_EVENTS_METADATA = Symbol("QUEUE_EVENTS_METADATA");

/**
 * Types of events that can be emitted by the queue system
 */
export enum QueueEventType {
  /**
   * Emitted when the queue connection is established
   */
  CONNECTED = "connected",

  /**
   * Emitted when the queue connection is lost
   */
  DISCONNECTED = "disconnected",

  /**
   * Emitted when an error occurs in the queue system
   */
  ERROR = "error",

  /**
   * Emitted when a message is received from the queue
   */
  MESSAGE_RECEIVED = "message_received",

  /**
   * Emitted when a message is successfully processed
   */
  MESSAGE_PROCESSED = "message_processed",

  /**
   * Emitted when message processing fails
   */
  MESSAGE_FAILED = "message_failed",
}

/**
 * Options for queue event handlers
 */
export interface QueueEventOptions {
  /**
   * The type of event to handle
   */
  event: QueueEventType;
}

/**
 * Base decorator for queue event handlers
 *
 * @param options - Event handler configuration
 * @returns MethodDecorator
 */
export const OnQueueEvent = (options: QueueEventOptions) => {
  return SetMetadata(QUEUE_EVENTS_METADATA, options);
};

/**
 * Decorator for handling queue error events
 *
 * @example
 * ```typescript
 * @OnQueueError()
 * async handleError(error: any) {
 *   console.error("Queue error:", error);
 * }
 * ```
 *
 * @returns MethodDecorator
 */
export const OnQueueError = () => OnQueueEvent({ event: QueueEventType.ERROR });

/**
 * Decorator for handling message received events
 *
 * @example
 * ```typescript
 * @OnMessageReceived()
 * async handleMessageReceived(data: { queue: string; content: any }) {
 *   console.log("Message received:", data);
 * }
 * ```
 *
 * @returns MethodDecorator
 */
export const OnMessageReceived = () =>
  OnQueueEvent({ event: QueueEventType.MESSAGE_RECEIVED });

/**
 * Decorator for handling message processed events
 *
 * @example
 * ```typescript
 * @OnMessageProcessed()
 * async handleMessageProcessed(data: { queue: string; content: any; result: any }) {
 *   console.log("Message processed:", data);
 * }
 * ```
 *
 * @returns MethodDecorator
 */
export const OnMessageProcessed = () =>
  OnQueueEvent({ event: QueueEventType.MESSAGE_PROCESSED });

/**
 * Decorator for handling message failed events
 *
 * @example
 * ```typescript
 * @OnMessageFailed()
 * async handleMessageFailed(data: { queue: string; content: any; error: any }) {
 *   console.error("Message failed:", data);
 * }
 * ```
 *
 * @returns MethodDecorator
 */
export const OnMessageFailed = () =>
  OnQueueEvent({ event: QueueEventType.MESSAGE_FAILED });
