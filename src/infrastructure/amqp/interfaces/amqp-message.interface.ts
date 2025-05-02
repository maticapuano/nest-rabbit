export interface AmqpMessage {
  content: Buffer;
  properties: {
    headers?: Record<string, unknown>;
    timestamp?: number;
    messageId?: string;
  };
}
