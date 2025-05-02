export class RabbitMQException extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "RabbitMQException";

    if (cause instanceof Error) {
      this.stack = cause.stack;
    }
  }
}
