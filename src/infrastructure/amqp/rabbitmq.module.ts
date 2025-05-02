import { QueueSubscriberService } from "@/core";
import { DynamicModule, Module, Provider } from "@nestjs/common";
import { DiscoveryModule, Reflector } from "@nestjs/core";
import { RABBITMQ_OPTIONS } from "./constants/rabbitmq.constants";
import { AmqpOptions } from "./interfaces/amqp-options.interface";
import { RabbitMQService } from "./services/rabbitmq.service";

@Module({
  imports: [DiscoveryModule],
})
export class RabbitMQModule {
  static forRoot(options: AmqpOptions): DynamicModule {
    const rabbitMQOptionsProvider: Provider = {
      provide: RABBITMQ_OPTIONS,
      useValue: options,
    };

    const rabbitMQServiceProvider: Provider = {
      provide: RabbitMQService,
      useFactory: (options: AmqpOptions) => new RabbitMQService(options),
      inject: [RABBITMQ_OPTIONS],
    };

    return {
      module: RabbitMQModule,
      providers: [
        rabbitMQOptionsProvider,
        rabbitMQServiceProvider,
        QueueSubscriberService,
        Reflector,
      ],
      exports: [RabbitMQService, QueueSubscriberService],
    };
  }

  static forRootAsync(options: {
    imports?: any[];
    useFactory: (...args: any[]) => Promise<AmqpOptions> | AmqpOptions;
    inject?: any[];
  }): DynamicModule {
    const rabbitMQOptionsProvider: Provider = {
      provide: RABBITMQ_OPTIONS,
      useFactory: options.useFactory,
      inject: options.inject || [],
    };

    const rabbitMQServiceProvider: Provider = {
      provide: RabbitMQService,
      useFactory: (options: AmqpOptions) => new RabbitMQService(options),
      inject: [RABBITMQ_OPTIONS],
    };

    return {
      module: RabbitMQModule,
      imports: [DiscoveryModule, ...(options.imports || [])],
      providers: [
        rabbitMQOptionsProvider,
        rabbitMQServiceProvider,
        QueueSubscriberService,
      ],
      exports: [RabbitMQService, QueueSubscriberService],
    };
  }
}
