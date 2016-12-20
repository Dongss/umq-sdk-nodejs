
import {UmqClient, UmqClientConfig, Producer as _Producer, 
  Consumer as _Consumer, Subscription as _Subscription, Message as _Message} from './UmqClient';
import {
  UmqError as _UmqError,
  UmqAPIError as _UmqAPIError,
} from './request';

import * as L from './logger';

export type Config = UmqClientConfig; 
export type Producer = _Producer;
export type Consumer = _Consumer;
export type Subscription = _Subscription;
export type UmqError = _UmqError;
export type UmqAPIError = _UmqAPIError;
export type Message = _Message;

export function newUmqClient(config: UmqClientConfig): UmqClient {
  return new UmqClient({
    projectId: config.projectId,
    host: config.host,
    timeout: config.timeout,
  });
}


export function setLogger(logger: L.Logger) {
  L.setLogger(logger);
}

